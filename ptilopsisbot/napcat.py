import asyncio
import hashlib
import json
import logging
import math
import time
from collections import Counter, deque
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, Protocol

import httpx
from nonebot.adapters.onebot.v11 import GroupMessageEvent, Message, MessageSegment

from .collector import OfflineError, Upload
from .files import CollectionError, SourceFileMissing, StorageLimitSatisfied, download

log = logging.getLogger(__name__)


class OneBotRPC(Protocol):
    async def call_api(self, api: str, **data: Any) -> Any: ...


@dataclass
class FileMessage:
    message_id: int
    uploader_id: int
    name: str
    size: int
    time: int
    used: bool = False

    def matches(self, upload: Upload) -> bool:
        # QQ's file upload time and chat message time can differ slightly.
        return (self.uploader_id, self.name, self.size) == (
            upload.uploader_id,
            upload.name,
            upload.size,
        ) and (upload.uploaded_at <= 0 or abs(self.time - upload.uploaded_at) <= 60)


class NapCat:
    def __init__(
        self, group_id: int, http: httpx.AsyncClient, *, clock: Callable[[], float] = time.time
    ) -> None:
        self.group_id = group_id
        self.http = http
        self.bot: OneBotRPC | None = None
        self.account_online = False
        self.root_files_lock = asyncio.Lock()
        self.delete_lock = asyncio.Lock()
        self.clock = clock
        self.file_messages: deque[FileMessage] = deque(maxlen=1000)
        self.initial_file_ids: set[str] | None = None
        self.observed_file_counts: Counter[str] = Counter()
        self.observed_latest_times: dict[str, float] = {}
        self.connection_generation = 0
        self.on_receipts_reset: Callable[[], None] | None = None
        self.reset_receipts()

    def reset_receipts(self) -> None:
        # Reply IDs belong to this live connection; never persist or replay them.
        self.file_messages.clear()
        self.initial_file_ids = None
        self.observed_file_counts.clear()
        self.observed_latest_times.clear()
        self.connection_generation += 1
        self.receipts_since = int(self.clock())
        if self.on_receipts_reset is not None:
            self.on_receipts_reset()

    def remember_file_message(self, event: GroupMessageEvent) -> None:
        if (
            event.group_id != self.group_id
            or event.user_id == event.self_id
            or event.time < self.receipts_since
            or any(message.message_id == event.message_id for message in self.file_messages)
        ):
            return
        files = [segment for segment in event.message if segment.type == "file"]
        if len(files) != 1:
            return
        try:
            source = FileMessage(
                event.message_id,
                event.user_id,
                str(files[0].data["file"]),
                int(files[0].data["file_size"]),
                event.time,
            )
        except (KeyError, TypeError, ValueError):
            return
        self.file_messages.append(source)

    @property
    def online(self) -> bool:
        return self.bot is not None and self.account_online

    async def refresh_status(self) -> bool:
        if self.bot is None:
            self.reset_receipts()
            self.account_online = False
            return False
        try:
            status = await self.bot.call_api("get_status")
            online = status.get("online") is True
            if online != self.account_online:
                self.reset_receipts()
            self.account_online = online
        except Exception as exc:
            self.reset_receipts()
            self.account_online = False
            raise OfflineError("无法确认 QQ 在线状态") from exc
        return self.account_online

    async def _call(self, action: str, **data: Any) -> Any:
        if not await self.refresh_status():
            raise OfflineError("QQ 账号未在线")
        assert self.bot is not None
        return await self.bot.call_api(action, group_id=self.group_id, **data)

    async def list_uploads(self) -> list[Upload]:
        files = await self._list_files()
        counts = Counter(upload.file_id for upload, _ in files)
        all_uploads = [upload for upload, _ in files]
        if self.initial_file_ids is None:
            self.initial_file_ids = {upload.file_id for upload in all_uploads}
        uploads = []
        missing_times = 0
        for file_id in counts:
            candidates = [(u, handle) for u, handle in files if u.file_id == file_id]
            if len(candidates) > 1:
                try:
                    upload, _ = self._latest(candidates)
                except CollectionError:
                    log.warning(
                        "群文件 %s 有重复候选但无法确定最新一份，暂时保留", candidates[0][0].name
                    )
                    continue
                upload = replace(upload, has_duplicates=True)
            else:
                upload, _ = candidates[0]
            source = self._matching_message(upload, all_uploads)
            current_message = (
                source is not None
                and (upload.uploaded_at <= 0 or upload.uploaded_at >= self.receipts_since)
            )
            live = current_message and source is not None and not source.used
            # Retention also protects files first seen after the initial root snapshot,
            # even if their chat event is delayed/missing and they have no priority.
            is_new = (
                current_message or upload.uploaded_at >= self.receipts_since
                or (upload.uploaded_at <= 0 and upload.file_id not in self.initial_file_ids)
                or (
                    upload.has_duplicates and (
                        any(message.matches(upload) for message in self.file_messages)
                        or (
                            upload.file_id in self.observed_file_counts
                            and (
                                counts[upload.file_id] > self.observed_file_counts[upload.file_id]
                                or upload.modified_at
                                > self.observed_latest_times.get(upload.file_id, 0)
                            )
                        )
                    )
                )
            )
            if upload.uploaded_at <= 0:
                missing_times += 1
                if source is not None:
                    # Keep the raw observation key: inferred dates must not change identity.
                    upload = replace(upload, uploaded_at=float(source.time), time_source="message")
            uploads.append(replace(upload, live=live, source_kind="live" if is_new else "history"))
        self.observed_file_counts = counts
        self.observed_latest_times = {
            file_id: max(u.modified_at for u in all_uploads if u.file_id == file_id)
            for file_id in counts
        }
        if missing_times:
            log.warning(
                "%s 个群文件没有有效上传时间，按文件消息时间或首次发现时间登记", missing_times
            )
        return uploads

    def _matching_message(self, upload: Upload, uploads: list[Upload]) -> FileMessage | None:
        matches = [message for message in self.file_messages if message.matches(upload)]
        if len(matches) != 1:
            return None
        source = matches[0]
        return source if sum(source.matches(candidate) for candidate in uploads) == 1 else None

    async def _list_files(self, *, require_complete: bool = False) -> list[tuple[Upload, str]]:
        # NapCat defaults to 50 entries. Keep this request bounded for a small group.
        # NapCat uses a shared, uncorrelated native event for every page. Keep the
        # entire paginated RPC exclusive across scans, downloads and cleanup.
        # Release before URL lookup and streaming so downloads remain concurrent.
        async with self.root_files_lock:
            response = await self._call("get_group_root_files", file_count=1000)
        files = response["files"]
        if not isinstance(files, list):
            raise CollectionError("NapCat 未返回有效根目录文件列表")
        folders = response.get("folders", [])
        if not isinstance(folders, list):
            raise CollectionError("NapCat 未返回有效根目录文件夹列表")
        if len(files) + len(folders) >= 1000:
            if require_complete:
                raise CollectionError("根目录达到扫描上限，无法确认源文件已删除")
            log.warning("根目录达到单次扫描上限 1000 项，部分文件可能需要清理后再补扫")
        uploads = []
        for file in files:
            try:
                if not isinstance(file["file_id"], str) or not file["file_id"]:
                    raise ValueError("缺少文件 ID")
                upload = Upload(
                    self.group_id,
                    str(file["file_id"]),
                    int(file["busid"]),
                    int(file["uploader"]),
                    str(file["file_name"]),
                    int(file["file_size"]),
                    float(file.get("upload_time") or 0),
                    str(file.get("uploader_name") or ""),
                    True,
                    modified_at=self._timestamp(file.get("modify_time")),
                )
                identity = json.dumps(
                    [
                        upload.group_id,
                        upload.busid,
                        upload.uploader_id,
                        upload.name,
                        upload.size,
                        upload.uploaded_at,
                    ],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                # The database stores our observation key, never NapCat's expiring handle.
                key = "napcat:" + hashlib.sha256(identity.encode()).hexdigest()
                uploads.append((replace(upload, file_id=key), str(file["file_id"])))
            except (KeyError, TypeError, ValueError):
                if require_complete:
                    raise CollectionError("根目录文件字段不完整，无法确认源文件已删除") from None
                log.warning("根目录文件缺少有效字段，跳过该项")
        return uploads

    async def storage_usage(self) -> int:
        # NapCat's used_space has returned zero despite existing files. Sum actual
        # root/folder entries instead, sharing the native pagination lock with scans
        # and deletion. Installers and other uncollected files count toward usage too.
        total = 0
        pending: deque[str | None] = deque([None])
        seen_folders: set[str] = set()
        async with self.root_files_lock:
            if not await self.refresh_status():
                raise OfflineError("QQ 账号未在线")
            generation = self.connection_generation
            while pending:
                folder = pending.popleft()
                response = (
                    await self._call("get_group_root_files", file_count=1000)
                    if folder is None else await self._call(
                        "get_group_files_by_folder", folder_id=folder, file_count=1000,
                    )
                )
                if not isinstance(response, dict):
                    raise CollectionError("容量统计未取得有效文件列表，暂停容量清理")
                files, folders = response.get("files"), response.get("folders")
                if not isinstance(files, list) or not isinstance(folders, list):
                    raise CollectionError("容量统计缺少文件或文件夹列表，暂停容量清理")
                if len(files) + len(folders) >= 1000:
                    raise CollectionError("容量统计达到目录扫描上限，暂停容量清理")
                handles: set[str] = set()
                for item in files:
                    if not isinstance(item, dict):
                        raise CollectionError("容量统计遇到无效文件字段，暂停容量清理")
                    handle, size = item.get("file_id"), item.get("file_size")
                    if (
                        not isinstance(handle, str) or not handle or handle in handles
                        or type(size) is not int or size < 0
                    ):
                        raise CollectionError("容量统计遇到无效或重复文件字段，暂停容量清理")
                    handles.add(handle)
                    total += size
                for item in folders:
                    folder_id = item.get("folder_id") if isinstance(item, dict) else None
                    if (
                        not isinstance(folder_id, str) or not folder_id
                        or folder_id in seen_folders or len(seen_folders) >= 1000
                    ):
                        raise CollectionError("容量统计遇到重复目录或扫描上限，暂停容量清理")
                    seen_folders.add(folder_id)
                    pending.append(folder_id)
            if generation != self.connection_generation:
                raise CollectionError("容量统计期间连接已重置，等待重新统计")
        return total

    @staticmethod
    def _timestamp(value: Any) -> float:
        try:
            result = float(value or 0)
        except (ValueError, TypeError):
            return 0
        return result if math.isfinite(result) and result > 0 else 0

    @staticmethod
    def _latest(candidates: list[tuple[Upload, str]]) -> tuple[Upload, str]:
        # The observation key includes raw upload_time, so conflicting candidates
        # have equal upload times. Use modification time only as an ordering fallback;
        # never put this mutable field into the persistent observation key.
        ordered = sorted(candidates, key=lambda item: item[0].modified_at, reverse=True)
        if (
            len({handle for _, handle in candidates}) != len(candidates)
            or any(upload.modified_at <= 0 for upload, _ in candidates)
            or ordered[0][0].modified_at == ordered[1][0].modified_at
        ):
            raise CollectionError("重复候选缺少可区分的时间，无法确定最新一份，保留源文件")
        return ordered[0]

    async def file_url(self, file_id: str, busid: int) -> str:
        _, handle = await self._resolve(file_id, busid, allow_latest=True)
        return await self._url(handle)

    async def _resolve(
        self, file_id: str, busid: int, *, allow_latest: bool = False,
    ) -> tuple[Upload, str]:
        matches = [
            (upload, handle)
            for upload, handle in await self._list_files()
            if upload.file_id == file_id and upload.busid == busid
        ]
        if allow_latest and len(matches) > 1:
            return self._latest(matches)
        if not matches:
            # Absence is not a confirmed deletion: the source may have moved or
            # fallen outside this bounded listing. A later scan can restore it.
            raise SourceFileMissing()
        if len(matches) != 1:
            raise CollectionError(
                f"根目录无法唯一定位源文件（匹配 {len(matches)} 项），"
                "保留记录，等待补扫或人工检查"
            )
        return matches[0]

    async def _url(self, handle: str) -> str:
        response = await self._call("get_group_file_url", file_id=handle)
        url = response.get("url")
        if not isinstance(url, str) or not url.startswith(("https://", "http://")):
            raise CollectionError("NapCat 未返回 HTTP 下载地址")
        return url

    async def delete_duplicates(
        self, file_id: str, busid: int, *, expected_hash: str,
        can_delete: Callable[[], bool],
    ) -> int:
        async def current() -> list[tuple[Upload, str]]:
            return [
                (upload, handle)
                for upload, handle in await self._list_files(require_complete=True)
                if upload.file_id == file_id and upload.busid == busid
            ]

        def signature(items: list[tuple[Upload, str]]) -> Counter[float]:
            # Handles change on every listing; compare the pre-verification metadata.
            # This is only a membership check. Deletion uses the original hashed handle.
            return Counter(upload.modified_at for upload, _ in items)

        candidates = await current()
        if not candidates:
            raise SourceFileMissing()
        if len(candidates) == 1:
            return 0
        keeper = self._latest(candidates)
        generation = self.connection_generation
        # Verify EVERY candidate against the checked local archive before deleting any.
        # A same-name file with different bytes is not a redundant copy.
        for upload, handle in [keeper, *(item for item in candidates if item != keeper)]:
            digest = await download(
                self.http, await self._url(handle), None, upload.size, upload.size,
                label=f"重复副本校验 {upload.name}",
            )
            if digest != expected_hash:
                raise CollectionError("重复候选内容与本地归档不一致，保留整组群文件")

        remaining = list(candidates)
        removed = 0
        async with self.delete_lock:
            for upload, handle in candidates:
                if handle == keeper[1]:
                    continue
                observed = await current()
                if signature(observed) != signature(remaining):
                    raise CollectionError("重复文件列表在校验期间变化，保留源文件，等待重新核对")
                async with self.root_files_lock:
                    if not await self.refresh_status():
                        raise OfflineError("QQ 账号未在线")
                    if generation != self.connection_generation:
                        raise CollectionError("校验期间连接已重置，等待重新核对重复文件")
                    if not can_delete():
                        raise CollectionError("环境不可用或自动清理已关闭，保留重复文件")
                    assert self.bot is not None
                    result = await self.bot.call_api(
                        "delete_group_file", group_id=self.group_id,
                        file_id=handle, busid=upload.busid,
                    )
                self._check_delete_result(result)
                remaining.remove((upload, handle))
                for attempt in range(3):
                    if attempt:
                        await asyncio.sleep(1)
                    if signature(await current()) == signature(remaining):
                        break
                else:
                    raise CollectionError("删除重复副本后未确认预期的剩余文件，保留待清理状态")
                removed += 1
        return removed

    async def delete_file(
        self, file_id: str, busid: int, *, expected_hash: str,
        can_delete: Callable[[], bool] | None = None,
        storage_limit_bytes: int | None = None,
    ) -> None:
        upload, handle = await self._resolve(file_id, busid)
        generation = self.connection_generation
        digest = await download(
            self.http,
            await self._url(handle),
            None,
            upload.size,
            upload.size,
            label=f"删源前远端校验 {upload.name}",
        )
        if digest != expected_hash:
            raise CollectionError("远端候选文件内容与本地归档不符，保留源文件")
        # Streams run concurrently; only the final deletion and confirmation wait
        # for this lock. Recheck retention after waiting, using the verified handle.
        async with self.delete_lock:
            if storage_limit_bytes is not None:
                # Another admin may have freed space while hashing or waiting for
                # the lock. Recheck the whole group before bypassing retention.
                if await self.storage_usage() <= storage_limit_bytes:
                    raise StorageLimitSatisfied("群文件容量已降至阈值以内，停止提前清理")
            # Avoid a deletion update interrupting another native paginated read.
            async with self.root_files_lock:
                if not await self.refresh_status():
                    raise OfflineError("QQ 账号未在线")
                if generation != self.connection_generation:
                    raise CollectionError("删源校验期间连接已重置，等待重新核对")
                if can_delete is not None and not can_delete():
                    raise CollectionError("清理时间尚未到或环境不可用，保留源文件")
                assert self.bot is not None
                result = await self.bot.call_api(
                    "delete_group_file", group_id=self.group_id, file_id=handle, busid=upload.busid,
                )
            await self._confirm_deletion(file_id, busid, result)

    async def _confirm_deletion(self, file_id: str, busid: int, result: Any) -> None:
        self._check_delete_result(result)
        # QQ can acknowledge the request without removing the file. Allow a short
        # propagation delay, but never repeat a delete using a newly acquired handle.
        for attempt in range(3):
            if attempt:
                await asyncio.sleep(1)
            remaining = await self._list_files(require_complete=True)
            if not any(u.file_id == file_id and u.busid == busid for u, _ in remaining):
                return
        raise CollectionError(
            f"删除接口返回成功，但补查仍发现源文件（busid={busid}），保留待清理状态"
        )

    @staticmethod
    def _check_delete_result(result: Any) -> None:
        if not isinstance(result, dict) or type(result.get("result")) is not int:
            raise CollectionError("NapCat 未返回明确的群文件删除结果")
        if result["result"] != 0:
            raise CollectionError(f"QQ 拒绝删除群文件，错误码 {result['result']}")

    async def send_message(self, text: str) -> None:
        # Force plain text so group nicknames cannot inject CQ commands.
        await self._call("send_group_msg", message=Message(MessageSegment.text(text)))

    async def send_receipt(self, file_id: str, busid: int, text: str) -> bool:
        if not any(not message.used for message in self.file_messages):
            return False
        # Match at send time: a file message may arrive after the initial root scan.
        uploads = [upload for upload, _ in await self._list_files()]
        targets = [u for u in uploads if u.file_id == file_id and u.busid == busid]
        if len(targets) != 1 or 0 < targets[0].uploaded_at < self.receipts_since:
            return False
        source = self._matching_message(targets[0], uploads)
        if source is None or source.used:
            return False
        source.used = True  # Failed sends are not replayed.
        if not await self.refresh_status():
            return False
        # A reconnect during the status call invalidates even the source selected above.
        if not any(message is source for message in self.file_messages):
            return False
        assert self.bot is not None
        result = await self.bot.call_api(
            "send_group_msg",
            group_id=self.group_id,
            message=Message([MessageSegment.reply(source.message_id), MessageSegment.text(text)]),
        )
        reply_id = result.get("message_id", "未知") if isinstance(result, dict) else "未知"
        log.info(
            "收包回复已发送：文件=%s，源消息=%s，回复消息=%s",
            source.name,
            source.message_id,
            reply_id,
        )
        return True
