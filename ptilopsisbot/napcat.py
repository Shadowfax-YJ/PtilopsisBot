import hashlib
import json
import logging
import time
from collections import Counter, deque
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, Protocol

import httpx
from nonebot.adapters.onebot.v11 import GroupMessageEvent, Message, MessageSegment

from .collector import OfflineError, Upload
from .files import download

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
        self.clock = clock
        self.file_messages: deque[FileMessage] = deque(maxlen=1000)
        self.reset_receipts()

    def reset_receipts(self) -> None:
        # Reply IDs belong to this live connection; never persist or replay them.
        self.file_messages.clear()
        self.receipts_since = int(self.clock())

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
        if any(count > 1 for count in counts.values()):
            log.warning("多个群文件的上传信息完全相同，暂不收集或清理这些文件")
        all_uploads = [upload for upload, _ in files]
        uploads = []
        missing_times = 0
        for upload in all_uploads:
            if counts[upload.file_id] != 1:
                continue
            if upload.uploaded_at <= 0:
                missing_times += 1
                source = self._matching_message(upload, all_uploads)
                if source is not None:
                    # Keep the raw observation key: inferred dates must not change identity.
                    upload = replace(upload, uploaded_at=float(source.time), time_source="message")
            uploads.append(upload)
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

    async def _list_files(self) -> list[tuple[Upload, str]]:
        # NapCat defaults to 50 entries. Keep this request bounded for a small group.
        response = await self._call("get_group_root_files", file_count=1000)
        files = response["files"]
        if not isinstance(files, list):
            raise ValueError("NapCat 未返回有效根目录文件列表")
        if len(files) + len(response.get("folders", [])) >= 1000:
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
                log.warning("根目录文件缺少有效字段，跳过该项")
        return uploads

    async def file_url(self, file_id: str, busid: int) -> str:
        _, handle = await self._resolve(file_id, busid)
        return await self._url(handle)

    async def _resolve(self, file_id: str, busid: int) -> tuple[Upload, str]:
        matches = [
            (upload, handle)
            for upload, handle in await self._list_files()
            if upload.file_id == file_id and upload.busid == busid
        ]
        if len(matches) != 1:
            # A bounded root listing cannot prove the file is gone (it may have moved).
            raise ValueError("根目录无法唯一定位源文件，保留记录，等待补扫或人工检查")
        return matches[0]

    async def _url(self, handle: str) -> str:
        response = await self._call("get_group_file_url", file_id=handle)
        url = response.get("url")
        if not isinstance(url, str) or not url.startswith(("https://", "http://")):
            raise ValueError("NapCat 未返回 HTTP 下载地址")
        return url

    async def delete_file(self, file_id: str, busid: int, *, expected_hash: str) -> None:
        upload, handle = await self._resolve(file_id, busid)
        digest = await download(
            self.http,
            await self._url(handle),
            None,
            upload.size,
            upload.size,
            label=f"删源前远端校验 {upload.name}",
        )
        if digest != expected_hash:
            raise ValueError("远端候选文件内容与本地归档不符，保留源文件")
        # Use exactly the handle whose content was checked. Never rebind on delete failure.
        result = await self._call("delete_group_file", file_id=handle)
        if not isinstance(result, dict) or type(result.get("result")) is not int:
            raise ValueError("NapCat 未返回明确的群文件删除结果")
        if result["result"] != 0:
            raise ValueError(f"QQ 拒绝删除群文件，错误码 {result['result']}")

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
