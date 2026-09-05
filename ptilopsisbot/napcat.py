import hashlib
import json
import logging
from collections import Counter
from dataclasses import replace
from typing import Any, Protocol

import httpx
from nonebot.adapters.onebot.v11 import Message, MessageSegment

from .collector import OfflineError, Upload
from .files import download

log = logging.getLogger(__name__)


class OneBotRPC(Protocol):
    async def call_api(self, api: str, **data: Any) -> Any: ...


class NapCat:
    def __init__(self, group_id: int, http: httpx.AsyncClient) -> None:
        self.group_id = group_id
        self.http = http
        self.bot: OneBotRPC | None = None
        self.account_online = False

    @property
    def online(self) -> bool:
        return self.bot is not None and self.account_online

    async def refresh_status(self) -> bool:
        if self.bot is None:
            self.account_online = False
            return False
        try:
            status = await self.bot.call_api("get_status")
            self.account_online = status.get("online") is True
        except Exception as exc:
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
        return [upload for upload, _ in files if counts[upload.file_id] == 1]

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
                    float(file["upload_time"]),
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
        digest = await download(self.http, await self._url(handle), None, upload.size, upload.size)
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
