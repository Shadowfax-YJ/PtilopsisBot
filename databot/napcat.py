import logging
from typing import Any, Protocol

from nonebot.adapters.onebot.v11 import ActionFailed, Message, MessageSegment

from .collector import OfflineError, SourceMissingError, Upload

log = logging.getLogger(__name__)


class OneBotRPC(Protocol):
    async def call_api(self, api: str, **data: Any) -> Any: ...


def upload_from_notice(data: dict[str, Any]) -> Upload | None:
    try:
        file = data["file"]
        if not isinstance(file["id"], str) or not file["id"]:
            raise ValueError("缺少文件 ID")
        return Upload(
            int(data["group_id"]),
            str(file["id"]),
            int(file["busid"]),
            int(data["user_id"]),
            str(file["name"]),
            int(file["size"]),
            float(data["time"]),
        )
    except (KeyError, TypeError, ValueError):
        log.warning("上传事件缺少有效字段，等待根目录补扫")
        return None


class NapCat:
    def __init__(self, group_id: int) -> None:
        self.group_id = group_id
        self.bot: OneBotRPC | None = None

    @property
    def online(self) -> bool:
        return self.bot is not None

    async def _call(self, action: str, **data: Any) -> Any:
        if self.bot is None:
            raise OfflineError("NapCat 未连接")
        return await self.bot.call_api(action, group_id=self.group_id, **data)

    async def list_uploads(self, count: int) -> list[Upload]:
        response = await self._call("get_group_root_files", file_count=count)
        files = response["files"]
        if not isinstance(files, list):
            raise ValueError("NapCat 未返回有效根目录文件列表")
        if len(files) >= count:
            log.warning("根目录列表达到 %s 条上限，请检查是否遗漏文件", count)
        uploads = []
        for file in files:
            try:
                if not isinstance(file["file_id"], str) or not file["file_id"]:
                    raise ValueError("缺少文件 ID")
                uploads.append(
                    Upload(
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
                )
            except (KeyError, TypeError, ValueError):
                log.warning("根目录文件缺少有效字段，跳过该项")
        return uploads

    async def file_url(self, file_id: str, busid: int) -> str:
        response = await self._call("get_group_file_url", file_id=file_id, busid=busid)
        url = response.get("url")
        if not isinstance(url, str) or not url.startswith(("https://", "http://")):
            raise ValueError("NapCat 未返回 HTTP 下载地址")
        return url

    async def delete_file(self, file_id: str, busid: int) -> None:
        missing = {"file not found", "文件不存在", "文件已不存在", "群文件不存在"}
        try:
            response = await self._call("delete_group_file", file_id=file_id, busid=busid)
        except ActionFailed as exc:
            message = str(exc.info.get("message") or exc.info.get("wording") or "").strip()
            if message.lower() in missing:
                raise SourceMissingError(message) from exc
            raise
        # NapCat wraps the NTQQ result inside a successful OneBot envelope.
        if isinstance(response, dict) and response.get("result", 0) != 0:
            message = str(response.get("errMsg") or "NapCat 删除失败").strip()
            if message.lower() in missing:
                raise SourceMissingError(message)
            raise RuntimeError(message)

    async def send_report(self, text: str) -> None:
        # Force plain text so group nicknames cannot inject CQ commands.
        await self._call("send_group_msg", message=Message(MessageSegment.text(text)))
