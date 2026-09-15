"""One log format for the host, NoneBot and ASGI; safe console text."""

import logging
import re
from logging.handlers import RotatingFileHandler
from typing import Any

from .config import Settings

ANSI = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))")
URL = re.compile(r"https?://\S+", re.IGNORECASE)


def clean(value: str) -> str:
    value = URL.sub("<链接已隐藏>", ANSI.sub("", value))
    return "".join(c if c.isprintable() else " " for c in value)


def channel(name: str, message: str) -> str:
    if "处理汇总：" in message:
        return "处理"
    if any(word in message for word in ("对象存储", "存储整理", "散文件", "数据库分片")):
        return "迁移"
    if any(word in message for word in ("分卷", "发布", "节点统计", "云端修订")):
        return "发布"
    if any(word in message for word in (
        "清理", "删源", "删前", "删除", "根目录", "群文件", "本地归档校验"
    )):
        return "清理"
    if "plugins" in name:
        return "处理"
    if name.endswith(("files", "collector")):
        return "下载"
    return "系统"


class UnifiedFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        message = clean(record.getMessage())
        label = channel(record.name, message)
        result = (f"{self.formatTime(record)} {record.levelname:<7} "
                  f"[{label}] {record.name} | {message}")
        if record.exc_info:
            # Keep the traceback in the file; escape control sequences and URLs.
            result += "\n" + "\n".join(
                clean(line) for line in self.formatException(record.exc_info).splitlines()
            )
        return result


def framework_sink(message: Any) -> None:
    record = message.record
    text = record["message"]
    name = record["name"] or "nonebot"
    # Raw QQ events contain large payloads/download URLs; Matcher lifecycle and
    # API access records don't explain application work. Keep failures visible.
    noisy = ("Event will be handled by Matcher" in text
             or ("Matcher(" in text and "running complete" in text)
             or " | [notice." in text or " | [message." in text
             or "ptilopsisbot/overview" in text or "ptilopsisbot/status" in text)
    level = record["level"].no
    if level < logging.WARNING and noisy:
        return
    exc = record["exception"]
    # A dedicated logger avoids cycling through Uvicorn's LoguruHandler.
    logging.getLogger("ptilopsisbot.framework").log(
        logging.INFO if level == 25 else level, "%s: %s", name, text,
        exc_info=(exc.type, exc.value, exc.traceback) if exc else None,
    )


def configure_logging(settings: Settings, *, console: bool = True) -> None:
    from nonebot.log import logger

    settings.data_dir.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        settings.data_dir / "collector.log", maxBytes=5 * 1024**2,
        backupCount=3, encoding="utf-8",
    )
    handlers: list[logging.Handler] = [handler]
    if console:
        handlers.append(logging.StreamHandler())
    for sink in handlers:
        sink.setFormatter(UnifiedFormatter())
    logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    for name in ("uvicorn.error", "uvicorn.access"):
        logging.getLogger(name).propagate = False
    logger.remove()
    logger.add(framework_sink, level="INFO", format="{message}", backtrace=False, diagnose=False)
