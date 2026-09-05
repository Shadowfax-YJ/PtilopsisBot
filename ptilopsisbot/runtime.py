import asyncio
import contextlib
import logging
import secrets
from datetime import date, datetime, timedelta
from logging.handlers import RotatingFileHandler
from typing import Any

import httpx
import nonebot
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import Depends, Header, HTTPException
from nonebot.adapters.onebot.v11 import Adapter, Bot, GroupMessageEvent, GroupUploadNoticeEvent
from nonebot.drivers.fastapi import Driver

from .collector import SHANGHAI, Collector
from .config import Settings
from .napcat import NapCat

log = logging.getLogger(__name__)


def configure_logging(settings: Settings) -> None:
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    handler = RotatingFileHandler(
        settings.data_dir / "collector.log", maxBytes=5 * 1024**2, backupCount=3, encoding="utf-8"
    )
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[handler, logging.StreamHandler()],
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)


def run(settings: Settings) -> None:
    configure_logging(settings)
    nonebot.init(
        driver="~fastapi",
        host="127.0.0.1",
        port=settings.port,
        onebot_access_token=settings.access_token.get_secret_value(),
        _env_file=None,
        fastapi_openapi_url=None,
        fastapi_docs_url=None,
        fastapi_redoc_url=None,
    )
    driver = nonebot.get_driver()
    assert isinstance(driver, Driver)
    driver.register_adapter(Adapter)
    app = driver.server_app
    http = httpx.AsyncClient(
        timeout=httpx.Timeout(60, connect=15),
        follow_redirects=True,
        limits=httpx.Limits(max_connections=2),
        trust_env=False,
    )
    api = NapCat(settings.group_id, http)
    collector = Collector(settings, api=api, http=http)
    wake = asyncio.Event()
    scan_requested = asyncio.Event()
    scheduler = AsyncIOScheduler(timezone=SHANGHAI)
    worker: asyncio.Task[None] | None = None

    async def scan() -> int:
        uploads = await api.list_uploads()
        count = sum(collector.register(upload) is not None for upload in uploads)
        wake.set()
        await collector.cleanup()
        log.info("根目录补扫结束，发现 %s 个符合条件的包", count)
        return count

    async def daily_report() -> None:
        day = datetime.now(SHANGHAI).date() - timedelta(days=1)
        try:
            await api.send_message(collector.report(day))
        except Exception as exc:
            log.warning("日报 %s 发送失败 (%s)，可使用 report 命令补发", day, type(exc).__name__)

    async def scheduled_scan() -> None:
        scan_requested.set()
        wake.set()

    async def work() -> None:
        last_error = ""
        while True:
            wake.clear()
            try:
                if await api.refresh_status() and scan_requested.is_set():
                    scan_requested.clear()
                    try:
                        await scan()
                    except Exception:
                        scan_requested.set()
                        raise
                if await collector.process_once():
                    last_error = ""
                    continue
                last_error = ""
            except Exception as exc:
                error = (
                    f"{type(exc).__name__}: {exc}"
                    if isinstance(exc, OSError)
                    else type(exc).__name__
                )
                if error != last_error:
                    log.error("处理暂停，5 秒后检查环境: %s", error)
                last_error = error
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(wake.wait(), timeout=5)

    @driver.on_startup
    async def start() -> None:
        nonlocal worker
        worker = asyncio.create_task(work())
        scheduler.add_job(scheduled_scan, "interval", minutes=30, max_instances=1, coalesce=True)
        scheduler.add_job(daily_report, "cron", hour=0, minute=5, max_instances=1, coalesce=True)
        scheduler.start()
        log.info(
            "目标群 %s，自动清理=%s，宽限期=%s 小时，等待 NapCat 连接",
            settings.group_id,
            settings.auto_delete,
            settings.delete_grace_hours,
        )

    @driver.on_shutdown
    async def stop() -> None:
        scheduler.shutdown(wait=False)
        if worker is not None:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        await http.aclose()
        collector.close()

    @driver.on_bot_connect
    async def connected(bot: Bot) -> None:
        if api.bot is not None and api.bot is not bot:
            log.warning("已有一个 QQ 账号连接，忽略额外账号")
            return
        api.bot = bot
        api.reset_receipts()
        await api.refresh_status()
        wake.set()
        await scheduled_scan()

    @driver.on_bot_disconnect
    async def disconnected(bot: Bot) -> None:
        if api.bot is bot:
            api.bot = None
            api.account_online = False
            api.reset_receipts()
            log.warning("NapCat 已断开，暂停采集和清理")

    notice = nonebot.on_notice(priority=10, block=False)

    @notice.handle()
    async def receive(bot: Bot, event: GroupUploadNoticeEvent) -> None:
        if api.bot is not bot or event.group_id != settings.group_id:
            return
        # Notice IDs are message handles, not stable group-file identities.
        await scheduled_scan()

    file_message = nonebot.on_message(priority=10, block=False)

    @file_message.handle()
    async def receive_file_message(bot: Bot, event: GroupMessageEvent) -> None:
        if api.bot is not bot or event.group_id != settings.group_id:
            return
        if not any(segment.type == "file" for segment in event.message):
            return
        api.remember_file_message(event)
        await scheduled_scan()

    async def authorize(authorization: str = Header(default="")) -> None:
        expected = "Bearer " + settings.access_token.get_secret_value()
        if not secrets.compare_digest(authorization, expected):
            raise HTTPException(401, "本机管理命令需要 access_token")

    auth = [Depends(authorize)]

    @app.get("/ptilopsisbot/status", dependencies=auth)
    async def status() -> dict[str, Any]:
        records = collector.records()
        return {
            "online": api.online,
            "group_id": settings.group_id,
            "auto_delete": settings.auto_delete,
            "total": len(records),
            "records": [dict(row) for row in records[-50:]],
        }

    @app.post("/ptilopsisbot/scan", dependencies=auth)
    async def request_scan() -> dict[str, str]:
        await scheduled_scan()
        return {"status": "queued"}

    @app.post("/ptilopsisbot/retry/{record_id}", dependencies=auth)
    async def request_retry(record_id: int) -> dict[str, str]:
        try:
            await collector.retry(record_id)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        wake.set()
        return {"status": "queued"}

    @app.post("/ptilopsisbot/report/{day}", dependencies=auth)
    async def request_report(day: date) -> dict[str, str]:
        text = collector.report(day)
        try:
            await api.send_message(text)
        except Exception as exc:
            raise HTTPException(503, f"发送失败: {type(exc).__name__}") from exc
        return {"report": text}

    nonebot.run()
