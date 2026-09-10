import asyncio
import contextlib
import logging
import secrets
from collections.abc import Awaitable, Callable
from datetime import date, datetime, timedelta
from functools import partial
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
from .milestones import Milestones
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
        limits=httpx.Limits(
            max_connections=settings.download_concurrency + settings.cleanup_concurrency,
        ),
        trust_env=False,
    )
    api = NapCat(settings.group_id, http)
    collector = Collector(settings, api=api, http=http)
    milestones = Milestones(settings, collector.db, api=api)
    api.on_receipts_reset = collector.reset_priority
    download_wake = asyncio.Event()
    scan_wake = asyncio.Event()
    cleanup_wake = asyncio.Event()
    milestone_wake = asyncio.Event()
    scan_pending = False
    cleanup_pending = False
    next_cleanup_check = 0.0
    scheduler = AsyncIOScheduler(timezone=SHANGHAI)
    workers: list[asyncio.Task[None]] = []

    def request_cleanup() -> None:
        nonlocal cleanup_pending
        cleanup_pending = True
        cleanup_wake.set()

    async def scan() -> bool:
        nonlocal scan_pending
        if not scan_pending or not await api.refresh_status():
            return False
        scan_pending = False
        try:
            uploads = await api.list_uploads()
            count = sum(collector.register(upload) is not None for upload in uploads)
        except Exception:
            scan_pending = True
            raise
        download_wake.set()
        request_cleanup()
        log.info("根目录补扫登记 %s 个符合条件的包，已请求检查清理条件", count)
        return False

    async def download_one(*, live_only: bool) -> bool:
        if not await api.refresh_status():
            return False
        processed = await collector.process_once(live_only=live_only)
        if processed:
            request_cleanup()
            milestone_wake.set()
        return processed

    async def cleanup() -> bool:
        nonlocal cleanup_pending, next_cleanup_check
        if not cleanup_pending and collector.clock() < next_cleanup_check:
            return False
        if not await api.refresh_status():
            return False
        cleanup_pending = False
        try:
            await collector.cleanup()
        except Exception:
            cleanup_pending = True
            raise
        next_cleanup_check = collector.clock() + 60
        log.info("本轮群文件清理检查结束")
        return False

    async def daily_report() -> None:
        day = datetime.now(SHANGHAI).date() - timedelta(days=1)
        try:
            await collector.send_report(day)
        except Exception as exc:
            log.warning("日报 %s 发送失败 (%s)，可使用 report 命令补发", day, type(exc).__name__)

    async def scheduled_scan() -> None:
        nonlocal scan_pending
        scan_pending = True
        scan_wake.set()

    async def worker_loop(
        operation: Callable[[], Awaitable[bool]],
        wake: asyncio.Event,
        label: str,
    ) -> None:
        last_error = ""
        while True:
            wake.clear()
            try:
                if await operation():
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
                    log.error("%s 暂停，5 秒后检查环境: %s", label, error)
                last_error = error
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(wake.wait(), timeout=5)

    @driver.on_startup
    async def start() -> None:
        collector.recover_legacy_failures()
        collector.recover_invalid_packages()
        collector.repair_dates()
        milestones.initialize()
        workers.append(asyncio.create_task(worker_loop(scan, scan_wake, "补扫")))
        workers.append(asyncio.create_task(worker_loop(cleanup, cleanup_wake, "清理")))
        workers.append(asyncio.create_task(worker_loop(milestones.check, milestone_wake, "里程碑")))
        for index in range(settings.download_concurrency):
            live_only = index == 0 and settings.download_concurrency > 1
            workers.append(
                asyncio.create_task(
                    worker_loop(
                        partial(download_one, live_only=live_only),
                        download_wake,
                        "新包下载" if live_only else f"下载 {index + 1}",
                    )
                )
            )
        scheduler.add_job(scheduled_scan, "interval", minutes=30, max_instances=1, coalesce=True)
        scheduler.add_job(daily_report, "cron", hour=0, minute=5, max_instances=1, coalesce=True)
        scheduler.start()
        log.info("收包回执=%s", "开启" if settings.send_receipts else "关闭")
        if settings.milestone_interval is not None:
            log.info("里程碑公告：每 %s 份唯一数据一次，下一档为 %s 份",
                     settings.milestone_interval, milestones.status()["next_threshold"])
        log.info("下载及 ZIP 检查失败最多尝试 %s 次（含首次），重试间隔 5 分钟",
                 settings.download_max_attempts)
        log.info("删源校验并发=%s，删除请求逐个执行", settings.cleanup_concurrency)
        log.info("重复候选：归档并核对整组内容后清理旧副本，最新一份按来源保留")
        if settings.group_storage_limit_gib is not None:
            log.info(
                "容量清理阈值=%s GiB；超限时可提前清理最旧的已归档包，降至阈值以内停止",
                settings.group_storage_limit_gib,
            )
        log.info(
            "目标群 %s，自动清理=%s，新包/未知来源宽限期=%s 小时，历史包宽限期=%s 小时，下载并发=%s"
            "（多名额时保留 1 个给新包），等待 NapCat 连接",
            settings.group_id,
            settings.auto_delete,
            settings.delete_grace_hours,
            settings.history_delete_grace_hours
            if settings.history_delete_grace_hours is not None else settings.delete_grace_hours,
            settings.download_concurrency,
        )

    @driver.on_shutdown
    async def stop() -> None:
        scheduler.shutdown(wait=False)
        for worker in workers:
            worker.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
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
        download_wake.set()
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
            "send_receipts": settings.send_receipts,
            "milestones": milestones.status(),
            "delete_grace_hours": settings.delete_grace_hours,
            "history_delete_grace_hours": settings.history_delete_grace_hours,
            "total": len(records),
            "download_concurrency": settings.download_concurrency,
            "download_max_attempts": settings.download_max_attempts,
            "cleanup_concurrency": settings.cleanup_concurrency,
            "group_storage_limit_gib": settings.group_storage_limit_gib,
            "group_storage_bytes": collector.group_storage_bytes,
            "storage_checked_at": collector.storage_checked_at,
            "storage_cleanup_error": collector.storage_cleanup_error,
            "active_downloads": sorted(
                key for key, phase in collector.active.items() if phase == "download"
            ),
            "active_cleanup": sorted(
                key for key, phase in collector.active.items() if phase == "cleanup"
            ),
            "records": [
                {**dict(row), "delete_after": collector.delete_after(row)} for row in records[-50:]
            ],
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
        download_wake.set()
        return {"status": "queued"}

    @app.post("/ptilopsisbot/report/{day}", dependencies=auth)
    async def request_report(day: date) -> dict[str, str]:
        try:
            text = await collector.send_report(day)
        except Exception as exc:
            raise HTTPException(503, f"发送失败: {type(exc).__name__}") from exc
        if text is None:
            return {"status": "skipped", "reason": "当日无新增唯一包，不发送日报"}
        return {"report": text}

    nonebot.run()
