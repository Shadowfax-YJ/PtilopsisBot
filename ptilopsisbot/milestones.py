"""Durable milestone announcements, independent of collection and daily reports."""

import asyncio
import logging
import sqlite3
import time
from collections.abc import Callable
from typing import Any

from . import messages
from .collector import GroupAPI
from .config import Settings

log = logging.getLogger(__name__)


class Milestones:
    def __init__(
        self, settings: Settings, db: sqlite3.Connection, *,
        api: GroupAPI | None = None, clock: Callable[[], float] = time.time,
    ) -> None:
        self.settings = settings
        self.db = db
        self.api = api
        self.clock = clock
        self.lock = asyncio.Lock()
        with db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS milestone_state (
                    group_id INTEGER PRIMARY KEY,
                    last_count INTEGER NOT NULL,
                    initialized_at REAL NOT NULL
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS milestone_announcements (
                    group_id INTEGER NOT NULL,
                    threshold INTEGER NOT NULL,
                    observed_count INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    finished_at REAL,
                    error TEXT,
                    PRIMARY KEY (group_id, threshold)
                )
            """)

    def total_count(self) -> int:
        # Match the daily report's unique-content definition, including sources
        # deleted after archiving and previously archived records under repair.
        return int(self.db.execute(
            """SELECT COUNT(DISTINCT sha256) FROM uploads
               WHERE group_id=? AND collected_at IS NOT NULL AND sha256 IS NOT NULL""",
            (self.settings.group_id,),
        ).fetchone()[0])

    def initialize(self, *, baseline_count: int | None = None) -> None:
        """Call before starting workers; never replay milestones predating enablement."""
        if self.settings.milestone_interval is None:
            return
        baseline = self.total_count() if baseline_count is None else baseline_count
        if baseline < 0:
            raise ValueError("里程碑起点不能小于 0")
        with self.db:
            self.db.execute(
                """INSERT OR IGNORE INTO milestone_state (group_id, last_count, initialized_at)
                   VALUES (?, ?, ?)""",
                (self.settings.group_id, baseline, self.clock()),
            )
            interrupted = self.db.execute(
                """UPDATE milestone_announcements SET status='uncertain', finished_at=?,
                   error='上次发送中断，结果未确认，不自动重发'
                   WHERE group_id=? AND status='sending'""",
                (self.clock(), self.settings.group_id),
            ).rowcount
        if interrupted:
            log.warning("%s 条里程碑公告的发送结果未确认，保留记录，不自动重发", interrupted)

    async def check(self) -> bool:
        interval = self.settings.milestone_interval
        if interval is None or self.api is None or not self.api.online:
            return False
        async with self.lock:
            if not self.api.online:
                return False
            count = self.total_count()
            threshold = count // interval * interval
            # Claim durably before awaiting QQ, so concurrent checks and restarts
            # cannot replay the same announcement. Send only the highest reached
            # threshold if multiple were crossed while offline or disabled.
            with self.db:
                claimed = self.db.execute(
                    "UPDATE milestone_state SET last_count=? WHERE group_id=? AND last_count<?",
                    (threshold, self.settings.group_id, threshold),
                ).rowcount
                if not claimed:
                    return False
                self.db.execute(
                    """INSERT INTO milestone_announcements
                       (group_id, threshold, observed_count, status, created_at)
                       VALUES (?, ?, ?, 'sending', ?)""",
                    (self.settings.group_id, threshold, count, self.clock()),
                )
            try:
                await self.api.send_message(messages.milestone(threshold))
            except asyncio.CancelledError:
                self._finish(threshold, "uncertain", "发送被中断，结果未确认，不自动重发")
                raise
            except Exception as exc:
                # A timeout can occur after QQ accepted the message. OneBot does
                # not provide an idempotency key, so do not blindly send it again.
                self._finish(threshold, "uncertain", type(exc).__name__)
                log.warning("里程碑 %s 公告发送结果未确认 (%s)，不自动重发",
                            threshold, type(exc).__name__)
                return False
            self._finish(threshold, "sent")
            log.info("已发送 %s 份唯一数据的里程碑公告", threshold)
            return True

    def _finish(self, threshold: int, status: str, error: str | None = None) -> None:
        with self.db:
            self.db.execute(
                """UPDATE milestone_announcements SET status=?, finished_at=?, error=?
                   WHERE group_id=? AND threshold=?""",
                (status, self.clock(), error, self.settings.group_id, threshold),
            )

    def status(self) -> dict[str, Any]:
        interval = self.settings.milestone_interval
        state = self.db.execute(
            "SELECT last_count FROM milestone_state WHERE group_id=?", (self.settings.group_id,),
        ).fetchone()
        latest = self.db.execute(
            """SELECT threshold, observed_count, status, created_at, finished_at, error
               FROM milestone_announcements WHERE group_id=? ORDER BY threshold DESC LIMIT 1""",
            (self.settings.group_id,),
        ).fetchone()
        total = self.total_count()
        last_count = int(state[0]) if state is not None else total
        return {
            "interval": interval,
            "unique_count": total,
            "next_threshold": (last_count // interval + 1) * interval if interval else None,
            "latest_announcement": dict(latest) if latest is not None else None,
        }
