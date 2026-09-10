import asyncio
import logging
import re
import shutil
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import partial
from typing import Literal, Protocol
from zoneinfo import ZoneInfo

import httpx

from . import messages
from .config import Settings
from .files import (
    CollectionError,
    InvalidPackage,
    SourceFileMissing,
    StorageLimitSatisfied,
    check_zip,
    download,
    file_check,
    file_matches,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
log = logging.getLogger(__name__)


class OfflineError(ConnectionError):
    pass


class GroupAPI(Protocol):
    @property
    def online(self) -> bool: ...

    async def file_url(self, file_id: str, busid: int) -> str: ...

    async def list_uploads(self) -> list["Upload"]: ...

    async def storage_usage(self) -> int: ...

    async def delete_file(
        self, file_id: str, busid: int, *, expected_hash: str,
        can_delete: Callable[[], bool] | None = None,
        storage_limit_bytes: int | None = None,
    ) -> None: ...

    async def delete_duplicates(
        self, file_id: str, busid: int, *, expected_hash: str,
        can_delete: Callable[[], bool],
    ) -> int: ...

    async def send_message(self, text: str) -> None: ...

    async def send_receipt(self, file_id: str, busid: int, text: str) -> bool: ...


@dataclass(frozen=True)
class Upload:
    group_id: int
    file_id: str
    busid: int
    uploader_id: int
    name: str
    size: int
    uploaded_at: float
    nickname: str = ""
    from_scan: bool = False
    time_source: str = "qq"
    live: bool = False
    source_kind: Literal["unknown", "history", "live"] = "unknown"
    modified_at: float = 0
    has_duplicates: bool = False


class Collector:
    def __init__(
        self,
        settings: Settings,
        *,
        api: GroupAPI | None = None,
        http: httpx.AsyncClient | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.settings = settings
        self.api = api
        self.http = http
        self.clock = clock
        self.cleanup_lock = asyncio.Lock()
        self.active: dict[int, str] = {}
        self.live_records: set[int] = set()
        self.group_storage_bytes: int | None = None
        self.storage_checked_at: float | None = None
        self.storage_cleanup_error: str | None = None
        settings.data_dir.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(settings.data_dir / "collector.sqlite3", timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS uploads (
                id INTEGER PRIMARY KEY,
                group_id INTEGER NOT NULL,
                file_id TEXT NOT NULL,
                busid INTEGER NOT NULL,
                uploader_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                size INTEGER NOT NULL,
                uploaded_at REAL NOT NULL,
                run_time TEXT NOT NULL,
                nickname TEXT NOT NULL,
                nickname_at REAL NOT NULL,
                from_scan INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL DEFAULT 0,
                last_error TEXT,
                sha256 TEXT,
                archive_path TEXT,
                collected_at REAL,
                deleted_at REAL,
                delete_error TEXT,
                UNIQUE (group_id, file_id, busid)
            )
        """)
        columns = {row["name"] for row in self.db.execute("PRAGMA table_info(uploads)")}
        if "time_source" not in columns:
            self.db.execute("ALTER TABLE uploads ADD COLUMN time_source TEXT NOT NULL DEFAULT 'qq'")
        if "source_kind" not in columns:
            self.db.execute(
                "ALTER TABLE uploads ADD COLUMN source_kind TEXT NOT NULL DEFAULT 'unknown'"
            )
            self.db.execute("UPDATE uploads SET source_kind='live' WHERE time_source='message'")
        if "duplicate_pending" not in columns:
            self.db.execute(
                "ALTER TABLE uploads ADD COLUMN duplicate_pending INTEGER NOT NULL DEFAULT 0"
            )
        if "retention_started_at" not in columns:
            self.db.execute("ALTER TABLE uploads ADD COLUMN retention_started_at REAL")
        if "duplicate_revision" not in columns:
            self.db.execute("ALTER TABLE uploads ADD COLUMN duplicate_revision REAL")
        if "source_missing_at" not in columns:
            self.db.execute("ALTER TABLE uploads ADD COLUMN source_missing_at REAL")
            # Preserve archives and deletion history; migrate only the two known
            # absence diagnostics, without retrying them all on the first restart.
            deferred = self.db.execute(
                """UPDATE uploads SET source_missing_at=?, delete_error=?
                   WHERE status='archived' AND collected_at IS NOT NULL AND deleted_at IS NULL
                   AND delete_error IN (?, ?)""",
                (
                    self.clock(), str(SourceFileMissing()),
                    "根目录无法唯一定位源文件（匹配 0 项），保留记录，等待补扫或人工检查",
                    "根目录找不到重复文件，保留待清理状态",
                ),
            ).rowcount
            if deferred:
                log.info("已将 %s 条源文件缺失的清理记录转为等待补扫", deferred)
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def reset_priority(self) -> None:
        self.live_records.clear()

    def recover_legacy_failures(self) -> int:
        """Give pre-fix NapCat ValueErrors one fresh retry budget, only at startup."""
        with self.db:
            self.db.execute("CREATE TABLE IF NOT EXISTS maintenance (name TEXT PRIMARY KEY)")
            applied = self.db.execute(
                "INSERT OR IGNORE INTO maintenance (name) VALUES (?)",
                (f"serialized-root-files:{self.settings.group_id}",),
            )
            if not applied.rowcount:
                return 0
            restored = self.db.execute(
                """UPDATE uploads SET status='queued', attempts=0, next_attempt_at=0,
                   last_error=NULL WHERE group_id=? AND file_id LIKE 'napcat:%'
                   AND status='failed' AND last_error='ValueError'
                   AND collected_at IS NULL AND deleted_at IS NULL""",
                (self.settings.group_id,),
            ).rowcount
        if restored:
            log.info("已恢复 %s 条旧版 ValueError 失败记录，将按正常规则重新下载", restored)
        return restored

    def recover_invalid_packages(self) -> int:
        """Resume old validation failures using only their remaining attempt budget."""
        with self.db:
            restored = self.db.execute(
                """UPDATE uploads SET status='failed'
                   WHERE group_id=? AND status='invalid' AND attempts<?
                   AND collected_at IS NULL AND deleted_at IS NULL""",
                (self.settings.group_id, self.settings.download_max_attempts),
            ).rowcount
        if restored:
            log.info("已恢复 %s 条 ZIP 检查失败记录，按剩余次数自动重试", restored)
        return restored

    def repair_dates(self) -> None:
        """Repair legacy zero timestamps before the worker starts, including deleted sources."""
        for row in self.db.execute("SELECT * FROM uploads WHERE uploaded_at<=0").fetchall():
            # Legacy rows did not store first_seen; use the earliest retained observation.
            known = [t for t in (row["nickname_at"], row["collected_at"]) if t and t > 0]
            observed = min(known) if known else self.clock()
            relative = row["archive_path"]
            try:
                if relative:
                    day = datetime.fromtimestamp(observed, SHANGHAI).date()
                    corrected = f"archive/{day}/{row['uploader_id']}/{row['id']}.zip"
                    root = self.settings.data_dir.resolve()
                    old = (root / relative).resolve()
                    new = (root / corrected).resolve()
                    if not old.is_relative_to(root) or not new.is_relative_to(root):
                        raise ValueError("归档路径超出数据目录")
                    if old != new:
                        if old.exists():
                            if new.exists():
                                raise FileExistsError("日期修正目标已存在，保留两个文件等待检查")
                            new.parent.mkdir(parents=True, exist_ok=True)
                            old.rename(new)
                        elif not file_matches(new, row["size"], row["sha256"]):
                            raise FileNotFoundError("找不到原归档或上次已移动的完整归档")
                    relative = corrected
                with self.db:
                    self.db.execute(
                        """UPDATE uploads SET uploaded_at=?,time_source='observed',archive_path=?
                           WHERE id=?""",
                        (observed, relative, row["id"]),
                    )
                log.info("已修正上传记录 %s 的缺失日期，按已知发现日期归档", row["id"])
            except (OSError, ValueError) as exc:
                log.error("上传记录 %s 日期修正失败: %s", row["id"], exc)

    def record(self, record_id: int) -> sqlite3.Row:
        row = self.db.execute("SELECT * FROM uploads WHERE id=?", (record_id,)).fetchone()
        if row is None:
            raise ValueError(f"没有上传记录 {record_id}")
        return row

    def records(self) -> list[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM uploads WHERE group_id=? ORDER BY id", (self.settings.group_id,)
        ).fetchall()

    def report(self, day: date) -> str | None:
        start = datetime.combine(day, datetime.min.time(), SHANGHAI).timestamp()
        end = datetime.combine(day + timedelta(days=1), datetime.min.time(), SHANGHAI).timestamp()
        rows = self.db.execute(
            """SELECT * FROM uploads WHERE group_id=? AND collected_at IS NOT NULL
               AND sha256 IS NOT NULL ORDER BY collected_at, id""",
            (self.settings.group_id,),
        ).fetchall()
        first_by_hash: dict[str, sqlite3.Row] = {}
        for row in rows:
            first_by_hash.setdefault(row["sha256"], row)
        total = [row for row in first_by_hash.values() if row["uploaded_at"] < end]
        new = [row for row in total if row["uploaded_at"] >= start]
        if not new:
            return None
        today = [row for row in rows if start <= row["uploaded_at"] < end]
        contributors = []
        for user in sorted({row["uploader_id"] for row in today}):
            latest = self.db.execute(
                """SELECT nickname FROM uploads WHERE group_id=? AND uploader_id=?
                   AND nickname<>'' ORDER BY nickname_at DESC, id DESC LIMIT 1""",
                (self.settings.group_id, user),
            ).fetchone()
            nickname = " ".join(str(latest["nickname"]).split()) if latest else ""
            contributors.append(nickname or str(user))
        text = messages.report(
            day, contributors,
            total_count=len(total), total_size=sum(row["size"] for row in total),
            new_count=len(new), new_size=sum(row["size"] for row in new),
        )
        return text

    async def send_report(self, day: date) -> str | None:
        text = self.report(day)
        if text is None:
            log.info("日报 %s 无新增唯一包，跳过发送", day)
            return None
        if self.api is None:
            raise OfflineError("未连接群消息接口")
        await self.api.send_message(text)
        return text

    def register(self, upload: Upload) -> int | None:
        if (
            upload.group_id != self.settings.group_id
            or not upload.file_id
            or upload.busid < 0
            or upload.uploader_id <= 0
            or not 1 <= upload.size <= self.settings.max_file_mib * 1024**2
            or not re.fullmatch(r"run-\d{8}-\d{6}-\d{6}\.zip", upload.name)
        ):
            return None
        try:
            run_time = datetime.strptime(upload.name, "run-%Y%m%d-%H%M%S-%f.zip")
            datetime.fromtimestamp(upload.uploaded_at, SHANGHAI)
        except (ValueError, OverflowError, OSError):
            return None
        uploaded_at = upload.uploaded_at if upload.uploaded_at > 0 else self.clock()
        time_source = upload.time_source if upload.uploaded_at > 0 else "observed"
        with self.db:
            self.db.execute(
                """INSERT OR IGNORE INTO uploads
                   (group_id,file_id,busid,uploader_id,name,size,uploaded_at,
                    run_time,nickname,nickname_at,from_scan,time_source,source_kind)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    upload.group_id,
                    upload.file_id,
                    upload.busid,
                    upload.uploader_id,
                    upload.name,
                    upload.size,
                    uploaded_at,
                    run_time.replace(tzinfo=SHANGHAI).isoformat(),
                    upload.nickname,
                    self.clock(),
                    upload.from_scan,
                    time_source,
                    "live" if upload.live else upload.source_kind,
                ),
            )
            row = self.db.execute(
                "SELECT * FROM uploads WHERE group_id=? AND file_id=? AND busid=?",
                (upload.group_id, upload.file_id, upload.busid),
            ).fetchone()
            assert row is not None
            if (row["uploader_id"], row["name"], row["size"]) != (
                upload.uploader_id,
                upload.name,
                upload.size,
            ):
                log.warning("上传记录 %s 元数据变化，保留原值", row["id"])
                return int(row["id"])
            if upload.from_scan and row["source_missing_at"] is not None:
                self.db.execute(
                    """UPDATE uploads SET source_missing_at=NULL, delete_error=NULL
                       WHERE id=?""",
                    (row["id"],),
                )
                log.info("上传记录 %s 补扫再次发现源文件，恢复清理核验", row["id"])
            if upload.live or upload.source_kind == "live":
                # Unlike download priority, retention survives receipts, reconnects
                # and restarts. Later observations may promote but never downgrade it.
                self.db.execute(
                    "UPDATE uploads SET source_kind='live' WHERE id=?", (row["id"],)
                )
            if upload.has_duplicates and (
                not row["duplicate_pending"]
                or upload.modified_at > float(row["duplicate_revision"] or 0)
                or (
                    (upload.live or upload.source_kind == "live") and row["source_kind"] != "live"
                )
            ):
                # A new copy can share an old observation key. Protect its retention
                # before any deletion, including across interruption or restart.
                self.db.execute(
                    """UPDATE uploads SET duplicate_pending=1, retention_started_at=?,
                       duplicate_revision=? WHERE id=?""",
                    (self.clock(), upload.modified_at, row["id"]),
                )
            elif upload.from_scan and not upload.has_duplicates:
                self.db.execute("UPDATE uploads SET duplicate_pending=0 WHERE id=?", (row["id"],))
            if upload.from_scan and row["status"] == "archived" and row["deleted_at"] is not None:
                # A fresh, unique root observation overrides an earlier delete ACK.
                # Keep the archive and contribution; cleanup verifies both copies again.
                self.db.execute(
                    """UPDATE uploads SET deleted_at=NULL,
                       delete_error='补扫仍发现源文件，等待重新校验清理' WHERE id=?""",
                    (row["id"],),
                )
                log.warning("上传记录 %s 已标记删除但补扫仍存在，恢复待清理状态", row["id"])
            if upload.nickname:
                self.db.execute(
                    "UPDATE uploads SET nickname=?, nickname_at=? WHERE id=?",
                    (upload.nickname, self.clock(), row["id"]),
                )
            if upload.from_scan and upload.uploaded_at > 0:
                self.db.execute(
                    "UPDATE uploads SET uploaded_at=?, time_source=?, from_scan=1 WHERE id=?",
                    (uploaded_at, time_source, row["id"]),
                )
        if upload.live and row["collected_at"] is None:
            self.live_records.add(int(row["id"]))
        return int(row["id"])

    def can_work(self) -> bool:
        if self.api is None or not self.api.online:
            return False
        if shutil.disk_usage(self.settings.data_dir).free < self.settings.min_free_gib * 1024**3:
            raise OSError("本地剩余空间不足，暂停下载和删源")
        # Exercise an actual write before any destructive remote operation.
        with self.db:
            self.db.execute("UPDATE uploads SET id=id WHERE id=-1")
        return True

    async def process_once(self, *, live_only: bool = False) -> bool:
        if not self.can_work():
            return False
        if (
            sum(phase == "download" for phase in self.active.values())
            >= self.settings.download_concurrency
        ):
            return False
        rows = self.db.execute(
            """SELECT * FROM uploads WHERE group_id=? AND attempts<? AND
               (status='queued' OR (status='failed' AND next_attempt_at<=?))
               ORDER BY id""",
            (self.settings.group_id, self.settings.download_max_attempts, self.clock()),
        ).fetchall()
        eligible = [
            row
            for row in rows
            if row["id"] not in self.active and (not live_only or row["id"] in self.live_records)
        ]
        if not eligible:
            return False
        row = min(eligible, key=lambda item: (item["id"] not in self.live_records, item["id"]))
        record_id = row["id"]
        # Claim before the first await; all database operations stay on this event loop.
        self.active[record_id] = "download"
        try:
            await self._collect(row)
            return True
        finally:
            self.active.pop(record_id, None)
            current = self.record(record_id)
            if (
                current["status"] in ("archived", "invalid")
                or current["attempts"] >= self.settings.download_max_attempts
            ):
                self.live_records.discard(record_id)

    async def _collect(self, row: sqlite3.Row) -> None:
        assert self.api is not None and self.http is not None
        record_id = row["id"]
        previous_attempts = row["attempts"]
        incoming = self.settings.data_dir / "incoming"
        incoming.mkdir(exist_ok=True)
        part = incoming / f"{record_id}.part"
        with self.db:
            self.db.execute(
                "UPDATE uploads SET status='queued', attempts=attempts+1 WHERE id=?",
                (record_id,),
            )
        collected = False
        try:
            url = await self.api.file_url(row["file_id"], row["busid"])
            digest = await download(
                self.http,
                url,
                part,
                row["size"],
                self.settings.max_file_mib * 1024**2,
                label=f"上传记录 {record_id} 下载归档",
            )
            checked_at = time.perf_counter()
            log.info("上传记录 %s 开始 ZIP 检查", record_id)
            await file_check(partial(check_zip, part))
            log.info(
                "上传记录 %s ZIP 检查通过，耗时 %.1f 秒",
                record_id,
                time.perf_counter() - checked_at,
            )
            if row["time_source"] == "observed":
                # The chat event may have arrived while the ZIP was downloading.
                try:
                    uploads = await self.api.list_uploads()
                except Exception as exc:
                    log.warning(
                        "上传记录 %s 时间补正失败 (%s)，保留发现日期",
                        record_id,
                        type(exc).__name__,
                    )
                else:
                    for upload in uploads:
                        if (upload.file_id, upload.busid) == (row["file_id"], row["busid"]):
                            self.register(upload)
                            break
            # The scanner can update dates and nicknames while downloads run.
            row = self.record(record_id)
            day = datetime.fromtimestamp(row["uploaded_at"], SHANGHAI).date()
            relative = f"archive/{day}/{row['uploader_id']}/{record_id}.zip"
            target = self.settings.data_dir / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            part.replace(target)
            with self.db:
                self.db.execute(
                    """UPDATE uploads SET status='archived', sha256=?, archive_path=?,
                       collected_at=COALESCE(collected_at,?), last_error=NULL WHERE id=?""",
                    (digest, relative, self.clock(), record_id),
                )
            log.info("已收集上传记录 %s (%s bytes)", record_id, row["size"])
            collected = True
        except InvalidPackage as exc:
            # A complete response can still contain a transiently unreadable ZIP.
            # Redownload through a fresh source lookup; stop after the same shared
            # budget as transport failures, without archiving or deleting bad bytes.
            exhausted = previous_attempts + 1 >= self.settings.download_max_attempts
            self._failed(record_id, "invalid" if exhausted else "failed", str(exc))
        except asyncio.CancelledError:
            with self.db:
                self.db.execute(
                    "UPDATE uploads SET status='queued', attempts=? WHERE id=?",
                    (previous_attempts, record_id),
                )
            raise
        except (OSError, sqlite3.Error) as exc:
            # Environment failures do not exhaust a file's retry budget.
            with self.db:
                self.db.execute(
                    "UPDATE uploads SET status='queued', attempts=?, last_error=? WHERE id=?",
                    (previous_attempts, type(exc).__name__, record_id),
                )
            raise
        except Exception as exc:
            # Do not persist temporary URLs or tokens from HTTP exception messages.
            error = type(exc).__name__
            if isinstance(exc, CollectionError):
                error = str(exc)
            elif isinstance(exc, httpx.HTTPStatusError):
                error += f" HTTP {exc.response.status_code}"
            self._failed(record_id, "failed", error)
        finally:
            part.unlink(missing_ok=True)
        if collected and row["collected_at"] is None and self.settings.send_receipts:
            # A notice failure must not turn an archived file into a failed download.
            try:
                sent = await self.api.send_receipt(
                    row["file_id"],
                    row["busid"],
                    messages.receipt(),
                )
                if not sent:
                    log.info(
                        "上传记录 %s 已归档，跳过回执（历史补扫或缺少唯一的当前文件消息）",
                        record_id,
                    )
            except Exception as exc:
                log.warning("上传记录 %s 的收包回复发送失败 (%s)", record_id, type(exc).__name__)

    def _failed(self, record_id: int, status: str, error: str) -> None:
        with self.db:
            self.db.execute(
                "UPDATE uploads SET status=?, last_error=?, next_attempt_at=? WHERE id=?",
                (status, error, self.clock() + 300, record_id),
            )
        attempts = self.record(record_id)["attempts"]
        retrying = status == "failed" and attempts < self.settings.download_max_attempts
        log.warning(
            "上传记录 %s: %s (%s)，尝试 %s/%s，%s",
            record_id, status, error, attempts, self.settings.download_max_attempts,
            "5 分钟后自动重试" if retrying else "已停止自动重试，可手动 retry",
        )

    async def cleanup(self) -> None:
        async with self.cleanup_lock:
            await self._cleanup()
            await self._cleanup_capacity()

    async def _measure_storage(self) -> int:
        assert self.api is not None
        try:
            used = await self.api.storage_usage()
            if type(used) is not int or used < 0:
                raise CollectionError("群文件容量无效，暂停容量清理")
        except Exception as exc:
            self.group_storage_bytes = None
            self.storage_cleanup_error = (
                str(exc) if isinstance(exc, CollectionError) else type(exc).__name__
            )
            raise
        else:
            self.group_storage_bytes = used
            self.storage_cleanup_error = None
            return used
        finally:
            self.storage_checked_at = self.clock()

    async def _cleanup_capacity(self) -> None:
        gib = self.settings.group_storage_limit_gib
        if gib is None or not self.settings.auto_delete or not self.can_work():
            return
        limit = int(gib * 1024**3)
        try:
            used = await self._measure_storage()
            if used <= limit:
                return
            log.warning("群文件占用 %.2f GiB，超过 %.2f GiB，开始从旧到新清理已归档包",
                        used / 1024**3, gib)
            rows = self.db.execute(
                """SELECT id FROM uploads WHERE group_id=? AND status='archived'
                   AND deleted_at IS NULL AND collected_at IS NOT NULL
                   AND source_missing_at IS NULL
                   ORDER BY MAX(uploaded_at, COALESCE(retention_started_at, 0)), id""",
                (self.settings.group_id,),
            ).fetchall()
            # Capacity cleanup is sequential: confirm each removal and recount before
            # choosing another file, so parallel workers cannot over-delete below limit.
            for row in rows:
                if not self.can_work():
                    return
                await self._cleanup_record(row["id"], storage_limit_bytes=limit)
                used = await self._measure_storage()
                if used <= limit:
                    log.info("群文件占用已降至 %.2f GiB，结束容量清理", used / 1024**3)
                    return
            self.storage_cleanup_error = "群文件仍超过容量阈值，当前没有更多可安全清理的已归档包"
            log.warning(self.storage_cleanup_error)
        except (OSError, sqlite3.Error):
            raise
        except Exception as exc:
            # A failed/incomplete count must never authorize retention bypass.
            self.storage_cleanup_error = (
                str(exc) if isinstance(exc, CollectionError) else type(exc).__name__
            )
            log.warning("容量清理暂停：%s", self.storage_cleanup_error)

    def delete_after(self, row: sqlite3.Row) -> float | None:
        if row["collected_at"] is None or row["deleted_at"] is not None:
            return None
        grace = self.settings.delete_grace_hours
        if row["source_kind"] == "history" and self.settings.history_delete_grace_hours is not None:
            grace = self.settings.history_delete_grace_hours
        start = max(float(row["collected_at"]), float(row["retention_started_at"] or 0))
        return start + grace * 3600

    def _cleanup_due(self, row: sqlite3.Row) -> bool:
        deadline = self.delete_after(row)
        return row["status"] == "archived" and deadline is not None and deadline <= self.clock()

    async def retry(self, record_id: int) -> None:
        if record_id in self.active:
            raise ValueError("这条记录正在下载或清理，请完成后再重试")
        row = self.record(record_id)
        if row["group_id"] != self.settings.group_id or row["deleted_at"] is not None:
            raise ValueError("只能重试当前群尚未清理源文件的记录")
        with self.db:
            self.db.execute(
                """UPDATE uploads SET status='queued', attempts=0, next_attempt_at=0,
                   last_error=NULL, delete_error=NULL, source_missing_at=NULL WHERE id=?""",
                (record_id,),
            )

    async def _cleanup(self) -> None:
        if not self.settings.auto_delete or not self.can_work():
            return
        assert self.api is not None
        rows = self.db.execute(
            """SELECT * FROM uploads WHERE group_id=? AND status='archived'
               AND deleted_at IS NULL AND collected_at IS NOT NULL
               AND source_missing_at IS NULL ORDER BY id""",
            (self.settings.group_id,),
        ).fetchall()
        # A fixed worker pool bounds both verification work and claimed records.
        # The outer cleanup_lock prevents overlapping rounds from duplicating work.
        candidates = iter(rows)

        async def worker() -> None:
            for candidate in candidates:
                if not self.can_work():
                    return
                await self._cleanup_record(candidate["id"])

        workers = [
            asyncio.create_task(worker())
            for _ in range(min(self.settings.cleanup_concurrency, len(rows)))
        ]
        try:
            await asyncio.gather(*workers)
        except BaseException:
            # gather does not cancel siblings when one worker raises. Drain all
            # children before the runtime can close SQLite or its HTTP client.
            for task in workers:
                if not task.done() and not task.cancelling():
                    task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            raise

    async def _cleanup_record(
        self, record_id: int, *, storage_limit_bytes: int | None = None,
    ) -> None:
        if record_id in self.active:
            return
        row = self.record(record_id)
        if row["status"] != "archived" or row["deleted_at"] is not None:
            return
        if row["collected_at"] is None or row["source_missing_at"] is not None:
            return
        if (
            storage_limit_bytes is None
            and not self._cleanup_due(row) and not row["duplicate_pending"]
        ):
            return
        assert self.api is not None
        self.active[record_id] = "cleanup"
        try:
            path = self.settings.data_dir / row["archive_path"]
            checked_at = time.perf_counter()
            log.info("上传记录 %s 开始删源前本地归档校验", row["id"])
            if not await file_check(partial(file_matches, path, row["size"], row["sha256"])):
                raise CollectionError("本地文件不存在或大小/哈希不符，请重试此上传记录")
            log.info(
                "上传记录 %s 本地归档校验通过，耗时 %.1f 秒",
                row["id"],
                time.perf_counter() - checked_at,
            )
            if not self.can_work():
                return
            if self.record(record_id)["duplicate_pending"]:
                removed = await self.api.delete_duplicates(
                    row["file_id"], row["busid"], expected_hash=row["sha256"],
                    can_delete=lambda: self.settings.auto_delete and self.can_work(),
                )
                with self.db:
                    self.db.execute(
                        "UPDATE uploads SET duplicate_pending=0, delete_error=NULL WHERE id=?",
                        (record_id,),
                    )
                log.info(
                    "上传记录 %s 已清理 %s 个相同内容的旧副本，保留最新一份", record_id, removed
                )
            if storage_limit_bytes is None:
                if not self._cleanup_due(self.record(record_id)):
                    return
                await self.api.delete_file(
                    row["file_id"], row["busid"], expected_hash=row["sha256"],
                    can_delete=lambda: (
                        self.can_work() and self._cleanup_due(self.record(record_id))
                    ),
                )
            else:
                await self.api.delete_file(
                    row["file_id"], row["busid"], expected_hash=row["sha256"],
                    can_delete=lambda: self.settings.auto_delete and self.can_work(),
                    storage_limit_bytes=storage_limit_bytes,
                )
            with self.db:
                self.db.execute(
                    "UPDATE uploads SET deleted_at=?, delete_error=NULL WHERE id=?",
                    (self.clock(), row["id"]),
                )
            log.info("已清理上传记录 %s 的源群文件", row["id"])
        except SourceFileMissing as exc:
            with self.db:
                self.db.execute(
                    "UPDATE uploads SET source_missing_at=?, delete_error=? WHERE id=?",
                    (self.clock(), str(exc), record_id),
                )
            log.info("上传记录 %s 暂停删源：%s", record_id, exc)
        except StorageLimitSatisfied as exc:
            log.info("上传记录 %s 保留：%s", row["id"], exc)
        except (sqlite3.Error, OSError):
            raise
        except Exception as exc:
            error = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
            with self.db:
                self.db.execute(
                    "UPDATE uploads SET delete_error=? WHERE id=?", (error, row["id"])
                )
            log.warning("上传记录 %s 未清理: %s", row["id"], error)
        finally:
            self.active.pop(record_id, None)
