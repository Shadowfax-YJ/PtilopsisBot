import asyncio
import logging
import re
import shutil
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol
from zoneinfo import ZoneInfo

import httpx

from . import messages
from .config import Settings
from .files import InvalidPackage, check_zip, download, file_matches

SHANGHAI = ZoneInfo("Asia/Shanghai")
log = logging.getLogger(__name__)


class OfflineError(ConnectionError):
    pass


class GroupAPI(Protocol):
    @property
    def online(self) -> bool: ...

    async def file_url(self, file_id: str, busid: int) -> str: ...

    async def list_uploads(self) -> list["Upload"]: ...

    async def delete_file(self, file_id: str, busid: int, *, expected_hash: str) -> None: ...

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
        self.lock = asyncio.Lock()
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
        self.db.commit()

    def close(self) -> None:
        self.db.close()

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

    def report(self, day: date) -> str:
        start = datetime.combine(day, datetime.min.time(), SHANGHAI).timestamp()
        end = datetime.combine(day + timedelta(days=1), datetime.min.time(), SHANGHAI).timestamp()
        rows = self.db.execute(
            "SELECT * FROM uploads WHERE group_id=? AND uploaded_at>=? AND uploaded_at<?",
            (self.settings.group_id, start, end),
        ).fetchall()
        first_by_hash: dict[str, int] = {}
        for row in self.db.execute(
            """SELECT id, sha256 FROM uploads WHERE group_id=? AND collected_at IS NOT NULL
               ORDER BY collected_at, id""",
            (self.settings.group_id,),
        ):
            first_by_hash.setdefault(row["sha256"], row["id"])
        counts: dict[int, list[int]] = {}
        pending = invalid = failed = 0
        for row in rows:
            count = counts.setdefault(row["uploader_id"], [0, 0])
            count[0] += 1
            count[1] += int(first_by_hash.get(row["sha256"]) == row["id"])
            pending += row["status"] == "queued"
            invalid += row["status"] == "invalid"
            failed += row["status"] == "failed"
        lines = []
        for user, (uploads, unique) in sorted(
            counts.items(), key=lambda item: (-item[1][1], item[0])
        ):
            latest = self.db.execute(
                """SELECT nickname FROM uploads WHERE group_id=? AND uploader_id=?
                   AND nickname<>'' ORDER BY nickname_at DESC, id DESC LIMIT 1""",
                (self.settings.group_id, user),
            ).fetchone()
            nickname = " ".join(str(latest["nickname"]).split()) if latest else str(user)
            lines.append(f"{nickname}（{user}）：上传 {uploads}，新增唯一包 {unique}。")
        text = messages.report(day, lines, pending=pending, invalid=invalid, failed=failed)
        observed = sum(row["time_source"] == "observed" for row in rows)
        if observed:
            text += f"\n日期说明：{observed} 次记录缺少上传时间，按发现日期统计。"
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
                    run_time,nickname,nickname_at,from_scan,time_source)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
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

    async def process_once(self) -> bool:
        async with self.lock:
            if not self.can_work():
                return False
            assert self.api is not None and self.http is not None
            row = self.db.execute(
                """SELECT * FROM uploads WHERE group_id=? AND
                   (status='queued' OR (status='failed' AND attempts<3 AND next_attempt_at<=?))
                   ORDER BY id LIMIT 1""",
                (self.settings.group_id, self.clock()),
            ).fetchone()
            if row is None:
                return False
            record_id = row["id"]
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
                await asyncio.to_thread(check_zip, part)
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
                                row = self.record(record_id)
                                break
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
                self._failed(record_id, "invalid", str(exc))
            except (OSError, sqlite3.Error) as exc:
                # Environment failures do not exhaust a file's retry budget.
                with self.db:
                    self.db.execute(
                        "UPDATE uploads SET status='queued', attempts=?, last_error=? WHERE id=?",
                        (row["attempts"], type(exc).__name__, record_id),
                    )
                raise
            except Exception as exc:
                # Do not persist temporary URLs or tokens from HTTP exception messages.
                error = type(exc).__name__
                if isinstance(exc, httpx.HTTPStatusError):
                    error += f" HTTP {exc.response.status_code}"
                self._failed(record_id, "failed", error)
            finally:
                part.unlink(missing_ok=True)
            if collected and row["collected_at"] is None:
                # A notice failure must not turn an archived file into a failed download.
                try:
                    sent = await self.api.send_receipt(
                        row["file_id"],
                        row["busid"],
                        messages.receipt(row["name"], row["uploader_id"], row["nickname"]),
                    )
                    if not sent:
                        log.info(
                            "上传记录 %s 已归档，跳过回执（历史补扫或缺少唯一的当前文件消息）",
                            record_id,
                        )
                except Exception as exc:
                    log.warning(
                        "上传记录 %s 的收包回复发送失败 (%s)", record_id, type(exc).__name__
                    )
            await self._cleanup(record_id)
            return True

    def _failed(self, record_id: int, status: str, error: str) -> None:
        with self.db:
            self.db.execute(
                "UPDATE uploads SET status=?, last_error=?, next_attempt_at=? WHERE id=?",
                (status, error, self.clock() + 300, record_id),
            )
        log.warning("上传记录 %s: %s (%s)", record_id, status, error)

    async def cleanup(self) -> None:
        async with self.lock:
            await self._cleanup()

    async def retry(self, record_id: int) -> None:
        async with self.lock:
            row = self.record(record_id)
            if row["group_id"] != self.settings.group_id or row["deleted_at"] is not None:
                raise ValueError("只能重试当前群尚未清理源文件的记录")
            with self.db:
                self.db.execute(
                    """UPDATE uploads SET status='queued', attempts=0, next_attempt_at=0,
                       last_error=NULL, delete_error=NULL WHERE id=?""",
                    (record_id,),
                )

    async def _cleanup(self, record_id: int | None = None) -> None:
        if not self.settings.auto_delete or not self.can_work():
            return
        assert self.api is not None
        rows = self.db.execute(
            """SELECT * FROM uploads WHERE group_id=? AND status='archived'
               AND deleted_at IS NULL AND collected_at<=? AND (? IS NULL OR id=?)""",
            (
                self.settings.group_id,
                self.clock() - self.settings.delete_grace_hours * 3600,
                record_id,
                record_id,
            ),
        ).fetchall()
        for row in rows:
            try:
                path = self.settings.data_dir / row["archive_path"]
                checked_at = time.perf_counter()
                log.info("上传记录 %s 开始删源前本地归档校验", row["id"])
                if not await asyncio.to_thread(file_matches, path, row["size"], row["sha256"]):
                    raise ValueError("本地文件不存在或大小/哈希不符，请重试此上传记录")
                log.info(
                    "上传记录 %s 本地归档校验通过，耗时 %.1f 秒",
                    row["id"],
                    time.perf_counter() - checked_at,
                )
                if not self.can_work():
                    return
                await self.api.delete_file(
                    row["file_id"], row["busid"], expected_hash=row["sha256"]
                )
                with self.db:
                    self.db.execute(
                        "UPDATE uploads SET deleted_at=?, delete_error=NULL WHERE id=?",
                        (self.clock(), row["id"]),
                    )
                log.info("已清理上传记录 %s 的源群文件", row["id"])
            except (sqlite3.Error, OSError):
                raise
            except Exception as exc:
                error = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
                with self.db:
                    self.db.execute(
                        "UPDATE uploads SET delete_error=? WHERE id=?", (error, row["id"])
                    )
                log.warning("上传记录 %s 未清理: %s", row["id"], error)
