"""Durable, at-least-once delivery to explicitly configured local commands."""

import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

from .config import PluginCommand, PluginConfig

log = logging.getLogger(__name__)
PROGRESS_PREFIX = b"PLUGIN_PROGRESS "
PROGRESS_HEARTBEAT_SECONDS = 15


def fingerprint(config: PluginConfig) -> str:
    # Keep existing receipt identities stable when the optional capability is absent.
    value = {
        key: getattr(config, key)
        for key in ("id", "version", "command", "timeout_seconds", "required_for_cleanup")
    }
    if config.maintenance is not None:
        value["maintenance"] = config.maintenance.model_dump()
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


class PluginOutbox:
    def __init__(self, db: sqlite3.Connection, root: Path, configs: list[PluginConfig]) -> None:
        self.db, self.root, self.configs = db, root.resolve(), configs
        self.lock = asyncio.Lock()
        self.receipt_counts: dict[str, int] = {}
        self.receipt_totals: dict[str, int] = {}
        self.receipt_last_log: dict[str, float] = {}
        db.execute("""CREATE TABLE IF NOT EXISTS plugin_events (
            key TEXT PRIMARY KEY, record_id INTEGER NOT NULL, plugin_id TEXT NOT NULL,
            config_hash TEXT NOT NULL, request TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
            attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
            result TEXT, updated_at REAL NOT NULL DEFAULT 0)""")
        db.execute("UPDATE plugin_events SET status='retry' WHERE status='running'")
        db.commit()

    def enqueue(self, row: sqlite3.Row) -> None:
        """Called inside the upload commit transaction; never commits itself."""
        event_id = f"archive:{row['id']}:{row['sha256']}"
        for config in self.configs:
            digest = fingerprint(config)
            key = hashlib.sha256(f"{event_id}:{digest}".encode()).hexdigest()
            request = {
                "protocol_version": 1,
                "plugin_id": config.id,
                "plugin_version": config.version,
                "event_id": event_id,
                "hook": "archive.committed",
                "input": {
                    "archive_id": str(row["id"]),
                    "root": str(self.root),
                    "path": row["archive_path"],
                    "original_name": row["name"],
                    "archive_relative_path": (
                        Path(row["archive_path"]).relative_to("archive").as_posix()
                    ),
                    "retained_locally": True,
                    "size": row["size"],
                    "sha256": row["sha256"],
                },
            }
            self.db.execute(
                """INSERT INTO plugin_events
                (key,record_id,plugin_id,config_hash,request) VALUES (?,?,?,?,?)
                ON CONFLICT(key) DO UPDATE SET request=excluded.request,
                status=CASE WHEN json_extract(plugin_events.request,'$.input')
                    IS NOT json_extract(excluded.request,'$.input')
                    THEN 'queued' ELSE plugin_events.status END,
                next_attempt=CASE WHEN json_extract(plugin_events.request,'$.input')
                    IS NOT json_extract(excluded.request,'$.input')
                    THEN 0 ELSE plugin_events.next_attempt END""",
                (key, row["id"], config.id, digest, json.dumps(request)),
            )

    def cleanup_ready(self, record_id: int) -> bool:
        upload = self.db.execute("SELECT sha256 FROM uploads WHERE id=?", (record_id,)).fetchone()
        if not upload:
            return False
        for config in self.configs:
            if not config.required_for_cleanup:
                continue
            event_id = f"archive:{record_id}:{upload['sha256']}"
            key = hashlib.sha256(f"{event_id}:{fingerprint(config)}".encode()).hexdigest()
            row = self.db.execute(
                "SELECT status,result FROM plugin_events WHERE key=?", (key,)
            ).fetchone()
            if not row or row["status"] != "ok":
                return False
            receipt = json.loads(row["result"]).get("receipt")
            if not isinstance(receipt, dict) or receipt.get("durable") is not True:
                return False
        return True

    def records(self) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.db.execute("""SELECT key,record_id,plugin_id,
            status,attempts,next_attempt,result,updated_at FROM plugin_events
            ORDER BY updated_at DESC,rowid DESC LIMIT 100""")
        ]

    def retry(self, record_id: int) -> None:
        with self.db:
            self.db.execute(
                """UPDATE plugin_events SET status='retry',next_attempt=0
                WHERE record_id=? AND status != 'running'""",
                (record_id,),
            )

    async def process_once(self) -> bool:
        async with self.lock:
            allowed = {fingerprint(config): config for config in self.configs}
            row = next(
                (
                    row
                    for row in self.db.execute(
                        """SELECT * FROM plugin_events
                WHERE status IN ('queued','retry') AND next_attempt<=? ORDER BY rowid""",
                        (time.time(),),
                    )
                    if row["config_hash"] in allowed
                ),
                None,
            )
            if row is None:
                for config in self.configs:
                    self.log_receipts(config, force=True)
                return False
            config = allowed[row["config_hash"]]
            request = json.loads(row["request"])
            request.update(
                invocation_id=str(uuid.uuid4()), deadline=time.time() + config.timeout_seconds
            )
            state = self.root / "plugin-state" / config.id
            state.mkdir(parents=True, exist_ok=True)
            request["state_dir"] = str(state)
            with self.db:
                self.db.execute(
                    """UPDATE plugin_events SET status='running',attempts=attempts+1
                    WHERE key=?""",
                    (row["key"],),
                )
            try:
                result = await invoke(config, request)
            except asyncio.CancelledError:
                with self.db:
                    self.db.execute(
                        "UPDATE plugin_events SET status='retry' WHERE key=?", (row["key"],)
                    )
                raise
            with self.db:
                self.db.execute(
                    """UPDATE plugin_events SET status=?,result=?,updated_at=?,
                    next_attempt=? WHERE key=?""",
                    (
                        result["status"],
                        json.dumps(result),
                        time.time(),
                        time.time() + min(3600, 5 * 2 ** min(row["attempts"], 9)),
                        row["key"],
                    ),
                )
            if result["status"] == "ok":
                self.receipt_counts[config.id] = self.receipt_counts.get(config.id, 0) + 1
                self.receipt_totals[config.id] = self.receipt_totals.get(config.id, 0) + 1
                self.log_receipts(config)
                log.debug("[%s] 原包交接 %s：%s", config.id,
                          request["input"]["path"], result.get("message", "ok"))
                warnings = result.get("warnings", [])
                if isinstance(warnings, list):
                    for warning in warnings[:10]:
                        if isinstance(warning, str):
                            log.warning("[%s] %s", config.id, warning[:2000])
            else:
                log.warning("[%s] 原包交接 %s：%s；%s", config.id,
                            request["input"]["path"], result["status"], result.get("message", ""))
            return True

    def log_receipts(self, config: PluginConfig, *, force: bool = False) -> None:
        count = self.receipt_counts.get(config.id, 0)
        now = time.monotonic()
        if not count or (not force and now - self.receipt_last_log.get(config.id, now) < 5):
            self.receipt_last_log.setdefault(config.id, now)
            return
        pending = self.db.execute(
            "SELECT count(*) FROM plugin_events WHERE config_hash=? "
            "AND status IN ('queued','retry','running')",
            (fingerprint(config),),
        ).fetchone()[0]
        log.info("[%s] 交接汇总：新增成功 %s 个，本次启动累计 %s 个，待交接 %s 个；"
                 "后台处理单独报告",
                 config.id, count, self.receipt_totals[config.id], pending)
        self.receipt_counts[config.id] = 0
        self.receipt_last_log[config.id] = now


class PluginMaintenanceRunner:
    """Run optional bounded background batches under the existing bot lifecycle."""

    def __init__(self, db: sqlite3.Connection, root: Path, configs: list[PluginConfig]) -> None:
        self.db, self.root = db, root.resolve()
        self.configs = {fingerprint(config): config for config in configs if config.maintenance}
        self.lock = asyncio.Lock()
        self.last_results: dict[str, tuple[str, str]] = {}
        for config in configs:
            if config.maintenance:
                log.info("[%s] 后台处理已启用；配置版本 %s；每 %s 秒检查，单批超时 %s 秒",
                         config.id, config.version, config.maintenance.interval_seconds,
                         config.maintenance.timeout_seconds)
            else:
                log.warning("[%s] 当前仅配置归档交接，未启用后台处理；该插件不会自动处理积压数据",
                            config.id)
        db.execute("""CREATE TABLE IF NOT EXISTS plugin_maintenance (
            config_hash TEXT PRIMARY KEY, plugin_id TEXT NOT NULL, status TEXT NOT NULL,
            attempts INTEGER NOT NULL DEFAULT 0, next_attempt REAL NOT NULL DEFAULT 0,
            result TEXT, updated_at REAL NOT NULL DEFAULT 0)""")
        with db:
            for digest, config in self.configs.items():
                db.execute(
                    """INSERT OR IGNORE INTO plugin_maintenance
                    (config_hash,plugin_id,status) VALUES (?,?,'queued')""",
                    (digest, config.id),
                )
                db.execute(
                    """UPDATE plugin_maintenance SET status='retry',next_attempt=0
                    WHERE config_hash=? AND status='running'""",
                    (digest,),
                )

    def records(self) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.db.execute("SELECT * FROM plugin_maintenance")
            if row["config_hash"] in self.configs
        ]

    async def process_once(self) -> bool:
        async with self.lock:
            row = next(
                (
                    row
                    for row in self.db.execute(
                        "SELECT * FROM plugin_maintenance WHERE next_attempt<=? "
                        "ORDER BY next_attempt",
                        (time.time(),),
                    )
                    if row["config_hash"] in self.configs
                ),
                None,
            )
            if row is None:
                return False
            config = self.configs[row["config_hash"]]
            command = config.maintenance
            assert command is not None
            state = self.root / "plugin-state" / config.id
            state.mkdir(parents=True, exist_ok=True)
            event_id = f"maintenance:{row['config_hash']}:{row['attempts'] + 1}"
            request = {
                "protocol_version": 1,
                "plugin_id": config.id,
                "plugin_version": config.version,
                "event_id": event_id,
                "invocation_id": str(uuid.uuid4()),
                "hook": "maintenance.tick",
                "deadline": time.time() + command.timeout_seconds,
                "state_dir": str(state),
                "input": {},
            }
            with self.db:
                self.db.execute(
                    """UPDATE plugin_maintenance SET status='running',
                    attempts=attempts+1 WHERE config_hash=?""",
                    (row["config_hash"],),
                )
            try:
                if row['config_hash'] not in self.last_results:
                    log.info("[%s] 后台处理开始", config.id)
                result = await invoke(command, request)
            except asyncio.CancelledError:
                with self.db:
                    self.db.execute(
                        """UPDATE plugin_maintenance SET status='retry',next_attempt=0
                        WHERE config_hash=?""",
                        (row["config_hash"],),
                    )
                raise
            with self.db:
                self.db.execute(
                    """UPDATE plugin_maintenance SET status=?,result=?,updated_at=?,
                    next_attempt=? WHERE config_hash=?""",
                    (
                        result["status"],
                        json.dumps(result),
                        time.time(),
                        time.time() + command.interval_seconds,
                        row["config_hash"],
                    ),
                )
            if result["status"] != "ok":
                log.warning("[%s] 后台任务 %s：%s", config.id, result["status"],
                            result.get("message", ""))
            summary = (result['status'], str(result.get('message', '')))
            if result['status'] == 'ok' and self.last_results.get(row['config_hash']) != summary:
                log.info("[%s] 后台处理结果：%s；%s", config.id, *summary)
            self.last_results[row['config_hash']] = summary
            return True


async def invoke(config: PluginCommand, request: dict[str, Any]) -> dict[str, Any]:
    process = None
    try:
        # No QQ token, signed URL, or inherited service credentials in the envelope/environment.
        env = {
            key: value
            for key, value in os.environ.items()
            if key.upper()
            in {
                "PATH",
                "SYSTEMROOT",
                "WINDIR",
                "TEMP",
                "TMP",
                "HOME",
                "USERPROFILE",
                "LOCALAPPDATA",
            }
        }
        env["PYTHONIOENCODING"] = "utf-8"
        process = await asyncio.create_subprocess_exec(
            *config.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            creationflags=0x08000000 if os.name == "nt" else 0,
        )
        assert process.stdin and process.stdout and process.stderr
        started = last_progress = time.monotonic()

        async def bounded(stream: asyncio.StreamReader, *, progress: bool = False) -> bytes:
            nonlocal last_progress
            data = bytearray()
            pending = bytearray()
            while chunk := await stream.read(65536):
                data.extend(chunk)
                if len(data) > 1024 * 1024:
                    raise ValueError("plugin output exceeds 1 MiB")
                if progress:
                    pending.extend(chunk)
                    while b"\n" in pending:
                        line, _, rest = pending.partition(b"\n")
                        pending = bytearray(rest)
                        if not line.startswith(PROGRESS_PREFIX):
                            continue
                        try:
                            value = json.loads(line[len(PROGRESS_PREFIX):])
                        except (ValueError, UnicodeError):
                            continue
                        if (isinstance(value, dict) and value.get("event_id") == request["event_id"]
                                and isinstance(value.get("message"), str)):
                            # One bounded, plain log line; no terminal control sequences.
                            message = "".join(c if c.isprintable() else " "
                                              for c in value["message"])[:2000]
                            log.info("[%s] %s", request.get("plugin_id", "plugin"), message)
                            last_progress = time.monotonic()
            return bytes(data)

        async def heartbeat() -> None:
            while True:
                await asyncio.sleep(PROGRESS_HEARTBEAT_SECONDS)
                if time.monotonic() - last_progress >= PROGRESS_HEARTBEAT_SECONDS:
                    log.info("[%s] 后台处理仍在运行，已耗时 %s 秒；等待插件报告阶段进度",
                             request.get('plugin_id', 'plugin'), int(time.monotonic() - started))

        async def communicate() -> bytes:
            assert process and process.stdin and process.stdout and process.stderr
            process.stdin.write(json.dumps(request).encode() + b"\n")
            await process.stdin.drain()
            process.stdin.close()
            readers = [asyncio.create_task(bounded(process.stdout)),
                       asyncio.create_task(bounded(process.stderr, progress=True))]
            pulse = (asyncio.create_task(heartbeat())
                     if request.get('hook') == 'maintenance.tick' else None)
            try:
                stdout, _ = await asyncio.gather(*readers)
            finally:
                if pulse:
                    pulse.cancel()
                    await asyncio.gather(pulse, return_exceptions=True)
                for reader in readers:
                    reader.cancel()
                await asyncio.gather(*readers, return_exceptions=True)
            await process.wait()
            return stdout

        stdout = await asyncio.wait_for(communicate(), config.timeout_seconds)
        if process.returncode != 0:
            raise ValueError("plugin process failed")
        result = json.loads(stdout)
        if (
            not isinstance(result, dict)
            or result.get("protocol_version") != 1
            or result.get("status") not in {"ok", "retry", "unsupported", "rejected"}
            or result.get("event_id") != request["event_id"]
        ):
            raise ValueError("invalid plugin response")
        return result
    except (OSError, ValueError, TimeoutError) as error:
        return {
            "protocol_version": 1,
            "status": "retry",
            "event_id": request["event_id"],
            "error": type(error).__name__,
            "message": "插件进程不可用、超时或协议错误",
        }
    finally:
        if process and process.returncode is None:
            process.kill()
            await process.wait()
