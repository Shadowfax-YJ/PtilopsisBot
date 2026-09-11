"""Durable, at-least-once delivery to explicitly configured local commands."""

import asyncio
import hashlib
import json
import os
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

from .config import PluginCommand, PluginConfig


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
                    "size": row["size"],
                    "sha256": row["sha256"],
                },
            }
            self.db.execute(
                """INSERT INTO plugin_events
                (key,record_id,plugin_id,config_hash,request) VALUES (?,?,?,?,?)
                ON CONFLICT(key) DO UPDATE SET request=excluded.request""",
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
            return True


class PluginMaintenanceRunner:
    """Run optional bounded background batches under the existing bot lifecycle."""

    def __init__(self, db: sqlite3.Connection, root: Path, configs: list[PluginConfig]) -> None:
        self.db, self.root = db, root.resolve()
        self.configs = {fingerprint(config): config for config in configs if config.maintenance}
        self.lock = asyncio.Lock()
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

        async def bounded(stream: asyncio.StreamReader) -> bytes:
            data = bytearray()
            while chunk := await stream.read(65536):
                data.extend(chunk)
                if len(data) > 1024 * 1024:
                    raise ValueError("plugin output exceeds 1 MiB")
            return bytes(data)

        async def communicate() -> bytes:
            assert process and process.stdin and process.stdout and process.stderr
            process.stdin.write(json.dumps(request).encode() + b"\n")
            await process.stdin.drain()
            process.stdin.close()
            readers = [
                asyncio.create_task(bounded(stream)) for stream in (process.stdout, process.stderr)
            ]
            try:
                stdout, _ = await asyncio.gather(*readers)
            finally:
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
