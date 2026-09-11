import asyncio
import hashlib
import json
import sys
from pathlib import Path

from test_plugins import plugin, settings, stage

from ptilopsisbot.collector import Collector
from ptilopsisbot.config import PluginConfig, PluginMaintenance
from ptilopsisbot.plugins import PluginMaintenanceRunner, fingerprint


def test_legacy_receipt_fingerprint_stays_identical(tmp_path: Path) -> None:
    cfg = plugin(tmp_path)
    old = {
        "id": cfg.id,
        "version": cfg.version,
        "command": cfg.command,
        "timeout_seconds": cfg.timeout_seconds,
        "required_for_cleanup": cfg.required_for_cleanup,
    }
    expected = hashlib.sha256(
        json.dumps(old, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()
    assert fingerprint(cfg) == expected


async def test_background_tick_runs_without_new_uploads_and_recovers(tmp_path: Path) -> None:
    cfg = plugin(tmp_path)
    cfg = cfg.model_copy(
        update={"maintenance": PluginMaintenance(command=cfg.command, interval_seconds=1)}
    )
    collector = Collector(settings(tmp_path, cfg))
    try:
        manager = PluginMaintenanceRunner(collector.db, collector.settings.data_dir, [cfg])
        assert await manager.process_once()
        assert manager.records()[0]["status"] == "ok"
        assert not await manager.process_once()
        with collector.db:
            collector.db.execute(
                "UPDATE plugin_maintenance SET status='running',next_attempt=99999999999"
            )
        restarted = PluginMaintenanceRunner(collector.db, collector.settings.data_dir, [cfg])
        assert await restarted.process_once()
        assert restarted.records()[0]["attempts"] == 2
        disabled = PluginMaintenanceRunner(collector.db, collector.settings.data_dir, [])
        assert not await disabled.process_once() and disabled.records() == []
    finally:
        collector.close()


async def test_slow_background_does_not_block_receipts_and_stops_with_host(
    tmp_path: Path, monkeypatch
) -> None:
    cfg = plugin(tmp_path)
    script = tmp_path / "slow-background.py"
    marker = tmp_path / "started"
    script.write_text(
        "import json,sys,time\nfrom pathlib import Path\n"
        f'json.load(sys.stdin)\nPath({str(marker)!r}).write_text("started")\ntime.sleep(60)\n',
        encoding="utf-8",
    )
    cfg = cfg.model_copy(
        update={"maintenance": PluginMaintenance(command=[sys.executable, str(script)])}
    )
    processes = []
    original = asyncio.create_subprocess_exec

    async def tracked(*args, **kwargs):
        process = await original(*args, **kwargs)
        if str(script) in args:
            processes.append(process)
        return process

    monkeypatch.setattr(asyncio, "create_subprocess_exec", tracked)
    collector = Collector(settings(tmp_path, cfg))
    manager = PluginMaintenanceRunner(collector.db, collector.settings.data_dir, [cfg])
    task = asyncio.create_task(manager.process_once())
    try:

        async def started():
            while not marker.exists():
                await asyncio.sleep(0.02)

        await asyncio.wait_for(started(), 5)
        record = stage(collector)
        collector.recover_archives()
        assert await asyncio.wait_for(collector.plugins.process_once(), 5)
        assert collector.plugins.cleanup_ready(record)
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        assert processes and processes[0].returncode is not None
        assert manager.records()[0]["status"] == "retry"
        collector.close()


async def test_missing_background_program_is_visible_and_retryable(tmp_path: Path) -> None:
    cfg = plugin(tmp_path)
    cfg = PluginConfig.model_validate(
        {
            **cfg.model_dump(),
            "maintenance": {"command": [str(tmp_path / "absent.exe")], "interval_seconds": 1},
        }
    )
    collector = Collector(settings(tmp_path, cfg))
    try:
        manager = PluginMaintenanceRunner(collector.db, collector.settings.data_dir, [cfg])
        assert await manager.process_once()
        assert manager.records()[0]["status"] == "retry"
        assert not await manager.process_once()
    finally:
        collector.close()
