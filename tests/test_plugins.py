import asyncio
import hashlib
import json
import logging
import sys
from pathlib import Path

import pytest

from ptilopsisbot.collector import Collector, Upload
from ptilopsisbot.config import FilePolicy, PluginConfig, Settings, load_settings
from ptilopsisbot.plugins import invoke


def plugin(tmp_path: Path, response: str = "ok") -> PluginConfig:
    script = tmp_path / "plugin.py"
    script.write_text(
        "import json,sys\nr=json.load(sys.stdin)\n"
        "assert 'access_token' not in str(r)\n"
        f"print(json.dumps({{'protocol_version':1,'event_id':r['event_id'],'status':{response!r},"
        "'receipt':{'durable':True}}))\n",
        encoding="utf-8",
    )
    return PluginConfig(
        id="test", version="1", command=[sys.executable, str(script)], required_for_cleanup=True
    )


def settings(tmp_path: Path, config: PluginConfig) -> Settings:
    return Settings(group_id=123, data_dir=tmp_path / "data", plugins=[config])


def stage(collector: Collector, content: bytes = b"archive bytes") -> int:
    record = collector.register(
        Upload(123, "test-file", 1, 456, "run-20260911-110000-123456.zip", len(content), 1789100000)
    )
    assert record
    target = collector.settings.data_dir / "archive/fixture.zip"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(content)
    with collector.db:
        collector.db.execute(
            "INSERT INTO archive_staging VALUES (?,?,?)",
            (record, "archive/fixture.zip", hashlib.sha256(content).hexdigest()),
        )
    return record


async def test_crash_after_rename_recovers_and_deduplicates_outbox(tmp_path: Path) -> None:
    cfg = settings(tmp_path, plugin(tmp_path))
    collector = Collector(cfg)
    record = stage(collector)
    collector.close()
    collector = Collector(cfg)
    assert collector.record(record)["status"] == "archived"
    assert not collector.plugins.cleanup_ready(record)
    assert await collector.plugins.process_once()
    assert collector.plugins.cleanup_ready(record)
    assert len(collector.plugins.records()) == 1
    collector.close()
    collector = Collector(cfg)
    assert not await collector.plugins.process_once()
    assert len(collector.plugins.records()) == 1
    # Replaced content can never reuse a receipt for a previous digest.
    with collector.db:
        collector.db.execute("UPDATE uploads SET sha256=? WHERE id=?", ("0" * 64, record))
    assert not collector.plugins.cleanup_ready(record)
    collector.close()


@pytest.mark.parametrize('missing', ['original_name', 'archive_relative_path'])
async def test_original_metadata_backfills_completed_legacy_event_once(tmp_path, missing):
    cfg = settings(tmp_path, plugin(tmp_path))
    collector = Collector(cfg)
    try:
        stage(collector)
        collector.recover_archives()
        assert await collector.plugins.process_once()
        row = collector.db.execute('SELECT * FROM plugin_events').fetchone()
        request = json.loads(row['request'])
        assert request['input']['original_name'] == 'run-20260911-110000-123456.zip'
        assert request['input']['archive_relative_path'] == 'fixture.zip'
        request['input'].pop(missing)
        with collector.db:
            collector.db.execute('UPDATE plugin_events SET request=?', (json.dumps(request),))
        collector.recover_archives()
        assert await collector.plugins.process_once()
        collector.recover_archives()
        assert not await collector.plugins.process_once()
        assert len(collector.plugins.records()) == 1
    finally:
        collector.close()


async def test_progress_logs_before_plugin_exits_without_polluting_response(tmp_path, caplog):
    release = tmp_path / 'release'
    script = tmp_path / 'progress.py'
    script.write_text(
        'import json,sys,time\nfrom pathlib import Path\nr=json.load(sys.stdin)\n'
        'print("PLUGIN_PROGRESS broken",file=sys.stderr,flush=True)\n'
        'print("PLUGIN_PROGRESS "+json.dumps({"event_id":"wrong","message":"ignore"}),'
        'file=sys.stderr,flush=True)\n'
        'print("PLUGIN_PROGRESS "+json.dumps({"event_id":r["event_id"],'
        '"message":"OCR 3/10\\u001b"}),'
        'file=sys.stderr,flush=True)\n'
        f'while not Path({str(release)!r}).exists(): time.sleep(.01)\n'
        'print(json.dumps({"protocol_version":1,"event_id":r["event_id"],"status":"ok"}))\n',
        encoding='utf-8')
    cfg = PluginConfig(id='progress', version='1', command=[sys.executable, str(script)])
    caplog.set_level(logging.INFO)
    task = asyncio.create_task(invoke(cfg, {'event_id': 'event', 'plugin_id': 'progress'}))
    try:
        async with asyncio.timeout(5):
            while 'OCR 3/10' not in caplog.text:
                await asyncio.sleep(.01)
        assert not task.done()
        assert 'ignore' not in caplog.text and '\x1b' not in caplog.text
    finally:
        release.touch()
        result = await task
    assert result['status'] == 'ok'


async def test_unsupported_retry_and_missing_program_are_separate_from_download(
    tmp_path: Path,
) -> None:
    cfg = settings(tmp_path, plugin(tmp_path, "unsupported"))
    collector = Collector(cfg)
    record = stage(collector)
    collector.recover_archives()
    assert await collector.plugins.process_once()
    assert collector.record(record)["status"] == "archived"
    assert collector.plugins.records()[0]["status"] == "unsupported"
    assert not await collector.plugins.process_once()
    collector.plugins.retry(record)
    assert await collector.plugins.process_once()
    collector.close()
    missing = PluginConfig(id="missing", version="1", command=[str(tmp_path / "missing.exe")])
    assert (await invoke(missing, {"event_id": "event"}))["status"] == "retry"


async def test_process_timeout_and_protocol_identity(tmp_path: Path) -> None:
    script = tmp_path / "slow.py"
    script.write_text("import time;time.sleep(30)", encoding="utf-8")
    cfg = PluginConfig(
        id="slow", version="1", command=[sys.executable, str(script)], timeout_seconds=1
    )
    assert (await invoke(cfg, {"event_id": "event"}))["status"] == "retry"
    script.write_text('print("{}")', encoding="utf-8")
    assert (await invoke(cfg, {"event_id": "event"}))["status"] == "retry"


def test_general_file_policy_and_disabled_plugin_baseline(tmp_path: Path) -> None:
    cfg = Settings(
        group_id=123,
        data_dir=tmp_path,
        file_policy=FilePolicy(name_pattern=r".*\.dat", time_format=None, validation="bytes"),
    )
    collector = Collector(cfg)
    record = collector.register(Upload(123, "test", 1, 456, "sample.dat", 10, 1789100000))
    assert record
    assert collector.plugins.cleanup_ready(record)
    assert collector.register(Upload(123, "bad", 1, 456, "../sample.dat", 10, 1789100000)) is None
    collector.close()


def test_installed_json_config_loads_relative_to_toml(tmp_path: Path) -> None:
    import json

    cfg = plugin(tmp_path)
    (tmp_path / "plugin.json").write_text(json.dumps(cfg.model_dump()), encoding="utf-8")
    config = tmp_path / "config.toml"
    config.write_text(
        'group_id=123\naccess_token="fixture-only"\nplugin_configs=["plugin.json"]\n',
        encoding="utf-8",
    )
    loaded = load_settings(config)
    assert loaded.plugins == [cfg]
    assert loaded.data_dir == tmp_path / "data"


@pytest.mark.parametrize("mode", ["retention", "duplicates", "capacity"])
async def test_all_cleanup_paths_wait_for_durable_plugin_receipt(tmp_path, mode):
    calls = []

    class CleanupAPI:
        online = True

        async def delete_file(self, *args, **kwargs):
            assert kwargs["can_delete"]()
            calls.append("file")

        async def delete_duplicates(self, *args, **kwargs):
            assert kwargs["can_delete"]()
            calls.append("duplicates")
            return 1

    cfg = Settings(group_id=123, data_dir=tmp_path / "data", min_free_gib=0,
                   delete_grace_hours=0, plugins=[plugin(tmp_path)])
    collector = Collector(cfg, api=CleanupAPI())
    try:
        record = stage(collector)
        collector.recover_archives()
        if mode == "duplicates":
            with collector.db:
                collector.db.execute("UPDATE uploads SET duplicate_pending=1 WHERE id=?", (record,))
        limit = 1 if mode == "capacity" else None
        await collector._cleanup_record(record, storage_limit_bytes=limit)
        assert calls == [] and collector.record(record)["deleted_at"] is None
        assert await collector.plugins.process_once()
        await collector._cleanup_record(record, storage_limit_bytes=limit)
        assert calls == (["duplicates", "file"] if mode == "duplicates" else ["file"])
        assert collector.record(record)["deleted_at"] is not None
    finally:
        collector.close()
