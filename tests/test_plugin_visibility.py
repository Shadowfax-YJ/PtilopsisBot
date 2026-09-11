import asyncio
import logging
import sys

from test_plugins import plugin, settings, stage

from ptilopsisbot import plugins
from ptilopsisbot.collector import Collector
from ptilopsisbot.config import PluginMaintenance


def test_receipt_only_configuration_reports_background_disabled(tmp_path, caplog):
    cfg = plugin(tmp_path)
    collector = Collector(settings(tmp_path, cfg))
    try:
        with caplog.at_level(logging.INFO, logger="ptilopsisbot.plugins"):
            runner = plugins.PluginMaintenanceRunner(
                collector.db, collector.settings.data_dir, [cfg]
            )
        assert not runner.configs
        assert "未启用后台处理" in caplog.text
    finally:
        collector.close()


async def test_legacy_background_success_is_visible_once_when_plugin_has_no_progress(
    tmp_path, caplog
):
    cfg = plugin(tmp_path)
    cfg = cfg.model_copy(update={"maintenance": PluginMaintenance(command=cfg.command)})
    collector = Collector(settings(tmp_path, cfg))
    try:
        with caplog.at_level(logging.INFO, logger="ptilopsisbot.plugins"):
            runner = plugins.PluginMaintenanceRunner(
                collector.db, collector.settings.data_dir, [cfg]
            )
            assert await runner.process_once()
            with collector.db:
                collector.db.execute("UPDATE plugin_maintenance SET next_attempt=0")
            assert await runner.process_once()
        assert "后台处理已启用" in caplog.text
        assert caplog.text.count("后台处理结果：ok") == 1
    finally:
        collector.close()


async def test_archive_backfill_summarizes_successes_instead_of_logging_every_receipt(
    tmp_path, monkeypatch, caplog
):
    cfg = plugin(tmp_path)
    collector = Collector(settings(tmp_path, cfg))
    try:
        record = stage(collector)
        collector.recover_archives()
        row = dict(collector.record(record))
        with collector.db:
            for index in range(1, 20):
                collector.plugins.enqueue({**row, "id": record + index})

        async def invoke(config, request):
            return {"status": "ok", "event_id": request["event_id"], "receipt": {"durable": True}}

        monkeypatch.setattr(plugins, "invoke", invoke)
        with caplog.at_level(logging.INFO, logger="ptilopsisbot.plugins"):
            while await collector.plugins.process_once():
                pass
        messages = [r.message for r in caplog.records if r.levelno >= logging.INFO]
        assert len(messages) <= 3
        assert any("交接汇总" in m and "20" in m for m in messages)
        assert len(collector.plugins.records()) == 20
    finally:
        collector.close()


async def test_silent_background_reports_running_before_process_finishes(
    tmp_path, monkeypatch, caplog
):
    cfg = plugin(tmp_path)
    script = tmp_path / "silent.py"
    script.write_text(
        "import json,sys,time\nr=json.load(sys.stdin)\ntime.sleep(60)\n", encoding="utf-8"
    )
    cfg = cfg.model_copy(
        update={"maintenance": PluginMaintenance(command=[sys.executable, str(script)])}
    )
    monkeypatch.setattr(plugins, "PROGRESS_HEARTBEAT_SECONDS", 0.02, raising=False)
    collector = Collector(settings(tmp_path, cfg))
    task = None
    try:
        with caplog.at_level(logging.INFO, logger="ptilopsisbot.plugins"):
            runner = plugins.PluginMaintenanceRunner(
                collector.db, collector.settings.data_dir, [cfg]
            )
            task = asyncio.create_task(runner.process_once())

            async def observed():
                while "后台处理仍在运行" not in caplog.text:
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(observed(), 2)
            assert not task.done()
    finally:
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        collector.close()
