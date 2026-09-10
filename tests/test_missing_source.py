import logging
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock

import httpx
import pytest
from test_capacity import NOW, StorageRPC, archive, config
from test_collector import FakeGroup, zip_bytes
from test_napcat import FilesRPC

from ptilopsisbot.collector import Collector, Upload
from ptilopsisbot.config import Settings
from ptilopsisbot.files import SourceFileMissing, file_matches
from ptilopsisbot.napcat import NapCat


@pytest.mark.parametrize("duplicates", [False, True])
@pytest.mark.parametrize("changed_content", [False, True])
async def test_missing_source_waits_across_restart_and_scan_restores_verification(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, monkeypatch: pytest.MonkeyPatch,
    duplicates: bool, changed_content: bool,
) -> None:
    caplog.set_level(logging.INFO)
    body = zip_bytes()
    remote = [body]
    rpc = FilesRPC(len(body))
    original = dict(rpc.files["source-a"])
    if duplicates:
        rpc.files["source-a"]["modify_time"] = NOW - 2
        rpc.files["source-b"] = {**original, "modify_time": NOW - 1}
    now = [NOW]
    settings = Settings(
        group_id=123, data_dir=tmp_path, min_free_gib=0,
        delete_grace_hours=12, send_receipts=False,
    )
    checked = Mock(wraps=file_matches)
    monkeypatch.setattr("ptilopsisbot.collector.file_matches", checked)
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=remote[0])
    )) as http:
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        collector = Collector(settings, api=api, http=http, clock=lambda: now[0])
        try:
            upload = replace((await api.list_uploads())[0], source_kind="live")
            record_id = collector.register(upload)
            assert record_id is not None
            assert await collector.process_once()
            archived = dict(collector.record(record_id))
            deadline = collector.delete_after(collector.record(record_id))
            rpc.files.clear()  # Manual deletion or moving out of the root directory.
            now[0] += 12 * 3600
            await collector.cleanup()
            missing = dict(collector.record(record_id))
            assert missing["source_missing_at"] == now[0]
            assert missing["deleted_at"] is None and missing["status"] == "archived"
            assert missing["delete_error"] == str(SourceFileMissing())
            assert checked.call_count == 1
            assert len([r for r in caplog.records if "暂停删源" in r.message]) == 1
            assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
            caplog.clear()

            # Ordinary observations cannot claim the source exists in the root.
            collector.register(replace(upload, from_scan=False))
            assert collector.record(record_id)["source_missing_at"] == now[0]
            assert await api.list_uploads() == []
            root_queries = rpc.generation
            for _ in range(3):
                await collector.cleanup()
            collector.close()
            now[0] += 86400
            api = NapCat(123, http, clock=lambda: now[0])
            api.bot = rpc
            collector = Collector(settings, api=api, http=http, clock=lambda: now[0])
            await collector.cleanup()
            assert rpc.generation == root_queries and checked.call_count == 1
            assert not caplog.records
            assert collector.record(record_id)["source_missing_at"] == missing["source_missing_at"]
            assert collector.delete_after(collector.record(record_id)) == deadline

            rpc.files["returned-source"] = original
            if changed_content:
                remote[0] = body[:-1] + b"!"
            assert collector.register((await api.list_uploads())[0]) == record_id
            assert collector.record(record_id)["source_missing_at"] is None
            assert collector.record(record_id)["delete_error"] is None
            assert collector.delete_after(collector.record(record_id)) == deadline
            await collector.cleanup()
            result = collector.record(record_id)
            assert checked.call_count == 2
            assert (result["deleted_at"] is None) == changed_content
            assert bool(rpc.files) == changed_content
            if changed_content:
                assert result["delete_error"] and result["source_missing_at"] is None
                assert any(r.levelno == logging.WARNING for r in caplog.records)
            for field in (
                "status", "sha256", "archive_path", "collected_at", "retention_started_at",
            ):
                assert result[field] == archived[field]
            assert len(collector.records()) == 1
            assert (tmp_path / result["archive_path"]).read_bytes() == body
            assert rpc.messages == []
        finally:
            collector.close()


@pytest.mark.parametrize("due", [False, True])
async def test_capacity_cleanup_skips_missing_sources_without_rechecking_each_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, due: bool,
) -> None:
    body = zip_bytes()
    rpc = StorageRPC(len(body))
    now = [NOW]
    checked = Mock(wraps=file_matches)
    monkeypatch.setattr("ptilopsisbot.collector.file_matches", checked)
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=body)
    )) as http:
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        settings = config(tmp_path, 2 * len(body))
        collector = Collector(settings, api=api, http=http, clock=lambda: now[0])
        try:
            await archive(collector, api)
            # Only an unrelated large video remains, keeping usage over the limit.
            original = dict(rpc.files["oldest"])
            rpc.files = {"video": {**original, "file_name": "video.mp4",
                                   "file_size": 3 * len(body)}}
            if due:
                now[0] += 12 * 3600
            await collector.cleanup()
            assert checked.call_count == 3  # Each absent file is checked only once.
            assert all(row["source_missing_at"] is not None for row in collector.records())
            assert all(row["deleted_at"] is None for row in collector.records())
            collector.close()
            collector = Collector(settings, api=api, http=http, clock=lambda: now[0])
            for _ in range(3):
                root_queries = rpc.generation
                await collector.cleanup()
                # One full capacity measurement, no per-record resolution or recount.
                assert rpc.generation == root_queries + 1
                assert checked.call_count == 3
            assert rpc.deleted == []

            # Reappearing files remain eligible for capacity cleanup before expiry.
            rpc.files["oldest"] = original
            for upload in await api.list_uploads():
                collector.register(upload)
            await collector.cleanup()
            assert rpc.deleted == ["oldest"]
            assert checked.call_count == 4
            assert sum(row["deleted_at"] is not None for row in collector.records()) == 1
        finally:
            collector.close()


async def test_manual_retry_clears_missing_state_without_resetting_retention(
    tmp_path: Path,
) -> None:
    body = zip_bytes()
    api = FakeGroup()
    now = [NOW]
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=body)
    )) as http:
        collector = Collector(
            Settings(group_id=123, data_dir=tmp_path, min_free_gib=0, send_receipts=False),
            api=api, http=http, clock=lambda: now[0],
        )
        try:
            record_id = collector.register(Upload(
                123, "file-a", 102, 456, "run-20260905-120000-000001.zip", len(body), NOW,
            ))
            assert record_id is not None
            assert await collector.process_once()
            deadline = collector.delete_after(collector.record(record_id))
            now[0] += 24 * 3600
            api.delete_error = SourceFileMissing()
            await collector.cleanup()
            assert collector.record(record_id)["source_missing_at"] == now[0]
            await collector.retry(record_id)
            assert collector.record(record_id)["source_missing_at"] is None
            assert collector.record(record_id)["delete_error"] is None
            api.delete_error = None
            assert await collector.process_once()
            assert collector.delete_after(collector.record(record_id)) == deadline
            await collector.cleanup()
            assert collector.record(record_id)["deleted_at"] == now[0]
            assert api.files == set()
        finally:
            collector.close()


def test_migration_defers_only_known_missing_cleanup_errors_once(tmp_path: Path) -> None:
    settings = Settings(group_id=123, data_dir=tmp_path)
    old_missing = "根目录无法唯一定位源文件（匹配 0 项），保留记录，等待补扫或人工检查"
    old_duplicate = "根目录找不到重复文件，保留待清理状态"
    cases = [
        ("archived", None, old_missing, True),
        ("archived", None, old_duplicate, True),
        ("archived", None, old_missing.replace("0 项", "2 项"), False),
        ("archived", None, "本地文件不存在或大小/哈希不符，请重试此上传记录", False),
        ("archived", NOW, old_missing, False),
        ("failed", None, old_missing, False),
    ]
    collector = Collector(settings)
    with collector.db:
        for index, (status, deleted_at, error, _) in enumerate(cases, 1):
            collector.register(Upload(
                123, f"file-{index}", 102, 456, "run-20260905-120000-000001.zip", 100, NOW,
            ))
            collector.db.execute(
                """UPDATE uploads SET status=?, deleted_at=?, collected_at=?,
                   delete_error=? WHERE id=?""",
                (status, deleted_at, NOW, error, index),
            )
        collector.db.execute("ALTER TABLE uploads DROP COLUMN source_missing_at")
    collector.close()
    for offset in (100, 200):
        collector = Collector(settings, clock=lambda: NOW + offset)
        try:
            for index, (status, deleted_at, error, deferred) in enumerate(cases, 1):
                row = collector.record(index)
                assert row["source_missing_at"] == (NOW + 100 if deferred else None)
                assert row["delete_error"] == (str(SourceFileMissing()) if deferred else error)
                assert row["status"] == status and row["deleted_at"] == deleted_at
                assert row["collected_at"] == NOW
        finally:
            collector.close()
