from dataclasses import replace
from pathlib import Path

import httpx
import pytest
from test_collector import FakeGroup, zip_bytes
from test_napcat import FilesRPC, file_message

from ptilopsisbot.collector import Collector, Upload
from ptilopsisbot.config import Settings
from ptilopsisbot.napcat import NapCat


async def test_history_is_immediate_and_live_keeps_24_hours_across_restart(tmp_path: Path) -> None:
    body = zip_bytes()
    now = [1788580800.0]
    settings = Settings(
        group_id=123, data_dir=tmp_path, min_free_gib=0,
        history_delete_grace_hours=0, delete_grace_hours=24,
    )
    group = FakeGroup()
    group.files.add("file-b")
    history = Upload(
        123, "file-a", 102, 456, "run-20260905-120000-000001.zip", len(body), now[0] - 300,
        source_kind="history",
    )
    live = replace(history, file_id="file-b", uploaded_at=now[0], live=True, source_kind="live")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body))
    ) as http:
        collector = Collector(settings, api=group, http=http, clock=lambda: now[0])
        history_id = collector.register(history)
        live_id = collector.register(live)
        assert history_id is not None and live_id is not None
        await collector.process_once()
        await collector.process_once()
        await collector.cleanup()
        assert group.files == {"file-b"}
        assert collector.record(history_id)["deleted_at"] == now[0]
        assert collector.delete_after(collector.record(live_id)) == now[0] + 86400
        collector.reset_priority()
        assert not collector.live_records
        collector.register(replace(live, live=False, source_kind="history"))
        assert collector.record(live_id)["source_kind"] == "live"
        collector.close()

        collector = Collector(settings, api=group, http=http, clock=lambda: now[0])
        try:
            now[0] += 3600
            await collector.retry(live_id)
            await collector.process_once()
            assert collector.delete_after(collector.record(live_id)) == 1788580800 + 86400
            now[0] = 1788580800 + 86399
            await collector.cleanup()
            assert group.files == {"file-b"}
            now[0] += 1
            await collector.cleanup()
            assert not group.files
            assert collector.record(live_id)["deleted_at"] == now[0]
            assert len(group.messages) == 2  # A retry does not send another receipt.
        finally:
            collector.close()


async def test_shorter_grace_subtracts_difference_without_resetting_on_restart(
    tmp_path: Path,
) -> None:
    body = zip_bytes()
    restart_at = 1788758400.0
    now = [restart_at]
    group = FakeGroup()
    group.files = {"expired", "boundary", "waiting", "renewed"}
    settings = Settings(
        group_id=123, data_dir=tmp_path, min_free_gib=0, send_receipts=False,
        history_delete_grace_hours=0, delete_grace_hours=24,
    )
    uploads: dict[str, Upload] = {}
    ids: dict[str, int] = {}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body))
    ) as http:
        collector = Collector(settings, api=group, http=http, clock=lambda: now[0])
        try:
            for key, age_hours in (("expired", 20), ("boundary", 12), ("waiting", 4),
                                   ("renewed", 20)):
                now[0] = restart_at - age_hours * 3600
                upload = Upload(
                    123, key, 102, 456, "run-20260905-120000-000001.zip", len(body), now[0],
                    source_kind="live",
                )
                uploads[key] = upload
                record_id = collector.register(upload)
                assert record_id is not None
                ids[key] = record_id
                await collector.process_once()
            with collector.db:
                collector.db.execute(
                    "UPDATE uploads SET retention_started_at=? WHERE id=?",
                    (restart_at - 4 * 3600, ids["renewed"]),
                )
            now[0] = restart_at
            old_deadlines = {
                key: collector.delete_after(collector.record(i)) for key, i in ids.items()
            }
            await collector.cleanup()
            assert len(group.files) == 4
        finally:
            collector.close()

        settings = settings.model_copy(update={"delete_grace_hours": 12})
        for elapsed_hours in (0, 1):
            now[0] = restart_at + elapsed_hours * 3600
            collector = Collector(settings, api=group, http=http, clock=lambda: now[0])
            try:
                # Startup observations cannot turn persisted live uploads into history.
                for key in tuple(group.files):
                    assert collector.register(replace(
                        uploads[key], from_scan=True, source_kind="history",
                    )) == ids[key]
                    old_deadline = old_deadlines[key]
                    assert old_deadline is not None
                    deadline = collector.delete_after(collector.record(ids[key]))
                    assert deadline == old_deadline - 43200
                await collector.cleanup()
                assert group.files == {"waiting", "renewed"}
                if elapsed_hours:
                    now[0] = restart_at + 8 * 3600
                    await collector.cleanup()
                    assert not group.files
            finally:
                collector.close()


def test_legacy_unknown_records_do_not_become_immediate_deletions(tmp_path: Path) -> None:
    settings = Settings(
        group_id=123, data_dir=tmp_path, history_delete_grace_hours=0, delete_grace_hours=24,
    )
    collector = Collector(settings)
    unknown = Upload(123, "old", 102, 456, "run-20260905-120000-000001.zip", 100, 1788580800)
    unknown_id = collector.register(unknown)
    live_id = collector.register(replace(unknown, file_id="known-live", time_source="message"))
    assert unknown_id is not None and live_id is not None
    with collector.db:
        collector.db.execute("ALTER TABLE uploads DROP COLUMN source_kind")
        collector.db.execute("UPDATE uploads SET status='archived',collected_at=1788580800")
    collector.close()
    collector = Collector(settings)
    try:
        collector.register(replace(unknown, source_kind="history"))
        assert collector.record(unknown_id)["source_kind"] == "unknown"
        assert collector.record(live_id)["source_kind"] == "live"
        assert collector.delete_after(collector.record(unknown_id)) == 1788580800 + 86400
        assert collector.delete_after(collector.record(live_id)) == 1788580800 + 86400
    finally:
        collector.close()


@pytest.mark.parametrize("missing_time", [False, True])
async def test_new_files_get_retention_even_without_a_live_message(missing_time: bool) -> None:
    now = 1788580800
    rpc = FilesRPC()
    rpc.files["source-a"]["upload_time"] = 0 if missing_time else now - 1
    async with httpx.AsyncClient() as http:
        api = NapCat(123, http, clock=lambda: now)
        api.bot = rpc
        old = (await api.list_uploads())[0]
        assert old.source_kind == "history"
        rpc.files["source-b"] = {
            **rpc.files["source-a"], "file_name": "run-20260905-120000-000002.zip",
            "upload_time": 0 if missing_time else now,
        }
        by_name = {u.name: u for u in await api.list_uploads()}
        new = by_name["run-20260905-120000-000002.zip"]
        assert new.source_kind == "live"
        assert not new.live  # Retention evidence does not grant message-based priority.
        # A separate zero-time candidate demonstrates late-message promotion.
        rpc.files["source-a"]["upload_time"] = 0
        old = next(u for u in await api.list_uploads() if u.name == old.name)
        api.remember_file_message(file_message(time=now))
        promoted = next(u for u in await api.list_uploads() if u.file_id == old.file_id)
        assert promoted.live and promoted.source_kind == "live"
        await api.send_receipt(promoted.file_id, promoted.busid, "已保存")
        after_receipt = next(u for u in await api.list_uploads() if u.file_id == old.file_id)
        assert not after_receipt.live
        assert after_receipt.source_kind == "live"


async def test_late_live_evidence_during_remote_hash_check_prevents_early_delete(
    tmp_path: Path,
) -> None:
    body = zip_bytes()
    now = [1788580800.0]
    rpc = FilesRPC(len(body))
    rpc.files["source-a"]["upload_time"] = 0
    downloads = 0

    def serve(request: httpx.Request) -> httpx.Response:
        nonlocal downloads
        downloads += 1
        if downloads == 2:
            collector.register(replace(upload, live=True, source_kind="live"))
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        collector = Collector(
            Settings(
                group_id=123, data_dir=tmp_path, min_free_gib=0,
                history_delete_grace_hours=0, delete_grace_hours=24,
            ), api=api, http=http, clock=lambda: now[0],
        )
        try:
            upload = (await api.list_uploads())[0]
            record_id = collector.register(upload)
            assert record_id is not None and upload.source_kind == "history"
            await collector.process_once()
            await collector.cleanup()
            assert collector.record(record_id)["source_kind"] == "live"
            assert collector.record(record_id)["deleted_at"] is None
            assert "清理时间尚未到" in collector.record(record_id)["delete_error"]
            assert "source-a" in rpc.files
            now[0] += 86400
            await collector.cleanup()
            assert not rpc.files
            assert collector.record(record_id)["deleted_at"] == now[0]
        finally:
            collector.close()


@pytest.mark.parametrize("invalid", [-1, float("inf"), float("nan")])
def test_invalid_history_grace_is_rejected(invalid: float) -> None:
    with pytest.raises(ValueError):
        Settings(group_id=123, history_delete_grace_hours=invalid)
