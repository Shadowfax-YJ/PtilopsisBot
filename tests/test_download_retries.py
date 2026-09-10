from dataclasses import replace
from datetime import date
from pathlib import Path

import httpx
import pytest
from test_collector import FakeGroup, zip_bytes

from ptilopsisbot.collector import Collector, Upload
from ptilopsisbot.config import Settings

NOW = 1788580800.0


def upload(size: int) -> Upload:
    return Upload(
        123, "file-a", 102, 456, "run-20260905-120000-000001.zip", size, NOW,
        live=True, source_kind="live",
    )


async def test_bad_zip_redownloads_after_delay_then_archives_and_reports_once(
    tmp_path: Path, caplog: pytest.LogCaptureFixture,
) -> None:
    class FreshURLGroup(FakeGroup):
        lookups = 0

        async def file_url(self, file_id: str, busid: int) -> str:
            self.lookups += 1
            return f"https://files.test/download-{self.lookups}"

    body = zip_bytes()
    now = [NOW]
    group = FreshURLGroup()
    fetched = []

    def serve(request: httpx.Request) -> httpx.Response:
        fetched.append(request.url.path)
        return httpx.Response(200, content=b"!" * len(body) if len(fetched) == 1 else body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        collector = Collector(
            Settings(group_id=123, data_dir=tmp_path, min_free_gib=0, delete_grace_hours=0),
            api=group, http=http, clock=lambda: now[0],
        )
        try:
            record_id = collector.register(upload(len(body)))
            assert record_id is not None
            assert await collector.process_once(live_only=True)
            row = collector.record(record_id)
            assert row["status"] == "failed" and row["attempts"] == 1
            assert row["next_attempt_at"] == NOW + 300
            assert "尝试 1/3，5 分钟后自动重试" in caplog.text
            assert record_id in collector.live_records
            assert collector.report(date(2026, 9, 5)) is None
            assert group.messages == [] and group.files == {"file-a"}
            assert not list(tmp_path.glob("archive/**/*.zip"))
            assert not list(tmp_path.glob("incoming/*.part"))
            await collector.cleanup()
            now[0] += 299
            collector.register(replace(upload(len(body)), from_scan=True))
            assert not await collector.process_once()
            now[0] += 1
            assert await collector.process_once(live_only=True)
            row = collector.record(record_id)
            assert row["status"] == "archived" and row["attempts"] == 2
            assert row["collected_at"] == now[0] and row["last_error"] is None
            assert (tmp_path / row["archive_path"]).read_bytes() == body
            assert fetched == ["/download-1", "/download-2"]
            assert len(group.messages) == 1
            assert "当日新增：1 个包" in collector.report(date(2026, 9, 5))
            assert not await collector.process_once()
            await collector.cleanup()
            assert group.files == set()
        finally:
            collector.close()


@pytest.mark.parametrize("maximum", [1, 3, 5])
async def test_invalid_zip_budget_and_cooldown_survive_restart_and_rescan(
    tmp_path: Path, caplog: pytest.LogCaptureFixture, maximum: int,
) -> None:
    now = [NOW]
    group = FakeGroup()
    settings = Settings(
        group_id=123, data_dir=tmp_path, min_free_gib=0,
        download_max_attempts=maximum, delete_grace_hours=0,
    )
    body = b"not a zip"
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=body)
    )) as http:
        collector = Collector(settings, api=group, http=http, clock=lambda: now[0])
        try:
            for attempt in range(1, maximum + 1):
                collector.register(upload(len(body)))
                assert await collector.process_once()
                row = collector.record(1)
                assert row["attempts"] == attempt
                assert row["status"] == ("invalid" if attempt == maximum else "failed")
                assert row["collected_at"] is None and row["deleted_at"] is None
                assert not await collector.process_once()
                collector.close()
                collector = Collector(settings, api=group, http=http, clock=lambda: now[0])
                assert collector.recover_invalid_packages() == 0
                now[0] += 299
                assert not await collector.process_once()
                now[0] += 1
            collector.register(replace(upload(len(body)), from_scan=True))
            now[0] += 86400
            assert not await collector.process_once()
            assert f"尝试 {maximum}/{maximum}，已停止自动重试" in caplog.text
            assert group.files == {"file-a"} and not group.messages
            assert not list(tmp_path.glob("archive/**/*.zip"))
            assert not list(tmp_path.glob("incoming/*.part"))
            # A deliberate manual retry gets a new budget; restart never grants one.
            await collector.retry(1)
            assert await collector.process_once()
            assert collector.record(1)["attempts"] == 1
        finally:
            collector.close()


async def test_transport_and_zip_errors_share_one_attempt_budget(tmp_path: Path) -> None:
    now = [NOW]
    calls = 0

    def serve(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503) if calls == 1 else httpx.Response(200, content=b"bad zip")

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        collector = Collector(
            Settings(group_id=123, data_dir=tmp_path, min_free_gib=0),
            api=FakeGroup(), http=http, clock=lambda: now[0],
        )
        try:
            collector.register(upload(7))
            for _ in range(3):
                assert await collector.process_once()
                now[0] += 300
            assert collector.record(1)["status"] == "invalid"
            assert collector.record(1)["attempts"] == calls == 3
            assert not await collector.process_once()
        finally:
            collector.close()


async def test_legacy_invalid_recovery_preserves_budget_cooldown_and_other_records(
    tmp_path: Path,
) -> None:
    settings = Settings(group_id=123, data_dir=tmp_path, min_free_gib=0)
    collector = Collector(settings)
    for index in range(1, 8):
        collector.register(replace(upload(len(zip_bytes())), file_id=f"file-{index}"))
    with collector.db:
        collector.db.execute(
            """UPDATE uploads SET status='invalid', attempts=1, next_attempt_at=?,
               last_error='ZIP 不可读: File is not a zip file'""", (NOW + 300,),
        )
        collector.db.execute("UPDATE uploads SET attempts=3 WHERE id=2")
        collector.db.execute("UPDATE uploads SET group_id=999 WHERE id=3")
        collector.db.execute("UPDATE uploads SET deleted_at=1 WHERE id=4")
        collector.db.execute("UPDATE uploads SET collected_at=1 WHERE id=5")
        collector.db.execute("UPDATE uploads SET status='archived' WHERE id=6")
        collector.db.execute("UPDATE uploads SET status='failed' WHERE id=7")
    before = {row["id"]: dict(row) for row in collector.db.execute("SELECT * FROM uploads")}
    collector.close()
    now = [NOW]
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=zip_bytes())
    )) as http:
        collector = Collector(settings, api=FakeGroup(), http=http, clock=lambda: now[0])
        try:
            assert dict(collector.record(1)) == before[1]  # Construction alone is not recovery.
            assert collector.recover_invalid_packages() == 1
            assert dict(collector.record(1)) == {**before[1], "status": "failed"}
            assert collector.recover_invalid_packages() == 0
            for index in range(2, 8):
                assert dict(collector.record(index)) == before[index]
            assert not await collector.process_once()
            now[0] += 300
            assert await collector.process_once()
            assert collector.record(1)["attempts"] == 2
            assert collector.record(1)["status"] == "archived"
        finally:
            collector.close()


@pytest.mark.parametrize("value", [0, -1, 11, 1.5])
def test_invalid_attempt_limit_is_rejected(value: int | float) -> None:
    with pytest.raises(ValueError):
        Settings(group_id=123, download_max_attempts=value)
