import hashlib
import io
import sqlite3
import struct
from datetime import date
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

import httpx
import pytest

from ptilopsisbot.collector import Collector, Upload
from ptilopsisbot.config import Settings


@pytest.mark.parametrize("move_done", [False, True])
def test_legacy_zero_date_repair_preserves_archives_records_and_cleanup_state(
    tmp_path: Path,
    move_done: bool,
) -> None:
    settings = Settings(group_id=123, data_dir=tmp_path)
    body = zip_bytes()
    collector = Collector(settings, clock=lambda: 1788611560)
    for file_id in ("file-a", "file-b"):
        collector.register(
            Upload(
                123,
                file_id,
                104,
                456,
                "run-20260905-172518-248257.zip",
                len(body),
                1788611560,
            )
        )
    collector.close()
    old = tmp_path / "archive/1970-01-01/456/1.zip"
    old.parent.mkdir(parents=True)
    old.write_bytes(body)
    new = tmp_path / "archive/2026-09-05/456/1.zip"
    if move_done:
        new.parent.mkdir(parents=True)
        old.rename(new)  # Simulate interruption between move and database commit.
    with sqlite3.connect(tmp_path / "collector.sqlite3") as legacy:
        legacy.execute("ALTER TABLE uploads DROP COLUMN time_source")
        legacy.execute("UPDATE uploads SET uploaded_at=0")
        legacy.execute(
            """UPDATE uploads SET status='archived',sha256=?,archive_path=?,
               collected_at=1788611699.686,deleted_at=1788611827.222 WHERE id=1""",
            (hashlib.sha256(body).hexdigest(), "archive/1970-01-01/456/1.zip"),
        )
    for _ in range(2):
        collector = Collector(settings, clock=lambda: 1788697960)
        try:
            collector.repair_dates()
            record = collector.record(1)
            assert record["archive_path"] == "archive/2026-09-05/456/1.zip"
            assert record["deleted_at"] == 1788611827.222
            assert record["collected_at"] == 1788611699.686
            assert record["time_source"] == "observed"
            assert collector.record(2)["status"] == "queued"
            assert collector.record(2)["uploaded_at"] == 1788611560
            assert len(collector.records()) == 2
            assert "上传 2，新增唯一包 1" in collector.report(date(2026, 9, 5))
            assert new.read_bytes() == body
            assert not old.exists()
        finally:
            collector.close()


def test_repeat_observation_survives_restart_without_double_counting(tmp_path: Path) -> None:
    settings = Settings(group_id=123, data_dir=tmp_path)
    upload = Upload(123, "file-a", 102, 456, "run-20260905-120000-000001.zip", 100, 1788580800)
    collector = Collector(settings)
    first = collector.register(upload)
    collector.close()

    collector = Collector(settings)
    assert collector.register(upload) == first
    assert first is not None
    assert collector.record(first)["status"] == "queued"
    assert len(collector.records()) == 1
    collector.close()


class FakeGroup:
    online = True

    def __init__(self) -> None:
        self.files: set[str] = {"file-a"}
        self.delete_error: Exception | None = None
        self.messages: list[str] = []
        self.message_error: Exception | None = None
        self.message_attempts = 0

    async def file_url(self, file_id: str, busid: int) -> str:
        return "https://files.test/run.zip"

    async def list_uploads(self) -> list[Upload]:
        return []

    async def delete_file(self, file_id: str, busid: int, *, expected_hash: str) -> None:
        if self.delete_error:
            raise self.delete_error
        if file_id not in self.files:
            raise ValueError("源文件未找到")
        self.files.remove(file_id)

    async def send_message(self, text: str) -> None:
        self.message_attempts += 1
        if self.message_error:
            raise self.message_error
        self.messages.append(text)

    async def send_receipt(self, file_id: str, busid: int, text: str) -> bool:
        await self.send_message(text)
        return True


def zip_bytes() -> bytes:
    stream = io.BytesIO()
    with ZipFile(stream, "w") as archive:
        archive.writestr("run.json", '{"game": "test"}')
    return stream.getvalue()


async def test_collects_zip_before_removing_group_source(tmp_path: Path) -> None:
    body = zip_bytes()
    group = FakeGroup()
    settings = Settings(group_id=123, data_dir=tmp_path, delete_grace_hours=0, min_free_gib=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body))
    ) as http:
        collector = Collector(settings, api=group, http=http)
        record_id = collector.register(
            Upload(123, "file-a", 102, 456, "run-20260905-120000-000001.zip", len(body), 1788580800)
        )
        assert record_id is not None
        await collector.process_once()
        record = collector.record(record_id)
        assert record["status"] == "archived"
        assert (tmp_path / record["archive_path"]).read_bytes() == body
        assert group.files == {"file-a"}  # Collection completes before the cleanup worker runs.
        await collector.cleanup()
        record = collector.record(record_id)
        assert group.files == set()
        assert record["deleted_at"] is not None
        assert len(group.messages) == 1
        assert "run-20260905-120000-000001.zip" in group.messages[0]
        assert "未验证游戏内容" in group.messages[0]
        collector.close()


async def test_collection_reply_is_not_repeated_by_rescan_restart_or_manual_retry(
    tmp_path: Path,
) -> None:
    body = zip_bytes()
    group = FakeGroup()
    settings = Settings(group_id=123, data_dir=tmp_path, auto_delete=False, min_free_gib=0)
    upload = Upload(
        123, "file-a", 102, 456, "run-20260905-120000-000001.zip", len(body), 1788580800
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body))
    ) as http:
        collector = Collector(settings, api=group, http=http)
        record_id = collector.register(upload)
        assert record_id is not None
        await collector.process_once()
        collector.register(upload)
        assert not await collector.process_once()
        collector.close()

        collector = Collector(settings, api=group, http=http)
        try:
            assert collector.register(upload) == record_id
            assert not await collector.process_once()
            await collector.retry(record_id)
            await collector.process_once()
            assert len(group.messages) == 1
        finally:
            collector.close()


@pytest.mark.parametrize("error", [RuntimeError("send failed"), OSError("connection lost")])
async def test_reply_failure_does_not_undo_collection_or_prevent_cleanup(
    tmp_path: Path,
    error: Exception,
) -> None:
    body = zip_bytes()
    group = FakeGroup()
    group.message_error = error
    settings = Settings(group_id=123, data_dir=tmp_path, delete_grace_hours=0, min_free_gib=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body))
    ) as http:
        collector = Collector(settings, api=group, http=http)
        try:
            record_id = collector.register(
                Upload(
                    123, "file-a", 102, 456, "run-20260905-120000-000001.zip", len(body), 1788580800
                )
            )
            assert record_id is not None
            await collector.process_once()
            await collector.cleanup()
            record = collector.record(record_id)
            assert record["status"] == "archived"
            assert record["attempts"] == 1
            assert group.message_attempts == 1
            assert record["deleted_at"] is not None
            assert not group.files
            assert (tmp_path / record["archive_path"]).read_bytes() == body
        finally:
            collector.close()


async def test_corrupt_archive_keeps_source_and_manual_retry_repairs_it(tmp_path: Path) -> None:
    body = zip_bytes()
    group = FakeGroup()
    now = [1788580800.0]
    settings = Settings(group_id=123, data_dir=tmp_path, min_free_gib=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body))
    ) as http:
        collector = Collector(settings, api=group, http=http, clock=lambda: now[0])
        record_id = collector.register(
            Upload(123, "file-a", 102, 456, "run-20260905-120000-000001.zip", len(body), now[0])
        )
        assert record_id is not None
        await collector.process_once()
        assert group.files == {"file-a"}  # Default 24-hour grace period.
        path = tmp_path / collector.record(record_id)["archive_path"]
        path.write_bytes(b"!" * len(body))  # Same size, different bytes.
        now[0] += 86400
        await collector.cleanup()
        assert group.files == {"file-a"}
        assert collector.record(record_id)["delete_error"]
        await collector.retry(record_id)
        await collector.process_once()
        await collector.cleanup()
        assert path.read_bytes() == body
        assert group.files == set()
        collector.close()


@pytest.mark.parametrize("case", ["bad_zip", "incomplete", "http_error", "disabled"])
async def test_does_not_delete_uncollected_files_or_when_disabled(
    tmp_path: Path, case: str
) -> None:
    body = b"not a zip" if case == "bad_zip" else zip_bytes()
    group = FakeGroup()
    settings = Settings(
        group_id=123,
        data_dir=tmp_path,
        delete_grace_hours=0,
        min_free_gib=0,
        auto_delete=case != "disabled",
    )
    status = 403 if case == "http_error" else 200
    size = len(body) + 1 if case == "incomplete" else len(body)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(status, content=body))
    ) as http:
        collector = Collector(settings, api=group, http=http)
        record_id = collector.register(
            Upload(123, "file-a", 102, 456, "run-20260905-120000-000001.zip", size, 1788580800)
        )
        assert record_id is not None
        await collector.process_once()
        await collector.cleanup()
        assert group.files == {"file-a"}
        assert collector.record(record_id)["deleted_at"] is None
        assert not list((tmp_path / "incoming").glob("*.part"))
        expected = {
            "bad_zip": "invalid",
            "incomplete": "failed",
            "http_error": "failed",
            "disabled": "archived",
        }
        assert collector.record(record_id)["status"] == expected[case]
        assert len(group.messages) == (1 if case == "disabled" else 0)
        collector.close()


async def test_report_counts_uploads_but_credits_identical_zip_only_once(tmp_path: Path) -> None:
    body = zip_bytes()
    group = FakeGroup()
    settings = Settings(group_id=123, data_dir=tmp_path, auto_delete=False, min_free_gib=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body))
    ) as http:
        collector = Collector(settings, api=group, http=http)
        for source, user, nick in [("file-a", 456, "小张"), ("file-b", 789, "小李")]:
            collector.register(
                Upload(
                    123,
                    source,
                    102,
                    user,
                    "run-20260905-120000-000001.zip",
                    len(body),
                    1788580800,
                    nickname=nick,
                )
            )
            await collector.process_once()
        report = collector.report(date(2026, 9, 5))
        assert "小张（456）：上传 1，新增唯一包 1" in report
        assert "小李（789）：上传 1，新增唯一包 0" in report
        assert "待处理 0，检查不通过 0，处理失败 0" in report
        assert "未验证游戏内容" in report
        collector.close()


async def test_retry_budget_survives_restart_and_duplicate_scan(tmp_path: Path) -> None:
    now = [1788580800.0]
    group = FakeGroup()
    settings = Settings(group_id=123, data_dir=tmp_path, min_free_gib=0)
    upload = Upload(123, "file-a", 102, 456, "run-20260905-120000-000001.zip", 100, now[0])
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(503))
    ) as http:
        collector = Collector(settings, api=group, http=http, clock=lambda: now[0])
        record_id = collector.register(upload)
        assert record_id is not None
        assert await collector.process_once()
        assert not await collector.process_once()
        collector.close()
        collector = Collector(settings, api=group, http=http, clock=lambda: now[0])
        for _ in range(2):
            now[0] += 300
            assert await collector.process_once()
        collector.register(upload)
        now[0] += 86400
        assert not await collector.process_once()
        assert collector.record(record_id)["attempts"] == 3
        assert group.files == {"file-a"}
        await collector.retry(record_id)
        assert await collector.process_once()
        assert collector.record(record_id)["attempts"] == 1
        collector.close()


@pytest.mark.parametrize("case", ["missing", "permission", "offline", "disk"])
async def test_cleanup_handles_missing_source_and_environment_failures(
    tmp_path: Path, case: str
) -> None:
    body = zip_bytes()
    now = [1788580800.0]
    group = FakeGroup()
    settings = Settings(group_id=123, data_dir=tmp_path, min_free_gib=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body))
    ) as http:
        collector = Collector(settings, api=group, http=http, clock=lambda: now[0])
        record_id = collector.register(
            Upload(
                123,
                "file-a",
                102,
                456,
                "run-20260905-120000-000001.zip",
                len(body),
                now[0],
            )
        )
        assert record_id is not None
        await collector.process_once()
        now[0] += 86400
        if case == "missing":
            group.files.clear()
        elif case == "permission":
            group.delete_error = RuntimeError("permission denied")
        elif case == "offline":
            group.online = False
        else:
            collector.settings = settings.model_copy(update={"min_free_gib": 10**10})
        if case == "disk":
            with pytest.raises(OSError):
                await collector.cleanup()
        else:
            await collector.cleanup()
        assert collector.record(record_id)["deleted_at"] is None
        if case != "missing":
            assert group.files == {"file-a"}
            group.delete_error = None
            group.online = True
            collector.settings = settings
            await collector.cleanup()
            assert collector.record(record_id)["deleted_at"]
        collector.close()


def test_scan_corrects_day_and_conflicting_metadata_does_not_overwrite_identity(
    tmp_path: Path,
) -> None:
    collector = Collector(Settings(group_id=123, data_dir=tmp_path))
    name = "run-20260905-120000-000001.zip"
    first = collector.register(Upload(123, "a", 102, 456, name, 100, 1788624000))
    assert first is not None
    assert (
        collector.register(
            Upload(
                123,
                "a",
                102,
                456,
                name,
                100,
                1788580800,
                nickname="新名字",
                from_scan=True,
            )
        )
        == first
    )
    collector.register(Upload(123, "a", 102, 789, name, 100, 1788580800))
    assert collector.record(first)["uploader_id"] == 456
    assert "新名字（456）：上传 1" in collector.report(date(2026, 9, 5))
    assert "当日未发现已登记的对局包" in collector.report(date(2026, 9, 6))
    assert collector.register(Upload(999, "a", 102, 456, name, 100, 1788580800)) is None
    assert collector.register(Upload(123, "b", 102, 456, name, 100, 1788580800)) != first
    collector.close()


async def test_corrupt_deflate_stream_is_invalid_and_never_deleted(tmp_path: Path) -> None:
    stream = io.BytesIO()
    with ZipFile(stream, "w", compression=ZIP_DEFLATED) as archive:
        archive.writestr("run.json", "test game data" * 100)
    body = bytearray(stream.getvalue())
    name_size, extra_size = struct.unpack_from("<HH", body, 26)
    payload_offset = 30 + name_size + extra_size
    body[payload_offset] = 0x07  # Invalid DEFLATE block type, leaving the ZIP directory intact.
    group = FakeGroup()
    settings = Settings(group_id=123, data_dir=tmp_path, min_free_gib=0, delete_grace_hours=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=bytes(body)))
    ) as http:
        collector = Collector(settings, api=group, http=http)
        record_id = collector.register(
            Upload(
                123,
                "file-a",
                102,
                456,
                "run-20260905-120000-000001.zip",
                len(body),
                1788580800,
            )
        )
        assert record_id is not None
        await collector.process_once()
        assert collector.record(record_id)["status"] == "invalid"
        assert group.files == {"file-a"}
        collector.close()
