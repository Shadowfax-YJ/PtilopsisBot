import hashlib
import io
from datetime import date
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import httpx
import pytest
from nonebot.adapters.onebot.v11 import ActionFailed, GroupMessageEvent

from ptilopsisbot.collector import Collector, OfflineError
from ptilopsisbot.config import Settings
from ptilopsisbot.napcat import NapCat


async def test_zero_upload_time_archives_on_observation_day_and_counts_in_report(
    tmp_path: Path,
) -> None:
    stream = io.BytesIO()
    with ZipFile(stream, "w") as archive:
        archive.writestr("run.json", "{}")
    body = stream.getvalue()
    rpc = FilesRPC(len(body))
    rpc.files["source-a"]["upload_time"] = 0
    now = 1788611560.0  # 2026-09-05 20:32:40 Asia/Shanghai, from the user's log.
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body))
    ) as http:
        api = NapCat(123, http, clock=lambda: now)
        api.bot = rpc
        collector = Collector(
            Settings(group_id=123, data_dir=tmp_path, auto_delete=False, min_free_gib=0),
            api=api,
            http=http,
            clock=lambda: now,
        )
        try:
            record_id = collector.register((await api.list_uploads())[0])
            assert record_id is not None
            await collector.process_once()
            record = collector.record(record_id)
            assert record["archive_path"] == "archive/2026-09-05/456/1.zip"
            report = collector.report(date(2026, 9, 5))
            assert report is not None
            assert "当日新增：1 个包" in report
            assert "发现日期" not in report
            assert rpc.messages == []
        finally:
            collector.close()


async def test_zero_time_live_upload_uses_message_date_without_changing_identity(
    tmp_path: Path,
) -> None:
    rpc = FilesRPC()
    rpc.files["source-a"]["upload_time"] = 0
    now = [1788580800.0]
    settings = Settings(group_id=123, data_dir=tmp_path)
    async with httpx.AsyncClient() as http:
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        collector = Collector(settings, clock=lambda: now[0])
        first = (await api.list_uploads())[0]
        record_id = collector.register(first)
        assert record_id is not None
        api.remember_file_message(file_message())
        from_message = (await api.list_uploads())[0]
        assert from_message.file_id == first.file_id
        assert from_message.uploaded_at == 1788580830
        collector.register(from_message)
        assert collector.record(record_id)["time_source"] == "message"
        await api.send_receipt(first.file_id, first.busid, "已保存")
        assert rpc.messages[0][0].data == {"id": "-42"}
        collector.close()

        now[0] += 86400
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        collector = Collector(settings, clock=lambda: now[0])
        try:
            assert collector.register((await api.list_uploads())[0]) == record_id
            assert len(collector.records()) == 1
            assert collector.record(record_id)["uploaded_at"] == 1788580830
            assert collector.record(record_id)["time_source"] == "message"
        finally:
            collector.close()


async def test_qq_reconnect_releases_reserved_slot_without_websocket_disconnect(
    tmp_path: Path,
) -> None:
    rpc = FilesRPC()
    now = [1788580800.0]
    async with httpx.AsyncClient() as http:
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        collector = Collector(
            Settings(group_id=123, data_dir=tmp_path, min_free_gib=0),
            api=api,
            http=http,
        )
        api.on_receipts_reset = collector.reset_priority
        try:
            await api.refresh_status()
            api.remember_file_message(file_message())
            upload = (await api.list_uploads())[0]
            assert upload.live
            record_id = collector.register(upload)
            assert record_id in collector.live_records
            rpc.online = False
            assert not await api.refresh_status()
            rpc.online = True
            now[0] += 120
            assert await api.refresh_status()
            old_upload = (await api.list_uploads())[0]
            assert not old_upload.live
            assert collector.register(old_upload) == record_id
            assert not collector.live_records
            assert not await collector.process_once(live_only=True)
            assert api.bot is rpc  # WebSocket connection itself never changed.
        finally:
            collector.close()


async def test_message_arriving_during_download_corrects_date_before_archive_and_delete(
    tmp_path: Path,
) -> None:
    stream = io.BytesIO()
    with ZipFile(stream, "w") as archive:
        archive.writestr("run.json", "{}")
    body = stream.getvalue()
    rpc = FilesRPC(len(body))
    rpc.files["source-a"]["upload_time"] = 0
    now = [1788623940.0]  # Connected at 2026-09-05 23:59:00 Shanghai.
    event = file_message(time=1788623999)
    event.message[0].data["file_size"] = str(len(body))

    def serve(request: httpx.Request) -> httpx.Response:
        api.remember_file_message(event)  # Chat event arrives after the first scan.
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        await api.refresh_status()
        now[0] = 1788624001  # First root scan is just after midnight.
        collector = Collector(
            Settings(group_id=123, data_dir=tmp_path, delete_grace_hours=0, min_free_gib=0),
            api=api,
            http=http,
            clock=lambda: now[0],
        )
        try:
            record_id = collector.register((await api.list_uploads())[0])
            assert record_id is not None
            await collector.process_once()
            await collector.cleanup()
            record = collector.record(record_id)
            assert record["time_source"] == "message"
            assert record["uploaded_at"] == 1788623999
            assert record["archive_path"] == "archive/2026-09-05/456/1.zip"
            assert record["deleted_at"] is not None
            report = collector.report(date(2026, 9, 5))
            assert report is not None
            assert "当日新增：1 个包" in report
            assert collector.report(date(2026, 9, 6)) is None
            assert rpc.messages[0][0].data == {"id": "-42"}
            assert not rpc.files
        finally:
            collector.close()


def file_message(message_id: int = -42, **changes: Any) -> GroupMessageEvent:
    return GroupMessageEvent.model_validate(
        {
            "time": 1788580830,
            "self_id": 999,
            "post_type": "message",
            "message_type": "group",
            "sub_type": "normal",
            "group_id": 123,
            "user_id": 456,
            "message_id": message_id,
            "message": [
                {
                    "type": "file",
                    "data": {
                        "file": "run-20260905-120000-000001.zip",
                        "file_id": "native-file-uuid-not-a-message-id",
                        "file_size": "100",
                    },
                }
            ],
            "raw_message": "",
            "font": 0,
            "sender": {"user_id": 456, "nickname": "测试成员"},
            **changes,
        }
    )


async def test_live_receipt_quotes_original_message_and_keeps_text_literal() -> None:
    rpc = FilesRPC()
    async with httpx.AsyncClient() as http:
        api = NapCat(123, http, clock=lambda: 1788580800)
        api.bot = rpc
        upload = (await api.list_uploads())[0]  # Notice/root scan can arrive first.
        api.remember_file_message(file_message())
        text = "已保存 [CQ:at,qq=all]"
        await api.send_receipt(upload.file_id, upload.busid, text)
        assert len(rpc.messages) == 1
        assert [segment.type for segment in rpc.messages[0]] == ["reply", "text"]
        assert rpc.messages[0][0].data == {"id": "-42"}
        assert rpc.messages[0][1].data == {"text": text}
        api.remember_file_message(file_message())
        await api.send_receipt(upload.file_id, upload.busid, text)
        assert len(rpc.messages) == 1


@pytest.mark.parametrize(
    "case",
    [
        "old_message",
        "before_connection_file",
        "wrong_group",
        "self_message",
        "reconnected",
        "qq_reconnected",
        "ambiguous_files",
        "ambiguous_messages",
        "unrelated_time",
    ],
)
async def test_receipts_stay_silent_without_a_unique_live_source(case: str) -> None:
    rpc = FilesRPC()
    now = [1788580800]
    async with httpx.AsyncClient() as http:
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        upload = (await api.list_uploads())[0]
        changes: dict[str, Any] = {}
        if case == "old_message":
            changes["time"] = now[0] - 1
        elif case == "before_connection_file":
            rpc.files["source-a"]["upload_time"] = now[0] - 10
            upload = (await api.list_uploads())[0]
        elif case == "wrong_group":
            changes["group_id"] = 321
        elif case == "self_message":
            changes["self_id"] = 456
        elif case == "unrelated_time":
            changes["time"] = now[0] + 120
        api.remember_file_message(file_message(**changes))
        if case == "reconnected":
            now[0] += 60
            api.reset_receipts()
            api.remember_file_message(file_message())  # An old event is still silent.
        elif case == "qq_reconnected":
            rpc.online = False
            await api.refresh_status()
            now[0] += 60
            rpc.online = True
            await api.refresh_status()
            api.remember_file_message(file_message())
        elif case == "ambiguous_files":
            rpc.files["source-b"] = {**rpc.files["source-a"], "upload_time": now[0] + 1}
        elif case == "ambiguous_messages":
            api.remember_file_message(file_message(message_id=43))
        await api.send_receipt(upload.file_id, upload.busid, "已保存")
        assert rpc.messages == []


async def test_reconnect_during_final_status_check_does_not_send_old_reply_id() -> None:
    class ReconnectingRPC(FilesRPC):
        armed = False
        status_checks = 0

        async def call_api(self, action: str, **data: Any) -> object:
            if action == "get_status" and self.armed:
                self.status_checks += 1
                if self.status_checks == 2:  # Final online check before sending.
                    api.reset_receipts()
            return await super().call_api(action, **data)

    rpc = ReconnectingRPC()
    async with httpx.AsyncClient() as http:
        api = NapCat(123, http, clock=lambda: 1788580800)
        api.bot = rpc
        upload = (await api.list_uploads())[0]
        api.remember_file_message(file_message())
        rpc.armed = True
        await api.send_receipt(upload.file_id, upload.busid, "已保存")
        assert rpc.messages == []


class FilesRPC:
    """Emulate NapCat's disposable handles and QQ's independent delete result."""

    def __init__(self, size: int = 100) -> None:
        self.online = True
        self.generation = 0
        self.delete_result = 0
        self.delete_effective = True
        self.handles: dict[str, str] = {}
        self.messages: list[Any] = []
        self.files = {
            "source-a": {
                "busid": 102,
                "uploader": 456,
                "file_name": "run-20260905-120000-000001.zip",
                "file_size": size,
                "upload_time": 1788580800,
            }
        }

    async def call_api(self, api: str, **data: Any) -> object:
        if api == "get_status":
            return {"online": self.online}
        if api == "send_group_msg":
            self.messages.append(data["message"])
            return {"message_id": 1}
        if api == "get_group_root_files":
            self.generation += 1
            current_handles = {
                f"temporary-{self.generation}-{source}": source for source in self.files
            }
            self.handles.update(current_handles)
            return {
                "files": [
                    {**self.files[source], "file_id": handle}
                    for handle, source in current_handles.items()
                ],
                "folders": [],
            }
        source = self.handles[data["file_id"]]
        if api == "get_group_file_url":
            return {"url": f"https://files.test/{source}"}
        assert api == "delete_group_file"
        assert data["busid"] == self.files[source]["busid"]
        if self.delete_result == 0 and self.delete_effective:
            del self.files[source]
        return {"result": self.delete_result, "errMsg": ""}


async def test_delete_failure_is_not_reported_as_success() -> None:
    class DeniedRPC(FilesRPC):
        async def call_api(self, api: str, **data: Any) -> object:
            if api == "delete_group_file":
                raise ActionFailed(message="permission denied", retcode=1200)
            return await super().call_api(api, **data)

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=b"abc"))
    ) as http:
        group = NapCat(123, http)
        group.bot = DeniedRPC(size=3)
        upload = (await group.list_uploads())[0]
        with pytest.raises(ActionFailed):
            await group.delete_file(
                upload.file_id, 102, expected_hash=hashlib.sha256(b"abc").hexdigest()
            )


async def test_qq_offline_with_live_websocket_pauses_operations() -> None:
    async with httpx.AsyncClient() as http:
        group = NapCat(123, http)
        rpc = FilesRPC()
        group.bot = rpc
        assert await group.refresh_status()
        rpc.online = False
        with pytest.raises(OfflineError):
            await group.file_url("source-a", 102)
        assert not group.online
        rpc.online = True
        assert await group.refresh_status()


async def test_changing_napcat_handles_do_not_double_count_after_restart(tmp_path: Path) -> None:
    settings = Settings(group_id=123, data_dir=tmp_path)
    rpc = FilesRPC()
    async with httpx.AsyncClient() as http:
        first_api = NapCat(123, http)
        first_api.bot = rpc
        collector = Collector(settings)
        record_id = collector.register((await first_api.list_uploads())[0])
        collector.close()

        restarted_api = NapCat(123, http)
        restarted_api.bot = rpc
        collector = Collector(settings)
        try:
            assert collector.register((await restarted_api.list_uploads())[0]) == record_id
            assert len(collector.records()) == 1
        finally:
            collector.close()


async def test_restart_reacquires_handle_verifies_content_and_cleans_archived_source(
    tmp_path: Path,
) -> None:
    stream = io.BytesIO()
    with ZipFile(stream, "w") as archive:
        archive.writestr("run.json", '{"game": "test"}')
    body = stream.getvalue()
    rpc = FilesRPC(len(body))
    settings = Settings(group_id=123, data_dir=tmp_path, min_free_gib=0)
    now = [1788580800.0]
    downloaded: list[str] = []

    def serve(request: httpx.Request) -> httpx.Response:
        downloaded.append(request.url.path)
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        api = NapCat(123, http)
        api.bot = rpc
        collector = Collector(settings, api=api, http=http, clock=lambda: now[0])
        record_id = collector.register((await api.list_uploads())[0])
        assert record_id is not None
        try:
            await collector.process_once()
            assert collector.record(record_id)["status"] == "archived"
            assert "source-a" in rpc.files
            assert rpc.messages == []  # Startup/backfill collection stays quiet.
        finally:
            collector.close()

        now[0] += 86400
        api = NapCat(123, http)
        api.bot = rpc
        rpc.handles.clear()  # NapCat also restarted, losing every cached file handle.
        await api.refresh_status()
        collector = Collector(settings, api=api, http=http, clock=lambda: now[0])
        try:
            await collector.cleanup()
            assert not rpc.files
            record = collector.record(record_id)
            assert record["deleted_at"] is not None
            assert (tmp_path / record["archive_path"]).read_bytes() == body
            assert downloaded == ["/source-a", "/source-a"]
            assert rpc.messages == []
        finally:
            collector.close()


@pytest.mark.parametrize("case", ["changed_content", "ambiguous", "missing", "qq_denied"])
async def test_cleanup_keeps_source_when_identity_content_or_delete_is_unconfirmed(
    tmp_path: Path,
    case: str,
) -> None:
    stream = io.BytesIO()
    with ZipFile(stream, "w") as archive:
        archive.writestr("run.json", '{"game": "test"}')
    body = stream.getvalue()
    remote = [body]
    rpc = FilesRPC(len(body))
    now = [1788580800.0]
    settings = Settings(group_id=123, data_dir=tmp_path, min_free_gib=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=remote[0]))
    ) as http:
        api = NapCat(123, http)
        api.bot = rpc
        collector = Collector(settings, api=api, http=http, clock=lambda: now[0])
        try:
            record_id = collector.register((await api.list_uploads())[0])
            assert record_id is not None
            await collector.process_once()
            assert collector.record(record_id)["status"] == "archived"
            now[0] += 86400
            if case == "changed_content":
                remote[0] = body[:-1] + b"!"
            elif case == "ambiguous":
                rpc.files["source-b"] = dict(rpc.files["source-a"])
                assert await api.list_uploads() == []
            elif case == "missing":
                rpc.files.clear()
            else:
                rpc.delete_result = 5
            await collector.cleanup()
            record = collector.record(record_id)
            assert record["deleted_at"] is None
            assert record["delete_error"]
            assert (record["source_missing_at"] is not None) == (case == "missing")
            assert (tmp_path / record["archive_path"]).read_bytes() == body
            if case != "missing":
                assert "source-a" in rpc.files
        finally:
            collector.close()


async def test_same_name_uploaded_later_is_a_new_upload(tmp_path: Path) -> None:
    rpc = FilesRPC()
    async with httpx.AsyncClient() as http:
        api = NapCat(123, http)
        api.bot = rpc
        collector = Collector(Settings(group_id=123, data_dir=tmp_path))
        try:
            first = collector.register((await api.list_uploads())[0])
            rpc.files["source-b"] = {**rpc.files["source-a"], "upload_time": 1788580801}
            registered = {collector.register(upload) for upload in await api.list_uploads()}
            assert first in registered
            assert len(registered) == len(collector.records()) == 2
        finally:
            collector.close()


@pytest.mark.parametrize("busid", [102, 104])
async def test_success_ack_without_removal_leaves_cleanup_pending(
    tmp_path: Path, busid: int,
) -> None:
    stream = io.BytesIO()
    with ZipFile(stream, "w") as archive:
        archive.writestr("run.json", "{}")
    body = stream.getvalue()
    rpc = FilesRPC(len(body))
    rpc.files["source-a"]["busid"] = busid
    rpc.delete_effective = False
    downloads = []

    def serve(request: httpx.Request) -> httpx.Response:
        downloads.append(request.url.path)
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        api = NapCat(123, http)
        api.bot = rpc
        collector = Collector(
            Settings(group_id=123, data_dir=tmp_path, delete_grace_hours=0, min_free_gib=0),
            api=api, http=http,
        )
        try:
            record_id = collector.register((await api.list_uploads())[0])
            assert record_id is not None
            await collector.process_once()
            before = collector.record(record_id)
            assert before["status"] == "archived"
            await collector.cleanup()
            before = collector.record(record_id)
            assert before["deleted_at"] is None
            assert "补查仍发现源文件" in before["delete_error"]
            assert f"busid={busid}" in before["delete_error"]
            assert "source-a" in rpc.files
            rpc.delete_effective = True
            assert collector.register((await api.list_uploads())[0]) == record_id
            await collector.cleanup()
            after = collector.record(record_id)
            assert not rpc.files
            assert after["deleted_at"] is not None
            assert after["delete_error"] is None
            assert after["attempts"] == 1
            assert after["collected_at"] == before["collected_at"]
            assert (tmp_path / after["archive_path"]).read_bytes() == body
            assert downloads == ["/source-a"] * 3
            assert rpc.messages == []
        finally:
            collector.close()


@pytest.mark.parametrize("case", ["same", "changed_remote", "changed_local", "disabled"])
async def test_rescan_recovers_legacy_deleted_source_and_rechecks_hashes(
    tmp_path: Path, case: str,
) -> None:
    stream = io.BytesIO()
    with ZipFile(stream, "w") as archive:
        archive.writestr("run.json", "{}")
    body = stream.getvalue()
    remote = [body]
    rpc = FilesRPC(len(body))
    rpc.files["source-a"].update(busid=104, upload_time=0)
    settings = Settings(
        group_id=123, data_dir=tmp_path, auto_delete=False, delete_grace_hours=0, min_free_gib=0,
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=remote[0]))
    ) as http:
        api = NapCat(123, http)
        api.bot = rpc
        collector = Collector(settings, api=api, http=http)
        record_id = collector.register((await api.list_uploads())[0])
        assert record_id is not None
        await collector.process_once()
        before = collector.record(record_id)
        # Reproduce the existing database: success recorded while the source remains.
        with collector.db:
            collector.db.execute("UPDATE uploads SET deleted_at=123 WHERE id=?", (record_id,))
        collector.close()
        if case == "changed_remote":
            remote[0] = body[:-1] + b"!"
        elif case == "changed_local":
            (tmp_path / before["archive_path"]).write_bytes(body[:-1] + b"!")
        collector = Collector(
            settings.model_copy(update={"auto_delete": case != "disabled"}), api=api, http=http,
        )
        try:
            assert collector.register((await api.list_uploads())[0]) == record_id
            assert collector.record(record_id)["deleted_at"] is None
            await collector.cleanup()
            after = collector.record(record_id)
            assert len(collector.records()) == 1
            for field in ("archive_path", "sha256", "collected_at", "uploaded_at", "attempts"):
                assert after[field] == before[field]
            assert not await collector.process_once()
            assert rpc.messages == []
            if case == "same":
                assert not rpc.files
                assert after["deleted_at"] is not None
                assert after["delete_error"] is None
                assert (tmp_path / after["archive_path"]).read_bytes() == body
            else:
                assert "source-a" in rpc.files
                assert after["deleted_at"] is None
                assert after["delete_error"]
        finally:
            collector.close()


@pytest.mark.parametrize("case", ["truncated", "malformed", "unavailable", "delayed"])
async def test_delete_confirmation_requires_a_complete_valid_listing(case: str) -> None:
    class ConfirmationRPC(FilesRPC):
        deleting = False
        checks = 0

        async def call_api(self, api: str, **data: Any) -> object:
            if api == "get_group_root_files" and self.deleting:
                self.checks += 1
                if case == "truncated":
                    return {"files": [], "folders": [{}] * 1000}
                if case == "malformed":
                    return {"files": [{}], "folders": []}
                if case == "unavailable":
                    raise RuntimeError("listing unavailable")
                if self.checks == 2:
                    self.files.clear()
            if api == "delete_group_file":
                self.deleting = True
            return await super().call_api(api, **data)

    rpc = ConfirmationRPC(size=3)
    rpc.delete_effective = case != "delayed"
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=b"abc"))
    ) as http:
        api = NapCat(123, http)
        api.bot = rpc
        upload = (await api.list_uploads())[0]
        if case == "delayed":
            await api.delete_file(
                upload.file_id, 102, expected_hash=hashlib.sha256(b"abc").hexdigest(),
            )
            assert rpc.checks == 2
            assert not rpc.files
        else:
            error = RuntimeError if case == "unavailable" else ValueError
            with pytest.raises(error):
                await api.delete_file(
                    upload.file_id, 102, expected_hash=hashlib.sha256(b"abc").hexdigest(),
                )
