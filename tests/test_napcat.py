import hashlib
import io
from pathlib import Path
from typing import Any
from zipfile import ZipFile

import httpx
import pytest
from nonebot.adapters.onebot.v11 import ActionFailed

from databot.collector import Collector, OfflineError
from databot.config import Settings
from databot.napcat import NapCat


class FilesRPC:
    """Emulate NapCat's disposable handles and QQ's independent delete result."""

    def __init__(self, size: int = 100) -> None:
        self.online = True
        self.generation = 0
        self.delete_result = 0
        self.handles: dict[str, str] = {}
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
        if api == "get_group_root_files":
            self.generation += 1
            self.handles = {
                f"temporary-{self.generation}-{source}": source for source in self.files
            }
            return {
                "files": [
                    {**self.files[source], "file_id": handle}
                    for handle, source in self.handles.items()
                ],
                "folders": [],
            }
        source = self.handles[data["file_id"]]
        if api == "get_group_file_url":
            return {"url": f"https://files.test/{source}"}
        assert api == "delete_group_file"
        if self.delete_result == 0:
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
