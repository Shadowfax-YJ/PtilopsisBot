import asyncio
from pathlib import Path
from typing import Any

import httpx
import pytest
from test_collector import zip_bytes
from test_napcat import FilesRPC

from ptilopsisbot.collector import Collector
from ptilopsisbot.config import Settings
from ptilopsisbot.napcat import NapCat


class PaginatedRPC(FilesRPC):
    """Concurrent readers receive repeated pages from NapCat's shared native event."""

    def __init__(self, size: int = 100) -> None:
        super().__init__(size)
        self.files["source-b"] = {
            **self.files["source-a"], "file_name": "run-20260905-120000-000002.zip",
        }
        self.in_flight = 0
        self.peak = 0

    async def call_api(self, api: str, **data: Any) -> Any:
        if api != "get_group_root_files":
            return await super().call_api(api, **data)
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        try:
            # Each native page yields to the other requests, which listen to the
            # same event. Repeated entries make a formerly unique file ambiguous.
            await asyncio.sleep(0)
            overlaps = self.in_flight > 1
            await asyncio.sleep(0)
            result = await super().call_api(api, **data)
            assert isinstance(result, dict)
            if overlaps:
                result["files"] *= 2
            return result
        finally:
            self.in_flight -= 1


async def test_scan_download_and_cleanup_queries_do_not_mix_pages() -> None:
    rpc = PaginatedRPC()
    async with httpx.AsyncClient() as http:
        api = NapCat(123, http)
        api.bot = rpc
        first, second = await api.list_uploads()
        scan, first_url, second_url, complete = await asyncio.gather(
            api.list_uploads(),
            api.file_url(first.file_id, first.busid),
            api.file_url(second.file_id, second.busid),
            api._list_files(require_complete=True),
        )
        assert [u.file_id for u in scan] == [first.file_id, second.file_id]
        assert len(complete) == 2
        assert first_url == "https://files.test/source-a"
        assert second_url == "https://files.test/source-b"
        assert rpc.peak == 1


async def test_serialized_queries_still_allow_parallel_downloads(tmp_path: Path) -> None:
    body = zip_bytes()
    rpc = PaginatedRPC(len(body))
    started: asyncio.Queue[str] = asyncio.Queue()
    release = asyncio.Event()

    async def serve(request: httpx.Request) -> httpx.Response:
        started.put_nowait(request.url.path)
        await release.wait()
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        api = NapCat(123, http)
        api.bot = rpc
        collector = Collector(
            Settings(group_id=123, data_dir=tmp_path, auto_delete=False, min_free_gib=0),
            api=api, http=http,
        )
        for upload in await api.list_uploads():
            collector.register(upload)
        tasks = [asyncio.create_task(collector.process_once()) for _ in range(2)]
        try:
            paths = {await asyncio.wait_for(started.get(), 2) for _ in range(2)}
            assert paths == {"/source-a", "/source-b"}
            # Scans remain available while both HTTP downloads are blocked.
            assert len(await asyncio.wait_for(api.list_uploads(), 2)) == 2
            release.set()
            assert await asyncio.wait_for(asyncio.gather(*tasks), 2) == [True, True]
            assert [r["status"] for r in collector.records()] == ["archived", "archived"]
            assert rpc.peak == 1
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            collector.close()


@pytest.mark.parametrize("outcome", ["cancel", "error"])
async def test_interrupted_root_query_releases_waiting_readers(outcome: str) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    class InterruptedRPC(FilesRPC):
        blocked = True

        async def call_api(self, api: str, **data: Any) -> object:
            if api == "get_group_root_files" and self.blocked:
                self.blocked = False
                entered.set()
                await release.wait()
                raise RuntimeError("native query interrupted")
            return await super().call_api(api, **data)

    async with httpx.AsyncClient() as http:
        api = NapCat(123, http)
        api.bot = InterruptedRPC()
        first = asyncio.create_task(api.list_uploads())
        await asyncio.wait_for(entered.wait(), 2)
        second = asyncio.create_task(api.list_uploads())
        try:
            if outcome == "cancel":
                first.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await first
            else:
                release.set()
                with pytest.raises(RuntimeError, match="interrupted"):
                    await first
            assert len(await asyncio.wait_for(second, 2)) == 1
        finally:
            release.set()
            await asyncio.gather(first, second, return_exceptions=True)
