import asyncio
import threading
from pathlib import Path

import httpx
import pytest
from test_collector import FakeGroup, zip_bytes

from ptilopsisbot.collector import Collector, Upload
from ptilopsisbot.config import Settings


class Group(FakeGroup):
    async def file_url(self, file_id: str, busid: int) -> str:
        return f"https://files.test/{file_id}"


async def test_distinct_uploads_download_in_parallel_without_double_claiming(
    tmp_path: Path,
) -> None:
    body = zip_bytes()
    started: asyncio.Queue[str] = asyncio.Queue()
    release = asyncio.Event()

    async def serve(request: httpx.Request) -> httpx.Response:
        started.put_nowait(request.url.path)
        await release.wait()
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        collector = Collector(
            Settings(
                group_id=123,
                data_dir=tmp_path,
                auto_delete=False,
                min_free_gib=0,
                download_concurrency=2,
            ),
            api=Group(),
            http=http,
        )
        for key in ("a", "b", "c"):
            collector.register(
                Upload(
                    123,
                    key,
                    102,
                    456,
                    "run-20260905-120000-000001.zip",
                    len(body),
                    1788580800,
                )
            )
        tasks = [asyncio.create_task(collector.process_once()) for _ in range(2)]
        try:
            paths = {await asyncio.wait_for(started.get(), 2) for _ in range(2)}
            assert paths == {"/a", "/b"}
            assert not await asyncio.wait_for(collector.process_once(), 1)
            release.set()
            assert await asyncio.gather(*tasks) == [True, True]
            assert [row["attempts"] for row in collector.records()] == [1, 1, 0]
            assert [row["status"] for row in collector.records()] == [
                "archived",
                "archived",
                "queued",
            ]
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            collector.close()


async def test_slow_cleanup_does_not_block_new_collection_or_allow_retry(tmp_path: Path) -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    class SlowCleanup(Group):
        async def delete_file(self, file_id: str, busid: int, *, expected_hash: str) -> None:
            entered.set()
            await release.wait()
            await super().delete_file(file_id, busid, expected_hash=expected_hash)

    body = zip_bytes()
    group = SlowCleanup()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body))
    ) as http:
        collector = Collector(
            Settings(group_id=123, data_dir=tmp_path, delete_grace_hours=0, min_free_gib=0),
            api=group,
            http=http,
        )
        collector.register(
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
        await asyncio.wait_for(collector.process_once(), 2)
        cleaning = asyncio.create_task(collector.cleanup())
        try:
            await asyncio.wait_for(entered.wait(), 2)
            with pytest.raises(ValueError, match="正在"):
                await collector.retry(1)
            collector.register(
                Upload(
                    123,
                    "file-b",
                    102,
                    456,
                    "run-20260905-120000-000002.zip",
                    len(body),
                    1788580801,
                    live=True,
                )
            )
            assert await asyncio.wait_for(collector.process_once(live_only=True), 2)
            assert collector.record(2)["status"] == "archived"
            assert not cleaning.done()
            assert collector.active == {1: "cleanup"}
            release.set()
            await cleaning
            assert collector.record(1)["deleted_at"] is not None
        finally:
            release.set()
            await asyncio.gather(cleaning, return_exceptions=True)
            collector.close()


@pytest.mark.parametrize("phase", ["download", "zip_check"])
async def test_shutdown_releases_files_and_preserves_retry_budget(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    body = zip_bytes()
    entered = asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()

    def check(path: Path) -> None:
        # Hold a real Windows file handle while shutdown is requested.
        with path.open("rb"):
            loop.call_soon_threadsafe(entered.set)
            assert release.wait(5)

    async def serve(request: httpx.Request) -> httpx.Response:
        if phase == "download" and not release.is_set():
            entered.set()
            await asyncio.Event().wait()
        return httpx.Response(200, content=body)

    if phase == "zip_check":
        monkeypatch.setattr("ptilopsisbot.collector.check_zip", check)
    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        group = Group()
        collector = Collector(
            Settings(group_id=123, data_dir=tmp_path, delete_grace_hours=0, min_free_gib=0),
            api=group,
            http=http,
        )
        collector.register(
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
        task = asyncio.create_task(collector.process_once())
        try:
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            await asyncio.sleep(0.05)
            if phase == "zip_check":
                assert not task.done(), "Wait for the ZIP reader before removing its file"
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not collector.active
            assert collector.record(1)["status"] == "queued"
            assert collector.record(1)["attempts"] == 0
            assert not list((tmp_path / "incoming").glob("*.part"))
            assert group.files == {"file-a"}
            assert await collector.process_once()
            assert collector.record(1)["attempts"] == 1
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            collector.close()


async def test_live_slot_stays_free_for_new_uploads_while_history_is_downloading(
    tmp_path: Path,
) -> None:
    body = zip_bytes()
    started: asyncio.Queue[str] = asyncio.Queue()
    release = asyncio.Event()

    async def serve(request: httpx.Request) -> httpx.Response:
        started.put_nowait(request.url.path)
        if request.url.path != "/live":
            await release.wait()
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        group = Group()
        collector = Collector(
            Settings(group_id=123, data_dir=tmp_path, auto_delete=False, min_free_gib=0),
            api=group,
            http=http,
        )
        for key in ("old-a", "old-b", "old-c"):
            collector.register(
                Upload(
                    123,
                    key,
                    102,
                    456,
                    "run-20260905-120000-000001.zip",
                    len(body),
                    1788580800,
                )
            )
        history = [asyncio.create_task(collector.process_once()) for _ in range(2)]
        try:
            assert {await asyncio.wait_for(started.get(), 2) for _ in range(2)} == {
                "/old-a",
                "/old-b",
            }
            assert not await collector.process_once(live_only=True)
            live_id = collector.register(
                Upload(
                    123,
                    "live",
                    102,
                    456,
                    "run-20260905-120000-000002.zip",
                    len(body),
                    1788580801,
                    live=True,
                )
            )
            assert live_id is not None
            assert await asyncio.wait_for(collector.process_once(live_only=True), 2)
            assert await started.get() == "/live"
            assert collector.record(live_id)["status"] == "archived"
            assert collector.record(3)["attempts"] == 0
            assert all(not task.done() for task in history)
            with pytest.raises(ValueError, match="正在"):
                await collector.retry(1)
            release.set()
            await asyncio.gather(*history)
            assert await collector.process_once()  # Remaining history still drains.
        finally:
            release.set()
            await asyncio.gather(*history, return_exceptions=True)
            collector.close()
