import asyncio
import threading
from collections import Counter
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import pytest
from test_collector import FakeGroup, zip_bytes
from test_napcat import FilesRPC

from ptilopsisbot.collector import Collector, Upload
from ptilopsisbot.config import Settings
from ptilopsisbot.files import download
from ptilopsisbot.napcat import NapCat


async def archive_packages(collector: Collector, count: int, *, future_last: bool = False) -> None:
    for index in range(count):
        collector.register(
            Upload(
                123, f"file-{index}", 102, 456,
                f"run-20260905-120000-{index:06d}.zip", len(zip_bytes()), 1788580800,
                source_kind="live" if future_last and index == count - 1 else "history",
            )
        )
        await collector.process_once()


@pytest.mark.parametrize("slots", [1, 3])
async def test_cleanup_pool_is_bounded_and_overlapping_rounds_do_not_double_delete(
    tmp_path: Path, slots: int,
) -> None:
    started: asyncio.Queue[str] = asyncio.Queue()
    release = asyncio.Event()

    class SlowGroup(FakeGroup):
        active = 0
        peak = 0

        def __init__(self) -> None:
            super().__init__()
            self.files = {f"file-{index}" for index in range(6)}
            self.calls: Counter[str] = Counter()

        async def delete_file(
            self, file_id: str, busid: int, *, expected_hash: str,
            can_delete: Callable[[], bool] | None = None,
        ) -> None:
            self.calls[file_id] += 1
            self.active += 1
            self.peak = max(self.peak, self.active)
            started.put_nowait(file_id)
            try:
                await release.wait()
                await super().delete_file(
                    file_id, busid, expected_hash=expected_hash, can_delete=can_delete,
                )
            finally:
                self.active -= 1

    group = SlowGroup()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=zip_bytes()))
    ) as http:
        collector = Collector(
            Settings(group_id=123, data_dir=tmp_path, cleanup_concurrency=slots,
                     history_delete_grace_hours=0, send_receipts=False, min_free_gib=0),
            api=group, http=http,
        )
        await archive_packages(collector, 6, future_last=True)
        tasks = [asyncio.create_task(collector.cleanup()) for _ in range(2)]
        try:
            assert {await asyncio.wait_for(started.get(), 2) for _ in range(slots)} == {
                f"file-{index}" for index in range(slots)
            }
            assert len(collector.active) == slots
            with pytest.raises(ValueError, match="正在"):
                await collector.retry(1)
            assert started.empty()
            release.set()
            await asyncio.wait_for(asyncio.gather(*tasks), 3)
            assert group.files == {"file-5"}  # New upload still has its 24-hour grace.
            assert group.calls == Counter({f"file-{index}": 1 for index in range(5)})
            assert group.peak == slots
            assert not collector.active
        finally:
            release.set()
            await asyncio.gather(*tasks, return_exceptions=True)
            collector.close()


@pytest.mark.parametrize("outcome", ["cancel", "environment_error"])
async def test_interrupted_cleanup_drains_all_workers_and_resumes_after_restart(
    tmp_path: Path, outcome: str,
) -> None:
    started: asyncio.Queue[str] = asyncio.Queue()
    all_started = asyncio.Event()
    release = asyncio.Event()

    class InterruptedGroup(FakeGroup):
        in_flight = 0
        failing = True

        def __init__(self) -> None:
            super().__init__()
            self.files = {f"file-{index}" for index in range(3)}

        async def delete_file(
            self, file_id: str, busid: int, *, expected_hash: str,
            can_delete: Callable[[], bool] | None = None,
        ) -> None:
            self.in_flight += 1
            started.put_nowait(file_id)
            if self.in_flight == 3:
                all_started.set()
            try:
                await all_started.wait()
                if self.failing and outcome == "environment_error" and file_id == "file-0":
                    raise OSError("disk became unavailable")
                await release.wait()
                await super().delete_file(
                    file_id, busid, expected_hash=expected_hash, can_delete=can_delete,
                )
            finally:
                self.in_flight -= 1

    group = InterruptedGroup()
    settings = Settings(group_id=123, data_dir=tmp_path, cleanup_concurrency=3,
                        history_delete_grace_hours=0, send_receipts=False, min_free_gib=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=zip_bytes()))
    ) as http:
        collector = Collector(settings, api=group, http=http)
        await archive_packages(collector, 3)
        task = asyncio.create_task(collector.cleanup())
        try:
            for _ in range(3):
                await asyncio.wait_for(started.get(), 2)
            if outcome == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                with pytest.raises(OSError, match="unavailable"):
                    await task
            assert group.in_flight == 0
            assert not collector.active
            assert len(group.files) == 3
            assert all(r["status"] == "archived" and r["deleted_at"] is None
                       and r["attempts"] == 1 for r in collector.records())
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            collector.close()
        group.failing = False
        collector = Collector(settings, api=group, http=http)
        try:
            await asyncio.wait_for(collector.cleanup(), 3)
            assert not group.files
            assert all(r["deleted_at"] is not None for r in collector.records())
        finally:
            collector.close()


async def test_cleanup_cancellation_waits_for_all_local_file_readers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered: asyncio.Queue[Path] = asyncio.Queue()
    release = threading.Event()
    loop = asyncio.get_running_loop()

    def slow_check(path: Path, size: int, digest: str) -> bool:
        with path.open("rb") as source:
            assert source.read(1)
            loop.call_soon_threadsafe(entered.put_nowait, path)
            assert release.wait(5)
        return True

    group = FakeGroup()
    group.files = {f"file-{index}" for index in range(3)}
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=zip_bytes()))
    ) as http:
        collector = Collector(
            Settings(group_id=123, data_dir=tmp_path, cleanup_concurrency=3,
                     history_delete_grace_hours=0, send_receipts=False, min_free_gib=0),
            api=group, http=http,
        )
        await archive_packages(collector, 3)
        monkeypatch.setattr("ptilopsisbot.collector.file_matches", slow_check)
        task = asyncio.create_task(collector.cleanup())
        try:
            paths = [await asyncio.wait_for(entered.get(), 2) for _ in range(3)]
            task.cancel()
            await asyncio.sleep(0.05)
            assert not task.done(), "Cleanup must finish its file readers before shutdown"
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not collector.active
            assert len(group.files) == 3
            for path in paths:
                moved = path.with_suffix(".handle-check")
                path.rename(moved)
                moved.rename(path)
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            collector.close()


class NativeRPC(FilesRPC):
    def __init__(self, size: int) -> None:
        super().__init__(size)
        self.files = {
            f"source-{index}": {**self.files["source-a"],
                                "file_name": f"run-20260905-120000-{index:06d}.zip"}
            for index in range(3)
        }
        self.native_active = 0
        self.native_peak = 0

    async def call_api(self, api: str, **data: Any) -> object:
        if api not in ("get_group_root_files", "delete_group_file"):
            return await super().call_api(api, **data)
        self.native_active += 1
        self.native_peak = max(self.native_peak, self.native_active)
        try:
            await asyncio.sleep(0)
            return await super().call_api(api, **data)
        finally:
            self.native_active -= 1


@pytest.mark.parametrize("corrupt", [False, True])
async def test_remote_hashes_run_in_parallel_but_native_operations_remain_serial(
    tmp_path: Path, corrupt: bool,
) -> None:
    body = zip_bytes()
    rpc = NativeRPC(len(body))
    checking = False
    started: asyncio.Queue[str] = asyncio.Queue()
    release = asyncio.Event()

    async def serve(request: httpx.Request) -> httpx.Response:
        if checking:
            started.put_nowait(request.url.path)
            await release.wait()
            if corrupt and request.url.path == "/source-2":
                return httpx.Response(200, content=b"!" * len(body))
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        api = NapCat(123, http)
        api.bot = rpc
        collector = Collector(
            Settings(group_id=123, data_dir=tmp_path, history_delete_grace_hours=0,
                     cleanup_concurrency=3, send_receipts=False, min_free_gib=0),
            api=api, http=http,
        )
        for upload in await api.list_uploads():
            collector.register(upload)
            await collector.process_once()
        checking = True
        task = asyncio.create_task(collector.cleanup())
        try:
            assert {await asyncio.wait_for(started.get(), 2) for _ in range(3)} == {
                "/source-0", "/source-1", "/source-2",
            }
            assert all(r["deleted_at"] is None for r in collector.records())
            assert len(await asyncio.wait_for(api.list_uploads(), 2)) == 3
            release.set()
            await asyncio.wait_for(task, 3)
            assert rpc.native_peak == 1
            assert set(rpc.files) == ({"source-2"} if corrupt else set())
            assert collector.record(1)["deleted_at"] is not None
            assert collector.record(2)["deleted_at"] is not None
            if corrupt:
                assert collector.record(3)["deleted_at"] is None
                assert "内容与本地归档不符" in collector.record(3)["delete_error"]
            else:
                assert collector.record(3)["deleted_at"] is not None
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            collector.close()


async def test_live_promotion_while_waiting_to_delete_preserves_grace_across_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = zip_bytes()
    rpc = NativeRPC(len(body))
    now = [1788580801.0]
    verified: asyncio.Queue[None] = asyncio.Queue()

    async def tracked_download(*args: Any, **kwargs: Any) -> str:
        result = await download(*args, **kwargs)
        verified.put_nowait(None)
        return result

    monkeypatch.setattr("ptilopsisbot.napcat.download", tracked_download)
    settings = Settings(group_id=123, data_dir=tmp_path, history_delete_grace_hours=0,
                        cleanup_concurrency=3, send_receipts=False, min_free_gib=0)
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, content=body))
    ) as http:
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        collector = Collector(settings, api=api, http=http, clock=lambda: now[0])
        uploads = await api.list_uploads()
        for upload in uploads:
            collector.register(upload)
            await collector.process_once()
        await api.delete_lock.acquire()
        task = asyncio.create_task(collector.cleanup())
        try:
            for _ in range(3):
                await asyncio.wait_for(verified.get(), 2)
            collector.register(replace(uploads[1], live=True, source_kind="live"))
            api.delete_lock.release()
            await asyncio.wait_for(task, 3)
            assert set(rpc.files) == {"source-1"}
            assert collector.record(2)["deleted_at"] is None
            assert "清理时间尚未到" in collector.record(2)["delete_error"]
        finally:
            if api.delete_lock.locked():
                api.delete_lock.release()
            await asyncio.gather(task, return_exceptions=True)
            collector.close()
        collector = Collector(settings, api=api, http=http, clock=lambda: now[0])
        try:
            now[0] += 86399
            await collector.cleanup()
            assert set(rpc.files) == {"source-1"}
            now[0] += 1
            await collector.cleanup()
            assert not rpc.files
        finally:
            collector.close()


@pytest.mark.parametrize("invalid", [0, 4])
def test_cleanup_concurrency_is_bounded(invalid: int) -> None:
    with pytest.raises(ValueError):
        Settings(group_id=123, cleanup_concurrency=invalid)
