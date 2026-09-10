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


class DuplicateRPC(FilesRPC):
    def __init__(self, size: int, busid: int = 104) -> None:
        super().__init__(size)
        original = {**self.files.pop('source-a'), 'busid': busid, 'upload_time': 0}
        # Neither list order nor the transient handle identifies the newest copy.
        self.files = {
            'middle': {**original, 'modify_time': 200},
            'latest': {**original, 'modify_time': 300},
            'oldest': {**original, 'modify_time': 100},
        }
        self.deleted: list[str] = []
        self.url_handles: dict[str, str] = {}
        self.fail_second = False
        self.native_active = 0
        self.native_max = 0

    async def call_api(self, api: str, **data: Any) -> object:
        native = api in ('get_group_root_files', 'delete_group_file')
        if native:
            self.native_active += 1
            self.native_max = max(self.native_max, self.native_active)
            await asyncio.sleep(0)
        try:
            if api == 'get_group_file_url':
                self.url_handles[self.handles[data['file_id']]] = data['file_id']
            if api == 'delete_group_file':
                source = self.handles[data['file_id']]
                # Must use exactly the handle that supplied the verified bytes.
                assert data['file_id'] == self.url_handles[source]
                if self.fail_second and self.deleted:
                    return {'result': 1}
                self.deleted.append(source)
            return await super().call_api(api, **data)
        finally:
            if native:
                self.native_active -= 1


def settings(tmp_path: Path, **overrides: Any) -> Settings:
    return Settings(
        group_id=123, data_dir=tmp_path, min_free_gib=0,
        send_receipts=False, delete_grace_hours=24, **overrides,
    )


async def archive(collector: Collector, api: NapCat) -> int:
    uploads = await api.list_uploads()
    assert len(uploads) == 1 and uploads[0].has_duplicates
    record_id = collector.register(uploads[0])
    assert record_id is not None
    assert await collector.process_once()
    assert collector.record(record_id)['status'] == 'archived'
    return record_id


@pytest.mark.parametrize('busid', [102, 104])
async def test_identical_copies_keep_latest_until_retention_expires_across_restart(
    tmp_path: Path, busid: int,
) -> None:
    body = zip_bytes()
    rpc = DuplicateRPC(len(body), busid)
    now = [1000.0]
    paths: list[str] = []

    def serve(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        config = settings(tmp_path)
        collector = Collector(config, api=api, http=http, clock=lambda: now[0])
        record_id = await archive(collector, api)
        assert paths == ['/latest']
        await asyncio.gather(collector.cleanup(), collector.cleanup(), api.list_uploads())
        assert set(rpc.files) == {'latest'}
        assert set(rpc.deleted) == {'middle', 'oldest'}
        assert rpc.native_max == 1
        assert paths == ['/latest', '/latest', '/middle', '/oldest']
        row = collector.record(record_id)
        assert row['deleted_at'] is None and not row['duplicate_pending']
        assert collector.delete_after(row) == 1000 + 86400
        assert (tmp_path / row['archive_path']).read_bytes() == body
        collector.close()

        rpc.handles.clear()
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        collector = Collector(config, api=api, http=http, clock=lambda: now[0])
        try:
            assert collector.register((await api.list_uploads())[0]) == record_id
            assert len(collector.records()) == 1
            now[0] += 86399
            await collector.cleanup()
            assert set(rpc.files) == {'latest'}
            now[0] += 1
            await collector.cleanup()
            assert not rpc.files
            assert collector.record(record_id)['deleted_at'] == now[0]
            assert rpc.messages == []
        finally:
            collector.close()


@pytest.mark.parametrize('case', ['history', 'disabled', 'corrupt_local', 'different_remote'])
async def test_duplicate_cleanup_obeys_archive_content_and_settings(
    tmp_path: Path, case: str,
) -> None:
    body = zip_bytes()
    rpc = DuplicateRPC(len(body))
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(
            200, content=b'x' * len(body)
            if case == 'different_remote' and request.url.path == '/oldest' else body,
        )
    )) as http:
        api = NapCat(123, http, clock=lambda: 1000)
        api.bot = rpc
        collector = Collector(
            settings(tmp_path, auto_delete=case != 'disabled', history_delete_grace_hours=0),
            api=api, http=http, clock=lambda: 1000,
        )
        try:
            record_id = await archive(collector, api)
            row = collector.record(record_id)
            if case == 'corrupt_local':
                (tmp_path / row['archive_path']).write_bytes(b'corrupt')
            await collector.cleanup()
            row = collector.record(record_id)
            if case == 'history':
                assert not rpc.files  # The survivor follows the existing history policy.
                assert rpc.deleted[-1] == 'latest'
                assert row['deleted_at'] == 1000
            else:
                assert len(rpc.files) == 3 and rpc.deleted == []
                assert row['duplicate_pending'] and row['deleted_at'] is None
                if case != 'disabled':
                    assert row['delete_error']
        finally:
            collector.close()


@pytest.mark.parametrize('times', [(0, 0, 0), (300, 300, 100), (None, 300, 100),
                                  ('invalid', 300, 100), (float('nan'), 300, 100)])
async def test_unknown_or_tied_order_preserves_ambiguous_group(times: tuple[Any, ...]) -> None:
    rpc = DuplicateRPC(100)
    for source, value in zip(rpc.files, times, strict=True):
        rpc.files[source]['modify_time'] = value
    async with httpx.AsyncClient() as http:
        api = NapCat(123, http)
        api.bot = rpc
        assert await api.list_uploads() == []
        assert rpc.deleted == []


@pytest.mark.parametrize('case', ['new_copy', 'keeper_removed', 'metadata_changed', 'reconnect'])
async def test_changes_during_hashing_abort_deletion(tmp_path: Path, case: str) -> None:
    body = zip_bytes()
    rpc = DuplicateRPC(len(body))
    armed = False

    def serve(request: httpx.Request) -> httpx.Response:
        if armed and request.url.path == '/oldest':
            if case == 'new_copy':
                rpc.files['newer'] = {**rpc.files['latest'], 'modify_time': 400}
            elif case == 'keeper_removed':
                rpc.files.pop('latest')
            elif case == 'metadata_changed':
                rpc.files['latest']['modify_time'] = 500
            else:
                api.reset_receipts()
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        api = NapCat(123, http, clock=lambda: 1000)
        api.bot = rpc
        collector = Collector(settings(tmp_path), api=api, http=http, clock=lambda: 1000)
        try:
            record_id = await archive(collector, api)
            armed = True
            await collector.cleanup()
            row = collector.record(record_id)
            assert not rpc.deleted and row['duplicate_pending']
            assert row['deleted_at'] is None and row['delete_error']
        finally:
            collector.close()


async def test_new_duplicate_of_old_record_gets_fresh_retention_without_reset_on_scan(
    tmp_path: Path,
) -> None:
    body = zip_bytes()
    rpc = DuplicateRPC(len(body))
    rpc.files = {'oldest': rpc.files['oldest']}
    now = [1000.0]
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=body)
    )) as http:
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        config = settings(tmp_path, history_delete_grace_hours=0)
        collector = Collector(config, api=api, http=http, clock=lambda: now[0])
        record_id = collector.register((await api.list_uploads())[0])
        assert record_id is not None
        await collector.process_once()
        now[0] += 90000
        rpc.files['latest'] = {**rpc.files['oldest'], 'modify_time': int(now[0])}
        upload = (await api.list_uploads())[0]
        assert upload.has_duplicates and upload.source_kind == 'live'
        assert collector.register(upload) == record_id
        deadline = now[0] + 86400
        assert collector.delete_after(collector.record(record_id)) == deadline
        collector.close()

        # Restart before pruning; neither repeated scans nor a successful prune reset the clock.
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        collector = Collector(config, api=api, http=http, clock=lambda: now[0])
        try:
            now[0] += 60
            collector.register((await api.list_uploads())[0])
            await collector.cleanup()
            assert set(rpc.files) == {'latest'}
            assert collector.record(record_id)['source_kind'] == 'live'
            assert collector.delete_after(collector.record(record_id)) == deadline
            now[0] += 60
            collector.register((await api.list_uploads())[0])
            assert collector.delete_after(collector.record(record_id)) == deadline
        finally:
            collector.close()


@pytest.mark.parametrize('case', ['denied', 'false_success', 'partial'])
async def test_delete_failure_preserves_pending_state_and_can_retry(
    tmp_path: Path, case: str,
) -> None:
    body = zip_bytes()
    rpc = DuplicateRPC(len(body))
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=body)
    )) as http:
        api = NapCat(123, http, clock=lambda: 1000)
        api.bot = rpc
        collector = Collector(settings(tmp_path), api=api, http=http, clock=lambda: 1000)
        try:
            record_id = await archive(collector, api)
            rpc.delete_result = 1 if case == 'denied' else 0
            rpc.delete_effective = case != 'false_success'
            rpc.fail_second = case == 'partial'
            await collector.cleanup()
            assert 'latest' in rpc.files and len(rpc.files) >= 2
            row = collector.record(record_id)
            assert row['duplicate_pending'] and row['deleted_at'] is None and row['delete_error']
            rpc.delete_result = 0
            rpc.delete_effective = True
            rpc.fail_second = False
            await collector.cleanup()
            assert set(rpc.files) == {'latest'}
            assert not collector.record(record_id)['duplicate_pending']
        finally:
            collector.close()


async def test_cancel_during_duplicate_hash_preserves_pending_and_releases_claim(
    tmp_path: Path,
) -> None:
    body = zip_bytes()
    rpc = DuplicateRPC(len(body))
    started = asyncio.Event()
    blocked = False

    async def serve(request: httpx.Request) -> httpx.Response:
        if blocked:
            started.set()
            await asyncio.Event().wait()
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        api = NapCat(123, http, clock=lambda: 1000)
        api.bot = rpc
        collector = Collector(settings(tmp_path), api=api, http=http, clock=lambda: 1000)
        try:
            record_id = await archive(collector, api)
            blocked = True
            task = asyncio.create_task(collector.cleanup())
            await asyncio.wait_for(started.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not collector.active and not rpc.deleted
            assert collector.record(record_id)['duplicate_pending']
            blocked = False
            await collector.cleanup()
            assert set(rpc.files) == {'latest'}
        finally:
            collector.close()


@pytest.mark.parametrize('same_count', [False, True])
async def test_another_copy_while_pruning_pending_extends_retention(
    tmp_path: Path, same_count: bool,
) -> None:
    body = zip_bytes()
    rpc = DuplicateRPC(len(body))
    now = [1000.0]
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=body)
    )) as http:
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        collector = Collector(settings(tmp_path), api=api, http=http, clock=lambda: now[0])
        try:
            record_id = await archive(collector, api)
            now[0] += 90000
            rpc.files['newer'] = {**rpc.files['latest'], 'modify_time': int(now[0])}
            if same_count:
                rpc.files.pop('middle')
            collector.register((await api.list_uploads())[0])
            deadline = now[0] + 86400
            await collector.cleanup()
            assert set(rpc.files) == {'newer'}
            assert collector.delete_after(collector.record(record_id)) == deadline
            assert collector.record(record_id)['source_kind'] == 'live'
        finally:
            collector.close()


async def test_environment_change_after_hashing_stops_duplicate_deletion(tmp_path: Path) -> None:
    body = zip_bytes()
    rpc = DuplicateRPC(len(body))
    armed = False

    def serve(request: httpx.Request) -> httpx.Response:
        if armed and request.url.path == '/oldest':
            collector.can_work = lambda: False  # type: ignore[method-assign]
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        api = NapCat(123, http, clock=lambda: 1000)
        api.bot = rpc
        collector = Collector(settings(tmp_path), api=api, http=http, clock=lambda: 1000)
        try:
            record_id = await archive(collector, api)
            armed = True
            await collector.cleanup()
            assert not rpc.deleted and collector.record(record_id)['duplicate_pending']
        finally:
            collector.close()
