import asyncio
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import pytest
from test_collector import zip_bytes
from test_napcat import FilesRPC

from ptilopsisbot.collector import Collector
from ptilopsisbot.config import Settings
from ptilopsisbot.files import CollectionError
from ptilopsisbot.napcat import NapCat

NOW = 1788758400.0


class StorageRPC(FilesRPC):
    def __init__(self, size: int) -> None:
        super().__init__(size)
        original = self.files.pop('source-a')
        self.files = {
            key: {**original, 'file_name': f'run-20260905-120000-{index:06d}.zip',
                  'upload_time': int(NOW - age * 3600), 'busid': 104}
            for index, (key, age) in enumerate((('newest', 1), ('oldest', 3), ('middle', 2)))
        }
        self.folders: dict[str, dict[str, Any]] = {
            'folder-a': {'files': [], 'folders': [{'folder_id': 'folder-b'}]},
            'folder-b': {'files': [], 'folders': []},
        }
        self.deleted: list[str] = []
        self.url_handles: dict[str, str] = {}
        self.invalid_count: str | None = None
        self.deny: set[str] = set()
        self.native_active = self.native_peak = 0

    async def call_api(self, api: str, **data: Any) -> Any:
        native = api in ('get_group_root_files', 'get_group_files_by_folder', 'delete_group_file')
        if native:
            self.native_active += 1
            self.native_peak = max(self.native_peak, self.native_active)
            await asyncio.sleep(0)
        try:
            if api == 'get_group_files_by_folder':
                assert data['file_count'] == 1000
                if self.invalid_count == 'query_error':
                    raise RuntimeError('folder request failed')
                if self.invalid_count == 'truncated':
                    return {'files': [], 'folders': [{'folder_id': 'x'}] * 1000}
                if self.invalid_count == 'missing_fields':
                    return {'files': []}
                if self.invalid_count == 'negative_size':
                    return {'files': [{'file_id': 'bad', 'file_size': -1}], 'folders': []}
                if self.invalid_count == 'duplicate_handle':
                    return {'files': [{'file_id': 'bad', 'file_size': 1}] * 2, 'folders': []}
                if self.invalid_count == 'cycle':
                    return {'files': [], 'folders': [{'folder_id': 'folder-a'}]}
                return self.folders[data['folder_id']]
            if api == 'get_group_file_url':
                self.url_handles[self.handles[data['file_id']]] = data['file_id']
            if api == 'delete_group_file':
                source = self.handles[data['file_id']]
                assert data['file_id'] == self.url_handles[source]
                if source in self.deny:
                    return {'result': 1}
                self.deleted.append(source)
            result = await super().call_api(api, **data)
            if api == 'get_group_root_files':
                assert isinstance(result, dict)
                result['folders'] = [{'folder_id': 'folder-a'}]
            return result
        finally:
            if native:
                self.native_active -= 1


def config(tmp_path: Path, threshold: int | None, **kwargs: Any) -> Settings:
    return Settings(
        group_id=123, data_dir=tmp_path, min_free_gib=0, send_receipts=False,
        delete_grace_hours=12, history_delete_grace_hours=0,
        group_storage_limit_gib=threshold / 1024**3 if threshold is not None else None,
        **kwargs,
    )


async def archive(collector: Collector, api: NapCat) -> dict[str, int]:
    ids = {}
    for upload in await api.list_uploads():
        record_id = collector.register(replace(upload, source_kind='live'))
        if record_id is not None:
            ids[upload.name] = record_id
            assert await collector.process_once()
    assert all(row['status'] == 'archived' for row in collector.records())
    return ids


async def test_usage_includes_nested_files_and_native_queries_do_not_overlap(
    tmp_path: Path,
) -> None:
    body = zip_bytes()
    size = len(body)
    rpc = StorageRPC(size)
    rpc.files['video'] = {**rpc.files['oldest'], 'file_name': 'video.mp4', 'file_size': size}
    rpc.folders['folder-b']['files'] = [{'file_id': 'installer', 'file_size': size}]
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=body)
    )) as http:
        api = NapCat(123, http, clock=lambda: NOW)
        api.bot = rpc
        collector = Collector(config(tmp_path, 3 * size), api=api, http=http, clock=lambda: NOW)
        try:
            await archive(collector, api)
            assert await api.storage_usage() == 5 * size
            await asyncio.gather(collector.cleanup(), collector.cleanup(), api.list_uploads(),
                                 api.storage_usage())
            assert rpc.deleted == ['oldest', 'middle']  # Timestamp order, not record ID order.
            assert set(rpc.files) == {'newest', 'video'}
            assert rpc.folders['folder-b']['files']
            assert collector.group_storage_bytes == 3 * size
            assert collector.storage_cleanup_error is None
            assert rpc.native_peak == 1
            assert collector.record(1)['deleted_at'] is None
            assert not collector.active and rpc.messages == []
        finally:
            collector.close()


@pytest.mark.parametrize('case', ['disabled', 'auto_delete_off', 'below', 'exact'])
async def test_threshold_is_opt_in_and_does_not_delete_below_limit(
    tmp_path: Path, case: str,
) -> None:
    body = zip_bytes()
    rpc = StorageRPC(len(body))
    threshold = {'disabled': None, 'auto_delete_off': len(body),
                 'below': 4 * len(body), 'exact': 3 * len(body)}[case]
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=body)
    )) as http:
        api = NapCat(123, http, clock=lambda: NOW)
        api.bot = rpc
        collector = Collector(config(tmp_path, threshold, auto_delete=case != 'auto_delete_off'),
                              api=api, http=http, clock=lambda: NOW)
        try:
            await archive(collector, api)
            await collector.cleanup()
            assert rpc.deleted == [] and len(rpc.files) == 3
        finally:
            collector.close()


@pytest.mark.parametrize('case', ['truncated', 'missing_fields', 'negative_size',
                                  'duplicate_handle', 'cycle', 'query_error'])
async def test_incomplete_usage_never_authorizes_early_deletion(tmp_path: Path, case: str) -> None:
    body = zip_bytes()
    rpc = StorageRPC(len(body))
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=body)
    )) as http:
        api = NapCat(123, http, clock=lambda: NOW)
        api.bot = rpc
        collector = Collector(config(tmp_path, len(body)), api=api, http=http, clock=lambda: NOW)
        try:
            await archive(collector, api)
            rpc.invalid_count = case
            await collector.cleanup()
            assert not rpc.deleted and len(rpc.files) == 3
            assert collector.group_storage_bytes is None and collector.storage_cleanup_error
            assert all(row['deleted_at'] is None for row in collector.records())
        finally:
            collector.close()


@pytest.mark.parametrize('case', ['freed', 'invalidated', 'reconnected'])
async def test_capacity_is_rechecked_after_remote_hash(tmp_path: Path, case: str) -> None:
    body = zip_bytes()
    rpc = StorageRPC(len(body))
    rpc.folders['folder-b']['files'] = [{'file_id': 'installer', 'file_size': len(body)}]
    armed = False

    def serve(request: httpx.Request) -> httpx.Response:
        if armed:
            if case == 'freed':
                rpc.folders['folder-b']['files'] = []
            elif case == 'invalidated':
                rpc.invalid_count = 'truncated'
            else:
                api.reset_receipts()
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        api = NapCat(123, http, clock=lambda: NOW)
        api.bot = rpc
        collector = Collector(
            config(tmp_path, 3 * len(body)), api=api, http=http, clock=lambda: NOW,
        )
        try:
            await archive(collector, api)
            armed = True
            await collector.cleanup()
            assert rpc.deleted == [] and len(rpc.files) == 3
            assert all(row['deleted_at'] is None for row in collector.records())
            if case == 'freed':
                assert collector.group_storage_bytes == 3 * len(body)
                assert collector.storage_cleanup_error is None
                assert all(row['delete_error'] is None for row in collector.records())
        finally:
            collector.close()


@pytest.mark.parametrize('case', ['corrupt_local', 'changed_remote', 'denied', 'false_success'])
async def test_unverified_or_failed_files_are_retained(tmp_path: Path, case: str) -> None:
    body = zip_bytes()
    rpc = StorageRPC(len(body))
    armed = False

    def serve(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b'x' * len(body) if (
            armed and case == 'changed_remote' and request.url.path == '/oldest'
        ) else body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        api = NapCat(123, http, clock=lambda: NOW)
        api.bot = rpc
        collector = Collector(
            config(tmp_path, 2 * len(body)), api=api, http=http, clock=lambda: NOW,
        )
        try:
            await archive(collector, api)
            armed = True
            if case == 'corrupt_local':
                (tmp_path / collector.record(2)['archive_path']).write_bytes(b'corrupt')
            elif case == 'denied':
                rpc.deny.add('oldest')
            elif case == 'false_success':
                rpc.delete_effective = False
            await collector.cleanup()
            assert 'oldest' in rpc.files
            assert collector.record(2)['deleted_at'] is None and collector.record(2)['delete_error']
            if case == 'false_success':
                assert len(rpc.files) == 3
                assert collector.storage_cleanup_error
            else:
                assert rpc.deleted == ['middle']
                assert set(rpc.files) == {'oldest', 'newest'}
        finally:
            collector.close()


async def test_capacity_cleanup_cancellation_can_resume_after_restart(tmp_path: Path) -> None:
    body = zip_bytes()
    rpc = StorageRPC(len(body))
    entered = asyncio.Event()
    blocked = False

    async def serve(request: httpx.Request) -> httpx.Response:
        if blocked:
            entered.set()
            await asyncio.Event().wait()
        return httpx.Response(200, content=body)

    async with httpx.AsyncClient(transport=httpx.MockTransport(serve)) as http:
        api = NapCat(123, http, clock=lambda: NOW)
        api.bot = rpc
        settings = config(tmp_path, 2 * len(body))
        collector = Collector(settings, api=api, http=http, clock=lambda: NOW)
        await archive(collector, api)
        blocked = True
        task = asyncio.create_task(collector.cleanup())
        try:
            await asyncio.wait_for(entered.wait(), 2)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not collector.active and not rpc.deleted
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            collector.close()
        blocked = False
        rpc.handles.clear()
        api = NapCat(123, http, clock=lambda: NOW)
        api.bot = rpc
        await api.refresh_status()
        collector = Collector(settings, api=api, http=http, clock=lambda: NOW)
        try:
            await collector.cleanup()
            assert rpc.deleted == ['oldest']
            assert set(rpc.files) == {'newest', 'middle'}
        finally:
            collector.close()


async def test_expired_cleanup_still_runs_when_capacity_count_fails(tmp_path: Path) -> None:
    body = zip_bytes()
    rpc = StorageRPC(len(body))
    now = [NOW]
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=body)
    )) as http:
        api = NapCat(123, http, clock=lambda: now[0])
        api.bot = rpc
        collector = Collector(config(tmp_path, len(body)), api=api, http=http, clock=lambda: now[0])
        try:
            await archive(collector, api)
            rpc.invalid_count = 'query_error'
            now[0] += 12 * 3600
            await collector.cleanup()
            assert not rpc.files
            assert collector.storage_cleanup_error == 'RuntimeError'
        finally:
            collector.close()


@pytest.mark.parametrize('value', [0, -1, float('inf'), float('nan')])
def test_invalid_storage_limit_rejected(value: float) -> None:
    with pytest.raises(ValueError):
        Settings(group_id=123, group_storage_limit_gib=value)


async def test_unknown_storage_response_is_an_error() -> None:
    class InvalidRPC(FilesRPC):
        async def call_api(self, api: str, **data: Any) -> Any:
            if api == 'get_group_root_files':
                return None
            return await super().call_api(api, **data)

    async with httpx.AsyncClient() as http:
        api = NapCat(123, http)
        api.bot = InvalidRPC()
        with pytest.raises(CollectionError, match='容量统计'):
            await api.storage_usage()
