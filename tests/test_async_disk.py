import asyncio
import threading
from pathlib import Path

import httpx
import pytest

from ptilopsisbot.files import download


async def test_slow_archive_write_yields_and_cancellation_drains_before_close(
    tmp_path, monkeypatch
):
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()
    target = tmp_path / "download.part"
    main_thread = threading.get_ident()
    real_open = Path.open

    class Writer:
        def __enter__(self):
            self.stream = real_open(target, "wb")
            return self

        def __exit__(self, *args):
            self.stream.close()
            closed.set()

        def write(self, chunk):
            entered.set()
            assert threading.get_ident() != main_thread
            assert release.wait(3)
            return self.stream.write(chunk)

        def flush(self):
            self.stream.flush()

    monkeypatch.setattr(Path, "open", lambda path, *args, **kwargs:
                        Writer() if path == target else real_open(path, *args, **kwargs))
    body = b"data" * 65536
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=body)
    )) as client:
        task = asyncio.create_task(download(client, "https://test.invalid/file", target,
                                             len(body), len(body)))
        try:
            assert await asyncio.to_thread(entered.wait, 1)
            task.cancel()
            await asyncio.sleep(0.02)
            assert not task.done()
            assert not closed.is_set()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert closed.is_set()
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
