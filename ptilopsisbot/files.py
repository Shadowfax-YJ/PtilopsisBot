import asyncio
import hashlib
import logging
import zlib
from collections.abc import Callable
from contextlib import nullcontext, suppress
from pathlib import Path
from time import perf_counter
from typing import TypeVar
from zipfile import BadZipFile, ZipFile

import httpx

CHUNK = 256 * 1024
log = logging.getLogger(__name__)
T = TypeVar("T")


class CollectionError(ValueError):
    """A controlled diagnostic that is safe to store without URLs or credentials."""


class InvalidPackage(CollectionError):
    pass


class SourceFileMissing(CollectionError):
    """Defer cleanup until a root scan observes this source again."""

    def __init__(self) -> None:
        super().__init__("根目录未发现源文件，可能已删除或移动，等待补扫再次发现后恢复清理")


class StorageLimitSatisfied(CollectionError):
    """Space was freed during verification; do not delete another retained file."""


async def file_check(operation: Callable[[], T]) -> T:
    task = asyncio.create_task(asyncio.to_thread(operation))
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        # A thread keeps its Windows file handle after task cancellation. Let it
        # finish before the caller removes the .part file or releases its claim.
        with suppress(Exception):
            await task
        raise


async def download(
    http: httpx.AsyncClient,
    url: str,
    target: Path | None,
    expected_size: int,
    max_size: int,
    *,
    label: str = "下载",
) -> str:
    digest = hashlib.sha256()
    total = 0
    started = last_progress = perf_counter()
    log.info("%s 开始，共 %.1f MiB", label, expected_size / 1024**2)
    async with http.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
        response.raise_for_status()
        if response.headers.get("Content-Encoding", "identity") != "identity":
            raise CollectionError("下载响应使用了非预期的 Content-Encoding")
        length = response.headers.get("Content-Length")
        if length is not None:
            try:
                declared_size = int(length)
            except ValueError:
                raise CollectionError("下载响应的 Content-Length 不是有效整数") from None
            if declared_size != expected_size:
                raise CollectionError("Content-Length 与群文件声明大小不符")
        # Cleanup hashes the remote stream again without storing a second local copy.
        with target.open("wb") if target is not None else nullcontext() as output:
            async for chunk in response.aiter_bytes(CHUNK):
                total += len(chunk)
                if total > max_size or total > expected_size:
                    raise CollectionError("下载超过文件大小上限")
                if output is not None:
                    output.write(chunk)
                digest.update(chunk)
                now = perf_counter()
                if now - last_progress >= 10:
                    log.info(
                        "%s %.0f%%，%.1f/%.1f MiB，平均 %.2f MiB/s",
                        label,
                        total * 100 / expected_size,
                        total / 1024**2,
                        expected_size / 1024**2,
                        total / 1024**2 / max(now - started, 0.001),
                    )
                    last_progress = now
    if total != expected_size:
        raise CollectionError("下载不完整，实收大小与群文件声明不符")
    elapsed = max(perf_counter() - started, 0.001)
    log.info("%s 完成，耗时 %.1f 秒，平均 %.2f MiB/s", label, elapsed, total / 1024**2 / elapsed)
    return digest.hexdigest()


def check_zip(path: Path) -> None:
    limit = 2 * 1024**3
    try:
        with ZipFile(path) as archive:
            members = archive.infolist()
            if len(members) > 10_000 or sum(item.file_size for item in members) > limit:
                raise InvalidPackage("ZIP 成员数量或总解压量超限")
            total = 0
            for member in members:
                if member.flag_bits & 1:
                    raise InvalidPackage("不接受加密 ZIP")
                with archive.open(member) as content:
                    while chunk := content.read(CHUNK):
                        total += len(chunk)
                        if total > limit:
                            raise InvalidPackage("ZIP 实际解压量超限")
    except (BadZipFile, NotImplementedError, EOFError, zlib.error) as exc:
        raise InvalidPackage(f"ZIP 不可读: {exc}") from exc


def file_matches(path: Path, expected_size: int, expected_hash: str) -> bool:
    if not path.is_file() or path.stat().st_size != expected_size:
        return False
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest() == expected_hash
