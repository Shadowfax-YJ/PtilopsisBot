import hashlib
from pathlib import Path
from zipfile import BadZipFile, ZipFile

import httpx

CHUNK = 256 * 1024


class InvalidPackage(ValueError):
    pass


async def download(
    http: httpx.AsyncClient, url: str, target: Path, expected_size: int, max_size: int
) -> str:
    digest = hashlib.sha256()
    total = 0
    async with http.stream("GET", url, headers={"Accept-Encoding": "identity"}) as response:
        response.raise_for_status()
        if response.headers.get("Content-Encoding", "identity") != "identity":
            raise ValueError("下载响应使用了非预期的 Content-Encoding")
        length = response.headers.get("Content-Length")
        if length is not None and int(length) != expected_size:
            raise ValueError("Content-Length 与群文件声明大小不符")
        with target.open("wb") as output:
            async for chunk in response.aiter_bytes(CHUNK):
                total += len(chunk)
                if total > max_size or total > expected_size:
                    raise ValueError("下载超过文件大小上限")
                output.write(chunk)
                digest.update(chunk)
    if total != expected_size:
        raise ValueError("下载不完整，实收大小与群文件声明不符")
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
    except (BadZipFile, NotImplementedError, EOFError) as exc:
        raise InvalidPackage(f"ZIP 不可读: {exc}") from exc


def file_matches(path: Path, expected_size: int, expected_hash: str) -> bool:
    if not path.is_file() or path.stat().st_size != expected_size:
        return False
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest() == expected_hash
