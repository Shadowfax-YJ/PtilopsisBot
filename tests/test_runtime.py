import asyncio
import json
import socket
import subprocess
import sys
import threading
import time
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from zipfile import ZipFile
from zoneinfo import ZoneInfo

import httpx
import pytest
from websockets.asyncio.client import connect


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@pytest.mark.parametrize(
    ("live", "missing_time", "backlog"),
    [(True, False, False), (False, False, False), (True, True, False), (True, True, True)],
    ids=["live-quoted", "backfill-silent", "live-quoted-zero-time", "live-during-backlog"],
)
async def test_real_onebot_websocket_collects_100mib_then_deletes_and_reports(
    tmp_path: Path,
    live: bool,
    missing_time: bool,
    backlog: bool,
) -> None:
    package = tmp_path / "sample.zip"
    with ZipFile(package, "w") as archive:
        with archive.open("run.bin", "w") as data:
            for _ in range(100):
                data.write(b"a" * 1024**2)

    release_history = threading.Event()

    class DownloadHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path.startswith("/old-"):
                release_history.wait(30)
            self.send_response(200)
            self.send_header("Content-Length", str(package.stat().st_size))
            self.end_headers()
            with package.open("rb") as source:
                while chunk := source.read(256 * 1024):
                    self.wfile.write(chunk)

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), DownloadHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = free_port()
    config = tmp_path / "config.toml"
    config.write_text(
        f'group_id = 123\naccess_token = "test-only-token"\nport = {port}\n'
        "delete_grace_hours = 0\nmin_free_gib = 0\n",
        encoding="utf-8",
    )
    headers = {"Authorization": "Bearer test-only-token"}
    group_files: set[str] = set() if live else {"file-a"}
    if backlog:
        group_files.update({"old-a", "old-b", "old-c"})
    names = {
        key: f"run-20260905-120000-{number:06d}.zip"
        for number, key in enumerate(("file-a", "old-a", "old-b", "old-c"), start=1)
    }
    uploaded_at = 1788580800
    handles: dict[str, str] = {}
    scan_seen = asyncio.Event()
    reports: list[str] = []
    replies: list[str] = []
    log_path = tmp_path / "bot.log"
    with log_path.open("wb") as output:
        process = subprocess.Popen(
            [sys.executable, "-X", "utf8", "-m", "ptilopsisbot", "--config", str(config), "run"],
            stdout=output,
            stderr=subprocess.STDOUT,
        )
        try:
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=5) as http:
                for _ in range(100):
                    if process.poll() is not None:
                        raise AssertionError(log_path.read_text(encoding="utf-8"))
                    try:
                        if (
                            await http.get("/ptilopsisbot/status", headers=headers)
                        ).status_code == 200:
                            break
                    except httpx.ConnectError:
                        pass
                    await asyncio.sleep(0.1)
                else:
                    raise AssertionError("Bot did not start")
                assert (await http.get("/ptilopsisbot/status")).status_code == 401
                async with connect(
                    f"ws://127.0.0.1:{port}/onebot/v11/",
                    additional_headers={**headers, "X-Self-ID": "999"},
                ) as websocket:

                    async def respond() -> None:
                        generation = 0
                        async for message in websocket:
                            request = json.loads(message)
                            action = request["action"]
                            if action != "get_status":
                                assert request["params"]["group_id"] == 123
                            if action == "get_status":
                                data = {"online": True, "good": True}
                            elif action == "get_group_root_files":
                                generation += 1
                                current_handles = {
                                    f"list-{generation}-{source}": source
                                    for source in sorted(group_files)
                                }
                                # NapCat caches each handle until expiry/restart; another
                                # concurrent listing doesn't invalidate an in-flight handle.
                                handles.update(current_handles)
                                scan_seen.set()
                                data = {
                                    "files": [
                                        {
                                            "file_id": handle,
                                            "file_name": names[source],
                                            "file_size": package.stat().st_size,
                                            "busid": 102,
                                            "uploader": 456,
                                            "uploader_name": "测试成员",
                                            "upload_time": 0
                                            if missing_time
                                            else (
                                                uploaded_at if source == "file-a" else 1788580800
                                            ),
                                        }
                                        for handle, source in current_handles.items()
                                    ],
                                    "folders": [],
                                }
                            elif action == "get_group_file_url":
                                assert request["params"]["file_id"] in handles
                                source = handles[request["params"]["file_id"]]
                                data = {
                                    "url": f"http://127.0.0.1:{server.server_port}/{source}.zip"
                                }
                            elif action == "delete_group_file":
                                group_files.remove(handles[request["params"]["file_id"]])
                                data = {"result": 0, "errMsg": ""}
                            elif action == "send_group_msg":
                                segments = request["params"]["message"]
                                reports.append(
                                    "".join(
                                        segment["data"]["text"]
                                        for segment in segments
                                        if segment["type"] == "text"
                                    )
                                )
                                replies.extend(
                                    segment["data"]["id"]
                                    for segment in segments
                                    if segment["type"] == "reply"
                                )
                                data = {"message_id": 10}
                            else:
                                raise AssertionError(action)
                            await websocket.send(
                                json.dumps(
                                    {
                                        "status": "ok",
                                        "retcode": 0,
                                        "data": data,
                                        "echo": request["echo"],
                                    }
                                )
                            )

                    responder = asyncio.create_task(respond())
                    try:
                        await asyncio.wait_for(scan_seen.wait(), timeout=5)
                        if backlog:
                            for _ in range(100):
                                state = (
                                    await http.get("/ptilopsisbot/status", headers=headers)
                                ).json()
                                if len(state["active_downloads"]) == 2:
                                    break
                                await asyncio.sleep(0.1)
                            else:
                                raise AssertionError(log_path.read_text(encoding="utf-8"))
                            assert state["download_concurrency"] == 3
                            assert len(state["records"]) == 3
                        if live:
                            uploaded_at = int(time.time())
                        group_files.add("file-a")
                        notice = {
                            "time": uploaded_at,
                            "self_id": 999,
                            "post_type": "notice",
                            "notice_type": "group_upload",
                            "group_id": 123,
                            "user_id": 456,
                            "file": {
                                "id": "message-handle-not-a-group-file-handle",
                                "name": "run-20260905-120000-000001.zip",
                                "size": package.stat().st_size,
                                "busid": 102,
                            },
                        }
                        await websocket.send(json.dumps(notice))
                        await websocket.send(json.dumps(notice))
                        file_message = {
                            "time": uploaded_at,
                            "self_id": 999,
                            "post_type": "message",
                            "message_type": "group",
                            "sub_type": "normal",
                            "group_id": 123,
                            "user_id": 456,
                            "message_id": -42,
                            "message": [
                                {
                                    "type": "file",
                                    "data": {
                                        "file": "run-20260905-120000-000001.zip",
                                        "file_id": "native-file-uuid",
                                        "file_size": str(package.stat().st_size),
                                    },
                                }
                            ],
                            "raw_message": "",
                            "font": 0,
                            "sender": {"user_id": 456, "nickname": "测试成员"},
                        }
                        await websocket.send(json.dumps(file_message))
                        await websocket.send(json.dumps(file_message))
                        for _ in range(200):
                            state = (await http.get("/ptilopsisbot/status", headers=headers)).json()
                            target = next(
                                (row for row in state["records"] if row["name"] == names["file-a"]),
                                None,
                            )
                            if target and target["deleted_at"]:
                                break
                            if responder.done():
                                await responder
                            await asyncio.sleep(0.1)
                        else:
                            raise AssertionError(log_path.read_text(encoding="utf-8"))
                        assert target is not None
                        assert group_files == ({"old-a", "old-b", "old-c"} if backlog else set())
                        assert len(state["records"]) == (4 if backlog else 1)
                        assert "1970-01-01" not in target["archive_path"]
                        assert len(reports) == int(live)
                        assert replies == (["-42"] if live else [])
                        if live:
                            assert "run-20260905-120000-000001.zip" in reports[0]
                        saved = tmp_path / "data" / target["archive_path"]
                        assert saved.stat().st_size == package.stat().st_size
                        with ZipFile(saved) as archive:
                            assert archive.testzip() is None
                        if backlog:
                            assert len(state["active_downloads"]) == 2
                            pending = next(
                                row for row in state["records"] if row["name"] == names["old-c"]
                            )
                            assert pending["attempts"] == 0
                            release_history.set()
                            for _ in range(200):
                                state = (
                                    await http.get("/ptilopsisbot/status", headers=headers)
                                ).json()
                                if all(row["deleted_at"] for row in state["records"]):
                                    break
                                if responder.done():
                                    await responder
                                await asyncio.sleep(0.1)
                            else:
                                raise AssertionError(log_path.read_text(encoding="utf-8"))
                            assert not group_files
                            assert len(reports) == 1  # History stays silent after draining.
                        day = datetime.fromtimestamp(uploaded_at, ZoneInfo("Asia/Shanghai")).date()
                        response = await http.post(f"/ptilopsisbot/report/{day}", headers=headers)
                        assert response.status_code == 200
                        assert len(reports) == int(live) + 1
                        assert "新增唯一包 1" in reports[-1]
                        logs = log_path.read_text(encoding="utf-8")
                        assert "下载归档 完成" in logs
                        assert "删源前远端校验" in logs
                        assert "MiB/s" in logs
                        if live:
                            assert "收包回复已发送" in logs
                            assert "源消息=-42，回复消息=10" in logs
                        else:
                            assert "跳过回执" in logs
                    finally:
                        responder.cancel()
                        await asyncio.gather(responder, return_exceptions=True)
        finally:
            release_history.set()
            process.terminate()
            process.wait(timeout=10)
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
