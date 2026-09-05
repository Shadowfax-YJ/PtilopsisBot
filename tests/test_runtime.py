import asyncio
import json
import socket
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from zipfile import ZipFile

import httpx
from websockets.asyncio.client import connect


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


async def test_real_onebot_websocket_collects_100mib_then_deletes_and_reports(
    tmp_path: Path,
) -> None:
    package = tmp_path / "sample.zip"
    with ZipFile(package, "w") as archive:
        with archive.open("run.bin", "w") as data:
            for _ in range(100):
                data.write(b"a" * 1024**2)

    class DownloadHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
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
    group_files: set[str] = set()
    handles: dict[str, str] = {}
    scan_seen = asyncio.Event()
    reports: list[str] = []
    log_path = tmp_path / "bot.log"
    with log_path.open("wb") as output:
        process = subprocess.Popen(
            [sys.executable, "-X", "utf8", "-m", "databot", "--config", str(config), "run"],
            stdout=output,
            stderr=subprocess.STDOUT,
        )
        try:
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}", timeout=5) as http:
                for _ in range(100):
                    if process.poll() is not None:
                        raise AssertionError(log_path.read_text(encoding="utf-8"))
                    try:
                        if (await http.get("/databot/status", headers=headers)).status_code == 200:
                            break
                    except httpx.ConnectError:
                        pass
                    await asyncio.sleep(0.1)
                else:
                    raise AssertionError("Bot did not start")
                assert (await http.get("/databot/status")).status_code == 401
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
                                handles.clear()
                                handles.update(
                                    {
                                        f"list-{generation}-{source}": source
                                        for source in group_files
                                    }
                                )
                                scan_seen.set()
                                data = {
                                    "files": [
                                        {
                                            "file_id": next(iter(handles)),
                                            "file_name": "run-20260905-120000-000001.zip",
                                            "file_size": package.stat().st_size,
                                            "busid": 102,
                                            "uploader": 456,
                                            "uploader_name": "测试成员",
                                            "upload_time": 1788580800,
                                        }
                                    ]
                                    if group_files
                                    else [],
                                    "folders": [],
                                }
                            elif action == "get_group_file_url":
                                assert request["params"]["file_id"] in handles
                                data = {"url": f"http://127.0.0.1:{server.server_port}/run.zip"}
                            elif action == "delete_group_file":
                                group_files.remove(handles[request["params"]["file_id"]])
                                data = {"result": 0, "errMsg": ""}
                            elif action == "send_group_msg":
                                reports.append(request["params"]["message"][0]["data"]["text"])
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
                        group_files.add("file-a")
                        notice = {
                            "time": 1788580830,
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
                        for _ in range(200):
                            state = (await http.get("/databot/status", headers=headers)).json()
                            if state["records"] and state["records"][0]["deleted_at"]:
                                break
                            if responder.done():
                                await responder
                            await asyncio.sleep(0.1)
                        else:
                            raise AssertionError(log_path.read_text(encoding="utf-8"))
                        assert group_files == set()
                        assert len(state["records"]) == 1
                        saved = tmp_path / "data" / state["records"][0]["archive_path"]
                        assert saved.stat().st_size == package.stat().st_size
                        with ZipFile(saved) as archive:
                            assert archive.testzip() is None
                        response = await http.post("/databot/report/2026-09-05", headers=headers)
                        assert response.status_code == 200
                        assert "新增唯一包 1" in reports[0]
                    finally:
                        responder.cancel()
                        await asyncio.gather(responder, return_exceptions=True)
        finally:
            process.terminate()
            process.wait(timeout=10)
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)
