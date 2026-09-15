import io
import logging
from types import SimpleNamespace

import httpx
import pytest
from rich.console import Console

from ptilopsisbot.config import Settings
from ptilopsisbot.dashboard import LogTail, Model, poll
from ptilopsisbot.logging_ui import UnifiedFormatter, framework_sink


def test_tail_partial_utf8_and_rotation(tmp_path):
    path = tmp_path / "collector.log"
    tail = LogTail(path)
    assert tail.read() == []
    data = "进度一\n进度二\n".encode()
    path.write_bytes(data[:-2])
    assert tail.read() == ["进度一"]
    with path.open("ab") as stream:
        stream.write(data[-2:])
    assert tail.read() == ["进度二"]
    assert tail.read() == []
    path.rename(tmp_path / "collector.log.1")
    path.write_text("新日志\n", encoding="utf-8")
    assert tail.read() == ["新日志"]
    path.write_text("短\n", encoding="utf-8")
    assert tail.read() == ["短"]


def test_parallel_downloads_classification_safety_and_bounded_history():
    model = Model()
    for number in range(1000):
        model.feed(f"2026-09-15 23:25:27,180 INFO ptilopsisbot.files "
                   f"上传记录 {number % 3} 下载归档 {number}%")
    model.feed("2026-09-15 23:25:29,180 WARNING [迁移] ptilopsisbot.plugins | "
               "[red]失败\x1b[2J https://secret-url/token")
    assert len(model.events) == 600
    assert len([key for key in model.latest if key[0] == "下载"]) == 3
    assert len(model.alerts) == 1
    assert "\x1b" not in model.alerts[0].message
    assert "secret-url" not in model.alerts[0].message
    assert "[red]" in model.alerts[0].message  # Literal text, never Rich markup.
    model.key("6")
    assert model.selected == "告警"
    model.key("p")
    assert model.paused
    model.feed("2026-09-15 23:25:30,180 INFO ptilopsisbot.plugins 对象存储迁移：1 个")
    assert list(model.latest.values())[-1].message.endswith("1 个")


@pytest.mark.parametrize("width,height", [(60, 24), (80, 24), (120, 36), (160, 45)])
def test_render_small_and_large_terminals(width, height):
    model = Model()
    model.accept({"online": True, "active_downloads": [1, 2], "download_concurrency": 3,
                  "group_storage_bytes": 4 * 1024**3, "group_storage_limit_gib": 8})
    model.feed("2026-09-15 23:25:27,180 INFO ptilopsisbot.files 上传记录 1 下载归档 80%")
    out = io.StringIO()
    console = Console(file=out, width=width, height=height, color_system=None)
    console.print(model.render(width, height))
    text = out.getvalue()
    assert "QQ 在线" in text
    assert "4.00 GiB" in text
    assert "80%" in text
    assert "退出面板" in text
    assert "下载 2/3" in text
    assert len(text.splitlines()) == height


def test_attach_legacy_endpoint_read_only_and_reconnect():
    requests = []
    online = [True]

    def handler(request):
        requests.append(request)
        if not online[0]:
            raise httpx.ConnectError("offline")
        if request.url.path.endswith("overview"):
            return httpx.Response(404)
        return httpx.Response(200, json={"online": True, "active_downloads": [42]})

    model = Model()
    cfg = Settings(group_id=123)
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        path = poll(client, cfg, model, "overview")
        assert path == "status"
        assert model.status["active_downloads"] == [42]
        online[0] = False
        poll(client, cfg, model, path)
        assert "自动重连" in model.connection
        assert model.status["active_downloads"] == [42]
        online[0] = True
        poll(client, cfg, model, path)
        assert model.connection == "QQ 在线"
    assert all(request.method == "GET" for request in requests)


def test_framework_bridge_filters_chatter_but_keeps_error_and_traceback(caplog):
    def emit(message, level=20, exception=None):
        framework_sink(SimpleNamespace(record={
            "name": "nonebot", "message": message, "level": SimpleNamespace(no=level),
            "exception": exception,
        }))

    with caplog.at_level(logging.INFO):
        emit("Event will be handled by Matcher(type='notice')")
        emit("OneBot V11 123 | [message.group.normal]: private file payload", 25)
        emit("connection ready", 25)
        try:
            raise ValueError("bad archive")
        except ValueError as exc:
            emit("worker failed", 40, SimpleNamespace(type=type(exc), value=exc,
                                                       traceback=exc.__traceback__))
    assert len(caplog.records) == 2
    formatter = UnifiedFormatter()
    output = "\n".join(formatter.format(record) for record in caplog.records)
    assert "[系统] ptilopsisbot.framework |" in output
    assert "ValueError: bad archive" in output
    assert "private file" not in output


def test_unified_formatter_escapes_control_sequences_and_signed_urls():
    record = logging.LogRecord("ptilopsisbot.files", logging.INFO, "", 0,
                               "上传记录 1 下载 https://example.com/token\x1b[2J", (), None)
    output = UnifiedFormatter().format(record)
    assert "[下载]" in output
    assert "example.com" not in output
    assert "\x1b" not in output
