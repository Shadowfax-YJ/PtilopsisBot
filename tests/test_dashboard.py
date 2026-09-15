import io
import logging
import threading
import time
from types import SimpleNamespace

import httpx
import pytest
from rich.console import Console

from ptilopsisbot.config import Settings
from ptilopsisbot.dashboard import LogTail, Model, StatusPoller, poll
from ptilopsisbot.logging_ui import UnifiedFormatter, framework_sink
from ptilopsisbot.progress_ui import Meter, Meters


def test_progress_bar_is_visible_and_unfinished_work_never_rounds_to_complete():
    output = io.StringIO()
    console = Console(file=output, width=90, legacy_windows=False, color_system=None)
    console.print(Meter("识别总队列", 99999, 100000, "条").display())
    rendered = output.getvalue()
    assert "99.9%" in rendered
    assert "100.0%" not in rendered
    assert "━" in rendered


def test_map_and_volume_progress_use_their_own_totals():
    meters = Meters()
    meters.feed("发布进度：整理地图表，已完成 0/1850 局")
    meters.feed("地图表进度：已保存 94/1850 局分片；下批续接")
    assert meters.rows["发布", "manifest"].completed == 94
    assert meters.rows["发布", "manifest"].total == 1850
    meters.feed("网盘分卷：本卷已写入 12.0 MiB 原始内容，已完成 150/800 个对象；到期先封存")
    assert meters.rows["发布", "volume"].completed == 150
    assert meters.rows["发布", "volume"].total == 800


def test_progress_counters_keep_batches_separate_and_never_invent_totals():
    meters = Meters()
    meters.feed("上传记录 10 下载归档 80%，40.0/50.0 MiB，平均 2.00 MiB/s")
    meters.feed("上传记录 11 下载归档 开始，共 100.0 MiB")
    meters.feed("识别队列：已完成 1000 条，待处理 3000 条")
    meters.feed("识别/整理进行中：候选 2048 条，本批已保存 500 条结果")
    assert meters.rows["下载", "10"].completed == 40
    assert meters.rows["下载", "11"].completed == 0
    assert meters.rows["处理", "queue"].completed == 1500
    assert meters.rows["处理", "queue"].total == 4000
    meters.feed("处理汇总：本批识别/整理 2048 条，复用 1000 条，失败 1 条，待续 952 条")
    assert meters.rows["处理", "queue"].completed == 3047
    assert meters.rows["处理", "queue"].total == 4000
    meters.feed("对象存储迁移：4 个 I/O 线程，本批已迁移 1234 个对象")
    assert meters.rows["迁移", "batch"].total is None
    meters.feed("迁移盘点：本次跟踪 10000 个散文件，剩余 8000 个")
    meters.feed("对象存储迁移：4 个 I/O 线程，本批已迁移 500 个对象")
    assert meters.rows["迁移", "total"].completed == 2500
    meters.feed("中心存储整理：本批迁移 500 个对象；下批继续")
    assert meters.rows["迁移", "total"].completed == 2500  # No double counting.
    meters.feed("迁移盘点：本次跟踪 10000 个散文件，剩余 7500 个")
    meters.feed("对象存储迁移：4 个 I/O 线程，本批已迁移 300 个对象")
    assert meters.rows["迁移", "total"].completed == 2800


def test_slow_status_request_does_not_block_refresh_or_keyboard():
    entered, release = threading.Event(), threading.Event()

    def handler(request):
        entered.set()
        assert release.wait(3)
        return httpx.Response(200, json={"online": True})

    model = Model()
    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        reader = StatusPoller(client, Settings(group_id=123))
        try:
            reader.tick(model)
            assert entered.wait(1)
            start = time.monotonic()
            for _ in range(20):
                reader.tick(model)
                model.key("3")
            assert time.monotonic() - start < 0.5
            assert model.selected == "迁移"
            assert model.status == {}
        finally:
            release.set()
            reader.close()
        reader.tick(model)
        assert model.connection == "QQ 在线"


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
