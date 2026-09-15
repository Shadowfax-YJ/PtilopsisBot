import io
import logging
import threading
import time
from types import SimpleNamespace

import httpx
import pytest
from rich.console import Console

from ptilopsisbot import dashboard
from ptilopsisbot.config import Settings
from ptilopsisbot.dashboard import LogTail, Model, StatusPoller, poll
from ptilopsisbot.logging_ui import UnifiedFormatter, framework_sink
from ptilopsisbot.progress_ui import Meter, Meters


def test_completed_release_and_idle_cycle_settle_stale_stage_bars():
    meters = Meters()
    meters.feed("并行识别：本批 1000 条图像任务、739 条日志任务")
    meters.feed("计算设备确认：cuda；本批完成 1739 条")
    meters.feed("核对识别缓存：127775/293036 条，内容重验 50 条")
    meters.feed("发布进度：核对识别关系，已完成 265889/291050 条")
    meters.feed("网盘分卷：本卷已写入 173.0 MiB，已完成 0/1 个对象")
    meters.feed("网盘分卷：已保存第 21 个新增分卷，包含 1 个对象")
    assert meters.rows["发布", "volume"].completed == 1
    assert meters.rows["发布", "volume"].finished
    meters.feed("发布完成：20260916-004735")
    assert [key for category, key in meters.rows if category == "发布"] == ["release"]
    meters.feed("处理汇总：本批识别/整理 0 条，复用 293036 条，失败 0 条，待续 0 条；"
                "数据继续处理，等待次日 00:05 发布；当前数据版本 20260916-004735；"
                "网盘分卷已就绪（191 卷，713455 个对象）")
    assert ("处理", "batch") not in meters.rows
    cache = meters.rows["处理", "cache"]
    assert cache.finished and cache.completed == cache.total == 293036
    assert all(m.finished and not m.active for m in meters.select("发布", 3))
    meters.feed("迁移盘点：本次跟踪 389826 个散文件，剩余 6778 个")
    assert not cache.active
    meters.feed("中心存储整理：本批迁移 6778 个对象；散文件迁移完成")
    assert meters.rows["迁移", "total"].finished
    assert meters.rows["迁移", "total"].completed == 389826
    meters.feed("发布数据：正在冻结 1860 局并导出到同步目录")
    meters.feed("发布进度：校验发布对象，已完成 0/800000 个")
    assert ("发布", "release") not in meters.rows
    assert not meters.rows["发布", "manifest"].finished


def test_stage_switch_waits_without_inventing_completion_and_keeps_overall_visible():
    meters = Meters()
    meters.feed("识别队列：已完成 100 条，待处理 20 条")
    meters.feed("核对识别缓存：50/100 条")
    meters.feed("发布进度：整理地图表，已完成 0/50 局")
    cache = meters.rows["处理", "cache"]
    assert not cache.active and not cache.finished and cache.completed == 50
    meters.feed("处理汇总：本批识别/整理 0 条，复用 100 条，失败 0 条，待续 20 条；"
                "发布校验/导出已保存进度，下批续接")
    assert not meters.rows["发布", "manifest"].finished
    assert not meters.rows["发布", "manifest"].active
    assert meters.rows["处理", "queue"].completed == 100
    assert meters.rows["处理", "queue"].total == 120


def test_transfer_selection_preserves_recency_and_active_counts_are_explicit():
    model = Model()
    model.meters.feed("删源前远端校验 old.zip 完成，平均 1.0 MiB/s")
    model.meters.feed("上传记录 12 下载归档 开始，共 100.0 MiB")
    model.meters.feed("上传记录 12 下载归档 完成，平均 2.0 MiB/s")
    assert model.meters.select("下载", 1)[0].label == "下载 #12"
    model.accept({"online": True, "active_downloads": [1, 2], "active_cleanup": [3],
                  "download_concurrency": 3, "cleanup_concurrency": 3})
    assert model.activity("下载", "active_downloads", "download_concurrency").startswith("下载 2/3")
    assert model.activity("清理", "active_cleanup", "cleanup_concurrency").startswith("清理 1/3")
    model.status["active_cleanup"] = []
    assert "空闲" in model.activity("清理", "active_cleanup", "cleanup_concurrency")


def test_render_cadence_is_one_fps_without_slowing_stop_checks(tmp_path, monkeypatch):
    now, frames, waits = [0.0], [], []

    class Stop:
        def is_set(self):
            return now[0] >= 3.1

        def wait(self, delay):
            waits.append(delay)
            now[0] += delay

    class Live:
        def __init__(self, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def update(self, renderable, **kwargs):
            frames.append(now[0])

    monkeypatch.setattr(dashboard.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(dashboard, "Console", lambda **kw:
                        SimpleNamespace(is_terminal=True, width=140, height=38))
    monkeypatch.setattr(dashboard, "Live", Live)
    monkeypatch.setattr(dashboard, "StatusPoller", lambda *args:
                        SimpleNamespace(tick=lambda model: None, close=lambda: None))
    monkeypatch.setattr(dashboard, "read_key", lambda: "")
    dashboard.watch(Settings(group_id=123, data_dir=tmp_path), stop=Stop())
    assert len(frames) == 4
    assert all(b - a >= 1 for a, b in zip(frames, frames[1:]))
    assert max(waits) <= 0.1


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
