"""Read-only terminal dashboard; attaching never constructs a Collector."""

import os
import re
import sys
import threading
import time
from collections import OrderedDict, deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from rich.console import Console, Group
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text

from .config import Settings
from .logging_ui import channel, clean
from .progress_ui import Meters

CHANNELS = ("全部", "下载", "处理", "迁移", "发布", "清理", "告警")
LOG_LINE = re.compile(
    r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d[,.]\d+)\s+"
    r"(\w+)\s+(?:\[([^]]+)\]\s+)?([\w.]+)\s+(?:\|\s+)?(.*)$"
)


@dataclass
class Event:
    stamp: str
    level: str
    category: str
    message: str


class LogTail:
    """Bounded binary tail, including UTF-8 partial lines and rotation."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.identity: tuple[int, int] | None = None
        self.offset = 0
        self.partial = b""

    def read(self) -> list[str]:
        try:
            with self.path.open("rb") as stream:
                info = os.fstat(stream.fileno())
                identity = (info.st_dev, info.st_ino)
                if identity != self.identity or info.st_size < self.offset:
                    self.identity, self.partial = identity, b""
                    self.offset = max(0, info.st_size - 128 * 1024)
                    if self.offset:
                        stream.seek(self.offset)
                        stream.readline()
                        self.offset = stream.tell()
                stream.seek(self.offset)
                data = stream.read(256 * 1024)
                self.offset = stream.tell()
        except OSError:
            return []
        lines = (self.partial + data).split(b"\n")
        self.partial = lines.pop()[-64 * 1024:]
        return [line.decode("utf-8", errors="replace").rstrip("\r") for line in lines]


class Model:
    def __init__(self) -> None:
        self.status: dict[str, Any] = {}
        self.events: deque[Event] = deque(maxlen=600)
        self.alerts: deque[Event] = deque(maxlen=100)
        self.latest: OrderedDict[tuple[str, str], Event] = OrderedDict()
        self.connection = "正在连接本机机器人"
        self.updated = "—"
        self.selected = "全部"
        self.paused = False
        self.meters = Meters()

    def feed(self, line: str) -> None:
        match = LOG_LINE.match(line)
        if match is None:
            return
        stamp, level, category, name, message = match.groups()
        message = clean(message)
        if "ptilopsisbot/overview" in message or "ptilopsisbot/status" in message:
            return
        self.meters.feed(message)
        event = Event(stamp[11:19], level, category or channel(name, message), message)
        self.events.append(event)
        if level in {"WARNING", "ERROR", "CRITICAL"}:
            self.alerts.append(event)
        # Separate overlapping uploads, while replacing each task's progress.
        task = re.search(r"上传记录\s+(\d+)", message)
        stage = re.sub(r"^\[[^]]+\]\s*", "", message).split("：", 1)[0]
        key = (event.category, task[1] if task else stage[:24])
        self.latest[key] = event
        self.latest.move_to_end(key)
        while len(self.latest) > 80:
            self.latest.popitem(last=False)

    def accept(self, status: dict[str, Any]) -> None:
        self.status = status
        self.connection = "QQ 在线" if status.get("online") else "QQ 离线"
        self.updated = time.strftime("%H:%M:%S")

    def key(self, key: str) -> None:
        if key in "0123456" and key:
            self.selected = CHANNELS[int(key)]
        elif key.lower() == "p":
            self.paused = not self.paused

    def task_panel(self, category: str, height: int) -> Panel:
        meters = self.meters.select(category, max(1, (height - 2) // 2))
        if meters:
            running = any(row.get("status") == "running"
                          for row in self.status.get("plugin_maintenance", []))
            return Panel(Group(*(meter.display(running=running or category in {"下载", "清理"})
                                 for meter in meters)),
                         title="下载 / 删前校验" if category == "下载" else category,
                         border_style="cyan")
        table = Table.grid(expand=True, padding=(0, 1))
        table.add_column(width=8, min_width=8, max_width=8, style="dim", no_wrap=True)
        table.add_column(ratio=1, overflow="ellipsis", no_wrap=True)
        rows = [v for k, v in self.latest.items() if k[0] == category]
        for event in rows[-max(1, height - 2):]:
            table.add_row(Text(event.stamp), Text(event.message))
        if not rows:
            table.add_row("", "尚无近期日志")
        return Panel(table, title=f"{category} · 最近进度", border_style="cyan")

    def render(self, width: int, height: int, *, attached: bool = True) -> Layout:
        status = self.status
        counts = status.get("counts", {})
        used = status.get("group_storage_bytes")
        storage = f"{used / 1024**3:.2f} GiB" if isinstance(used, (int, float)) else "未知"
        header = Text("PTILOPSIS  /  运行面板", style="bold cyan")
        header.append(f"    {self.connection}    状态更新 {self.updated}", style="white")
        header.append(f"\n群文件 {storage} / {status.get('group_storage_limit_gib', '—')} GiB"
                      f"    下载 {len(status.get('active_downloads', []))}"
                      f"/{status.get('download_concurrency', '—')}"
                      f"    清理 {len(status.get('active_cleanup', []))}"
                      f"/{status.get('cleanup_concurrency', '—')}"
                      f"    已归档 {counts.get('archived', 0) if 'counts' in status else '—'}"
                      f"    排队 {counts.get('queued', 0) if 'counts' in status else '—'}",
                      style="white")
        maintenance = status.get("plugin_maintenance", [])
        summaries = []
        for row in maintenance:
            state = {"running": "运行中", "ok": "等待下轮", "retry": "等待重试"}.get(
                row.get("status"), str(row.get("status", "未知")))
            if row.get("status") != "running" and row.get("next_attempt"):
                state += f" {max(0, int(row['next_attempt'] - time.time()))}s"
            summaries.append(f"{clean(str(row.get('plugin_id', '插件')))}: {state}")
        header.append("\n" + ("  |  ".join(summaries) or "后台状态等待更新"), style="dim")
        layout = Layout()
        small = width < 100 or height < 30
        header_height = 7 if width < 100 else 6
        limit = status.get("group_storage_limit_gib")
        capacity = ProgressBar(total=limit * 1024**3 if limit else None,
                               completed=used or 0,
                               complete_style="cyan", finished_style="red", animation_time=0)
        task_height = 5 if small else 8 if height >= 36 else 6
        task_size = task_height * 2
        layout.split_column(
            Layout(Panel(Group(header, capacity), border_style="blue"), size=header_height),
            Layout(name="tasks", size=task_size),
            Layout(name="events", minimum_size=3),
            Layout(name="footer", size=2),
        )
        categories = ("下载", "处理") if small else ("下载", "处理", "迁移", "发布")
        if small:
            if self.selected in {"迁移", "发布", "清理"}:
                categories = (self.selected, "处理")
            layout["tasks"].split_column(*[
                Layout(self.task_panel(c, task_height)) for c in categories
            ])
        else:
            layout["tasks"].split_column(Layout(name="top"), Layout(name="bottom"))
            layout["top"].split_row(*[
                Layout(self.task_panel(c, task_height)) for c in categories[:2]
            ])
            layout["bottom"].split_row(*[
                Layout(self.task_panel(c, task_height)) for c in categories[2:]
            ])
        table = Table.grid(expand=True, padding=(0, 1))
        table.add_column(width=8, min_width=8, max_width=8, style="dim")
        table.add_column(width=4)
        table.add_column(ratio=1, no_wrap=True, overflow="ellipsis")
        source = self.alerts if self.selected == "告警" else self.events
        events = [e for e in source if self.selected in {"全部", "告警", e.category}]
        available = max(1, height - header_height - task_size - 4)
        for event in events[-available:]:
            color = ("red" if event.level in {"ERROR", "CRITICAL"} else
                     "yellow" if event.level == "WARNING" else "white")
            table.add_row(Text(event.stamp), Text(event.category), Text(event.message, style=color))
        if not events:
            table.add_row("", "", "该分区暂无日志")
        layout["events"].update(Panel(table, title=f"日志 / {self.selected}", border_style="blue"))
        exit_hint = "Q 退出面板（机器人继续）" if attached else "Ctrl+C 停止机器人"
        footer = Text("0 全部  1 下载  2 处理  3 迁移  4 发布  5 清理  6 告警\n", style="dim")
        footer.append(f"P {'恢复刷新' if self.paused else '暂停画面'}  ·  {exit_hint}"
                      f"  ·  历史告警 {len(self.alerts)}", style="yellow" if self.paused else "dim")
        layout["footer"].update(footer)
        return layout


def poll(client: httpx.Client, settings: Settings, model: Model, path: str) -> str:
    try:
        response = client.get(f"http://127.0.0.1:{settings.port}/ptilopsisbot/{path}")
        if response.status_code == 404 and path == "overview":
            return poll(client, settings, model, "status")
        if response.status_code == 401:
            model.connection = "认证失败，请检查配置中的 access_token"
        else:
            response.raise_for_status()
            value = response.json()
            if not isinstance(value, dict):
                raise ValueError("Invalid status")
            model.accept(value)
    except (httpx.HTTPError, ValueError):
        model.connection = "无法连接机器人；保留上次状态，自动重连"
    return path


def read_key() -> str:
    if os.name == "nt" and sys.stdin.isatty():
        import msvcrt

        if msvcrt.kbhit():
            value = msvcrt.getwch()
            if value in {"\x00", "\xe0"}:
                msvcrt.getwch()
                return ""
            return value
    return ""


class StatusPoller:
    """One outstanding read; slow/offline HTTP never stalls terminal refresh."""

    def __init__(self, client: httpx.Client, settings: Settings) -> None:
        self.client, self.settings = client, settings
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dashboard-status")
        self.future: Future[tuple[str, Model]] | None = None
        self.path = "overview"
        self.next_poll = 0.0

    def read(self) -> tuple[str, Model]:
        snapshot = Model()
        return poll(self.client, self.settings, snapshot, self.path), snapshot

    def tick(self, model: Model) -> None:
        if self.future is not None and self.future.done():
            self.path, snapshot = self.future.result()
            model.connection = snapshot.connection
            if snapshot.updated != "—":
                model.status, model.updated = snapshot.status, snapshot.updated
            self.future = None
            self.next_poll = time.monotonic() + 3
        if self.future is None and time.monotonic() >= self.next_poll:
            self.future = self.pool.submit(self.read)

    def close(self) -> None:
        self.pool.shutdown(wait=True, cancel_futures=True)


def watch(settings: Settings, *, stop: threading.Event | None = None,
          once: bool = False) -> None:
    """GET local status and tail logs only. Ctrl+C/q never control the bot."""
    attached = stop is None
    stop = stop or threading.Event()
    model = Model()
    tail = LogTail(settings.data_dir / "collector.log")
    console = Console(legacy_windows=False)
    headers = {"Authorization": "Bearer " + settings.access_token.get_secret_value()}
    with httpx.Client(headers=headers, timeout=2, trust_env=False) as client:
        path = "overview"
        if once or not console.is_terminal:
            poll(client, settings, model, path)
            for line in tail.read():
                model.feed(line)
            console.print(model.render(console.width, max(24, console.height), attached=attached))
            return
        reader = StatusPoller(client, settings)
        with Live(console=console, screen=True, auto_refresh=False,
                  redirect_stdout=False, redirect_stderr=False) as live:
            try:
                while not stop.is_set():
                    for line in tail.read():
                        model.feed(line)
                    reader.tick(model)
                    key = read_key()
                    if attached and key.lower() == "q":
                        break
                    model.key(key)
                    if not model.paused:
                        live.update(model.render(console.width, console.height, attached=attached),
                                    refresh=True)
                    stop.wait(0.25)
            except KeyboardInterrupt:
                if not attached:
                    raise
            finally:
                reader.close()
