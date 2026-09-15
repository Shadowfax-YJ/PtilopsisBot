"""Turn measured legacy/new stage logs into bounded dashboard meters."""

import re
from collections import OrderedDict
from dataclasses import dataclass

from rich.progress_bar import ProgressBar
from rich.table import Table
from rich.text import Text


@dataclass
class Meter:
    label: str
    completed: float
    total: float | None
    unit: str
    detail: str = ""
    finished: bool = False

    def display(self, *, running: bool = True) -> Table:
        table = Table.grid(expand=True, padding=(0, 1))
        table.add_column(ratio=1)
        table.add_column(justify="right", no_wrap=True)
        number = (f"{self.completed:,.1f}" if self.unit == "MiB"
                  else f"{self.completed:,.0f}")
        if self.total is not None:
            ratio = min(100, 100 * self.completed / self.total) if self.total else 100
            if self.completed < self.total:
                ratio = min(99.9, ratio)
            total_text = f"{self.total:,.1f}" if self.unit == "MiB" else f"{self.total:,.0f}"
            counter = f"{number}/{total_text} {self.unit} · {ratio:.1f}%"
        else:
            counter = f"{number} {self.unit} · 总量未知"
        detail = self.detail or ("本批完成" if self.finished else
                                 "运行中" if running else "上次进度")
        if self.finished and self.unit == "MiB":
            detail = "完成 · " + detail
        table.add_row(Text(self.label, style="bold", no_wrap=True, overflow="ellipsis"),
                      Text(detail, style="dim", no_wrap=True, overflow="ellipsis"))
        table.add_row(ProgressBar(
            total=self.total, completed=self.completed,
            animation_time=None if running and not self.finished else 0,
            complete_style="cyan", finished_style="green", pulse_style="cyan",
        ), Text(counter, style="green" if self.finished else "cyan"))
        return table


class Meters:
    def __init__(self) -> None:
        self.rows: OrderedDict[tuple[str, str], Meter] = OrderedDict()
        self.queue_base = 0
        self.migration_base = 0

    def put(self, category: str, key: str, meter: Meter) -> None:
        self.rows[category, key] = meter
        self.rows.move_to_end((category, key))
        while len(self.rows) > 48:
            self.rows.popitem(last=False)

    def feed(self, message: str) -> None:
        # Transfer values use the measured MiB counters when available.
        transfer = re.search(
            r"(上传记录\s+(\d+)\s+下载归档|删源前远端校验\s+(\S+))\s+(.*)", message)
        if transfer:
            key = transfer[2] or transfer[3]
            category = "下载" if transfer[2] else "清理"
            label = "下载 #" + transfer[2] if transfer[2] else "删前校验 " + transfer[3]
            body = transfer[4]
            previous = self.rows.get((category, key))
            values = re.search(r"([\d.]+)/([\d.]+) MiB", body)
            speed = re.search(r"平均 ([\d.]+ MiB/s)", body)
            detail = speed[1] if speed else ""
            if values:
                self.put(category, key, Meter(
                    label, float(values[1]), float(values[2]), "MiB", detail))
            elif start := re.search(r"开始，共 ([\d.]+) MiB", body):
                self.put(category, key, Meter(label, 0, float(start[1]), "MiB"))
            elif percent := re.search(r"([\d.]+)%", body):
                self.put(category, key, Meter(label, float(percent[1]), 100, "%", detail))
            elif "完成" in body:
                self.put(category, key, Meter(label, previous.total or previous.completed,
                    previous.total, previous.unit, detail, True) if previous else
                    Meter(label, 0, None, "MiB", "已完成（未记录总量）", True))
            return
        if match := re.search(r"识别队列：已完成 (\d+) 条，待处理 (\d+) 条", message):
            done, remaining = map(int, match.groups())
            self.queue_base = done
            self.put("处理", "queue", Meter(
                "识别总队列", done, done + remaining, "条", finished=remaining == 0))
            if cache := self.rows.get(("处理", "cache")):
                cache.completed, cache.finished = cache.total or 0, True
        elif match := re.search(r"候选 (\d+) 条，本批已保存 (\d+) 条", message):
            total, done = map(int, match.groups())
            self.put("处理", "batch", Meter("本批识别", done, total, "条"))
            if queue := self.rows.get(("处理", "queue")):
                queue.completed = min(queue.total or 0, self.queue_base + done)
                queue.finished = queue.completed == queue.total
        elif match := re.search(r"本批 (\d+) 条图像任务、(\d+) 条日志任务", message):
            self.put("处理", "batch", Meter("本批识别", 0, sum(map(int, match.groups())), "条"))
        elif match := re.search(r"计算设备确认：.*本批完成 (\d+) 条", message):
            done = int(match[1])
            if batch := self.rows.get(("处理", "batch")):
                batch.completed, batch.finished = done, True
            if queue := self.rows.get(("处理", "queue")):
                queue.completed = min(queue.total or 0, self.queue_base + done)
                queue.finished = queue.completed == queue.total
        elif match := re.search(
            r"本批识别/整理 (\d+) 条，复用 (\d+) 条，失败 (\d+) 条，待续 (\d+) 条", message):
            processed, reused, errors, remaining = map(int, match.groups())
            done = reused + processed - errors
            self.queue_base = done
            self.put("处理", "queue", Meter("识别总队列", done, done + remaining + errors,
                                             "条", f"待续 {remaining + errors:,}",
                                             remaining + errors == 0))
            if batch := self.rows.get(("处理", "batch")):
                batch.completed, batch.finished = processed, True
                batch.detail = f"失败 {errors}" if errors else "本批完成"
        elif match := re.search(r"核对识别缓存：([\d,]+)/([\d,]+) 条", message):
            done, total = [int(v.replace(",", "")) for v in match.groups()]
            self.put("处理", "cache", Meter("缓存校验", done, total, "条"))
        elif match := re.search(r"核对识别缓存：批量定位 (\d+) 条", message):
            self.put("处理", "cache", Meter("缓存校验", 0, int(match[1]), "条"))
        elif match := re.search(r"迁移盘点：本次跟踪 (\d+) 个散文件，剩余 (\d+) 个", message):
            total, remaining = map(int, match.groups())
            self.migration_base = total - remaining
            self.put("迁移", "total", Meter(
                "散文件迁移（本次跟踪）", self.migration_base, total, "个"))
        elif match := re.search(r"(?:本批已迁移|本批迁移) (\d+) 个对象", message):
            done = int(match[1])
            finished = "中心存储整理" in message
            self.put("迁移", "batch", Meter(
                "本批迁移", done, None, "个", "本批结束" if finished else "", finished))
            if migration := self.rows.get(("迁移", "total")):
                migration.completed = min(migration.total or 0, self.migration_base + done)
                migration.finished = "散文件迁移完成" in message
        elif match := re.search(r"发布进度：([^，]+)，已完成 (\d+)/(\d+) (局|条|个)", message):
            label, completed_text, total_text, unit = match.groups()
            self.put("发布", "manifest", Meter(label, int(completed_text), int(total_text), unit))
        elif match := re.search(r"地图表进度：已保存 (\d+)/(\d+) 局", message):
            self.put("发布", "manifest", Meter("整理地图表", int(match[1]), int(match[2]), "局"))
        elif match := re.search(r"本卷已写入 ([\d.]+) MiB.*已完成 (\d+)/(\d+) 个对象", message):
            self.put("发布", "volume", Meter(
                "当前分卷", int(match[2]), int(match[3]), "个", match[1] + " MiB"))
        elif match := re.search(r"本批已校验 (\d+) 个对象，复用 (\d+) 个校验", message):
            checked, reused = map(int, match.groups())
            self.put("发布", "export", Meter(
                "本批校验 / 导出", checked + reused, None, "个", f"复用 {reused:,}"))
        elif match := re.search(r"([^：]+)：([\d,]+)/([\d,]+) (个对象|条|局|张|个)", message):
            label, completed_text, total_text, unit = match.groups()
            label = re.sub(r"^\[[^]]+\]\s*", "", label)[-30:]
            self.put("发布", label, Meter(label, int(completed_text.replace(",", "")),
                                        int(total_text.replace(",", "")), unit))

    def select(self, category: str, count: int) -> list[Meter]:
        rows = [(key, value) for key, value in self.rows.items() if key[0] == category]
        if category == "下载":
            rows += [(key, value) for key, value in self.rows.items() if key[0] == "清理"]
            active = [row for row in rows if not row[1].finished]
            rows = active or rows
        if not rows:
            return []
        chosen = rows[-count:]
        overall = self.rows.get((category, "queue" if category == "处理" else "total"))
        if count > 1 and overall is not None:
            chosen = [(('', ''), overall), rows[-1]] if rows[-1][1] is not overall else chosen
        return [value for _, value in chosen]
