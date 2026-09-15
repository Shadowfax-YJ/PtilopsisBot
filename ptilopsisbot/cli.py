import argparse
import json
import sys
import threading
from datetime import date
from pathlib import Path

import httpx

from .collector import Collector
from .config import load_settings


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="ptilopsisbot", description="PtilopsisBot · 单群对局包收集与群文件清理"
    )
    parser.add_argument("--config", type=Path, default=Path("config.toml"))
    commands = parser.add_subparsers(dest="command", required=True)
    run_parser = commands.add_parser("run", help="运行机器人，交互终端默认显示 TUI")
    display = run_parser.add_mutually_exclusive_group()
    display.add_argument("--tui", dest="tui", action="store_true", help="显示运行面板")
    display.add_argument("--plain", dest="tui", action="store_false", help="使用普通日志")
    run_parser.set_defaults(tui=None)
    dashboard = commands.add_parser("dashboard", help="只读查看现有机器人，不启动机器人")
    dashboard.add_argument("--once", action="store_true", help="打印一次面板后退出")
    commands.add_parser("status", help="查看连接和最近 50 条上传记录")
    commands.add_parser("scan", help="请求补扫并检查群文件清理条件")
    retry = commands.add_parser("retry", help="重试指定上传记录")
    retry.add_argument("record_id", type=int)
    plugin_retry = commands.add_parser("plugin-retry", help="仅重试指定记录的后处理插件")
    plugin_retry.add_argument("record_id", type=int)
    report = commands.add_parser("report", help="发送指定日期日报")
    report.add_argument("day", type=date.fromisoformat)
    report.add_argument("--preview", action="store_true", help="仅本地预览，无需 NapCat 连接")
    args = parser.parse_args()
    try:
        settings = load_settings(args.config)
        if args.command == "dashboard":
            from .dashboard import watch

            watch(settings, once=args.once)
            return
        if args.command == "run":
            from .runtime import run

            interactive = sys.stdout.isatty() and sys.stdin.isatty()
            tui = interactive if args.tui is None else args.tui and interactive
            if not tui:
                run(settings)
                return
            from .dashboard import watch

            stop = threading.Event()
            monitor = threading.Thread(target=watch, kwargs={"settings": settings, "stop": stop},
                                       name="terminal-dashboard", daemon=True)
            monitor.start()
            try:
                run(settings, console=False)
            finally:
                stop.set()
                monitor.join(timeout=5)
            return
        if args.command == "report" and args.preview:
            collector = Collector(settings)
            try:
                print(collector.report(args.day) or "当日无新增唯一包，不发送日报。")
            finally:
                collector.close()
            return
        path = args.command
        if args.command in {"retry", "plugin-retry"}:
            path += f"/{args.record_id}"
        elif args.command == "report":
            path += f"/{args.day}"
        method = "GET" if args.command == "status" else "POST"
        with httpx.Client(timeout=600, trust_env=False) as client:
            response = client.request(
                method,
                f"http://127.0.0.1:{settings.port}/ptilopsisbot/{path}",
                headers={"Authorization": "Bearer " + settings.access_token.get_secret_value()},
            )
        if response.is_error:
            print(response.text, file=sys.stderr)
            raise SystemExit(1)
        print(json.dumps(response.json(), ensure_ascii=False, indent=2))
    except httpx.ConnectError:
        parser.exit(1, "机器人未运行，请先执行 ptilopsisbot run\n")
    except (OSError, ValueError) as exc:
        parser.exit(1, f"配置或文件错误: {exc}\n")


if __name__ == "__main__":
    main()
