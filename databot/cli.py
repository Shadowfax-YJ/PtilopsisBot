import argparse
import json
import sys
from datetime import date
from pathlib import Path

import httpx

from .collector import Collector
from .config import load_settings


def main() -> None:
    parser = argparse.ArgumentParser(description="单群对局包收集与群文件清理")
    parser.add_argument("--config", type=Path, default=Path("config.toml"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run", help="运行机器人")
    commands.add_parser("status", help="查看连接和最近 50 条上传记录")
    commands.add_parser("scan", help="请求补扫并检查群文件清理条件")
    retry = commands.add_parser("retry", help="重试指定上传记录")
    retry.add_argument("record_id", type=int)
    report = commands.add_parser("report", help="发送指定日期日报")
    report.add_argument("day", type=date.fromisoformat)
    report.add_argument("--preview", action="store_true", help="仅本地预览，无需 NapCat 连接")
    args = parser.parse_args()
    try:
        settings = load_settings(args.config)
        if args.command == "run":
            from .runtime import run

            run(settings)
            return
        if args.command == "report" and args.preview:
            collector = Collector(settings)
            try:
                print(collector.report(args.day))
            finally:
                collector.close()
            return
        path = args.command
        if args.command == "retry":
            path += f"/{args.record_id}"
        elif args.command == "report":
            path += f"/{args.day}"
        method = "GET" if args.command == "status" else "POST"
        with httpx.Client(timeout=600, trust_env=False) as client:
            response = client.request(
                method,
                f"http://127.0.0.1:{settings.port}/databot/{path}",
                headers={"Authorization": "Bearer " + settings.access_token.get_secret_value()},
            )
        if response.is_error:
            print(response.text, file=sys.stderr)
            raise SystemExit(1)
        print(json.dumps(response.json(), ensure_ascii=False, indent=2))
    except httpx.ConnectError:
        parser.exit(1, "机器人未运行，请先执行 databot run\n")
    except (OSError, ValueError) as exc:
        parser.exit(1, f"配置或文件错误: {exc}\n")


if __name__ == "__main__":
    main()
