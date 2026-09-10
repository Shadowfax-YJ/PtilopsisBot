"""Group reply templates; character voice guidance lives in README.md."""

import random
from datetime import date

RECEIPT_TEMPLATES = (
    "对局包已保存。感谢您提供数据。",
    "数据归档完成。感谢您的协助。",
    "白面鸮已保存这份数据，谢谢您。",
    "数据接收完成，已归档。感谢您的协助。",
    "这份对局数据已保存。辛苦了。",
    "这份数据已收录，感谢您的协助。",
    "白面鸮已完成数据归档。谢谢您。",
    "数据保存完毕，谢谢您。",
)

REPORT_TEMPLATES = (
    "白面鸮已完成本次数据汇总。",
    "数据整理完成。请查阅统计结果。",
    "数据汇总完成。",
    "本次收集记录已汇总。",
    "归档数据统计完成。结果如下。",
    "数据统计完成。结果如下。",
    "收集情况已汇总，请查阅。",
    "白面鸮已完成汇总。统计结果如下。",
)


def receipt() -> str:
    return random.choice(RECEIPT_TEMPLATES)


def milestone(count: int) -> str:
    return (
        f"白面鸮确认：对局数据累计收录已达 {count:,} 份。\n"
        "感谢各位 MAA 训练家提供数据。辛苦了。"
    )


def data_size(size: int) -> str:
    for unit, divisor in (("GiB", 1024**3), ("MiB", 1024**2), ("KiB", 1024)):
        if size >= divisor:
            return f"{size / divisor:.1f} {unit}"
    return f"{size} B"


def report(
    day: date,
    contributors: list[str],
    *,
    total_count: int,
    total_size: int,
    new_count: int,
    new_size: int,
) -> str:
    return "\n".join(
        [
            random.choice(REPORT_TEMPLATES),
            f"统计日期：{day}。",
            f"数据总量：{total_count} 个包，共 {data_size(total_size)}。",
            f"当日新增：{new_count} 个包，共 {data_size(new_size)}。",
            f"感谢当日上传数据的 MAA 训练家：{'、'.join(contributors)}。",
        ]
    )
