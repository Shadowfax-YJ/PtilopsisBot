"""Group reply templates; character voice guidance lives in README.md."""

import random
from datetime import date

RECEIPT_TEMPLATES = (
    "接收确认。对局包已通过 ZIP 可读性检查，并保存至本地。",
    "文件处理完成。本地归档已建立，请查阅收集记录。",
    "白面鸮已保存这份对局包。相关数据已登记。",
    "本次收集完成。文件已归档，感谢您提供的数据。",
    "归档结果已确认。这份对局包现已保存在本地。",
    "数据读取完毕。对局包已保存，收集记录已更新。",
    "文件校验与保存已完成。白面鸮将继续处理后续数据。",
    "接收流程已完成。本地副本已保存，请放心。",
)

REPORT_TEMPLATES = (
    "白面鸮已完成本次数据汇总。请查阅。",
    "统计结果已生成。以下为当日对局包的收集记录。",
    "数据检索完毕。白面鸮已整理当日收集情况。",
    "日报整理完成。上传记录与处理状态如下。",
    "当日数据已汇总。请查阅各成员的收集记录。",
    "白面鸮已生成本次统计报告。感谢各位提供的数据。",
    "统计流程已完成。以下为当日上传与归档的汇总结果。",
    "本次记录检索已完成。白面鸮将为您列出统计结果。",
)

CHECK_SCOPE = "校验范围：仅检查 ZIP 可读性，未验证游戏内容。"


def receipt(name: str, uploader_id: int, nickname: str) -> str:
    nickname = " ".join(nickname.split()) or str(uploader_id)
    return "\n".join(
        [
            random.choice(RECEIPT_TEMPLATES),
            f"上传者：{nickname}（{uploader_id}）。",
            f"文件：{name}",
            CHECK_SCOPE,
        ]
    )


def report(day: date, contributions: list[str], *, pending: int, invalid: int, failed: int) -> str:
    return "\n".join(
        [
            random.choice(REPORT_TEMPLATES),
            f"统计日期：{day}。",
            *(contributions or ["检索结果：当日未发现已登记的对局包。"]),
            f"处理状态：待处理 {pending}，检查不通过 {invalid}，处理失败 {failed}。",
            CHECK_SCOPE,
        ]
    )
