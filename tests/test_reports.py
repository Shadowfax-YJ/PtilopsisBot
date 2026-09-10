import hashlib
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from ptilopsisbot.collector import SHANGHAI, Collector, Upload
from ptilopsisbot.config import Settings

DAY = date(2026, 9, 5)
START = datetime.combine(DAY, datetime.min.time(), SHANGHAI).timestamp()


def add_record(
    collector: Collector,
    file_id: str,
    *,
    uploaded_at: float = START,
    user: int = 456,
    nickname: str = "小张",
    size: int = 1024**2,
    content: str = "package-a",
    status: str = "archived",
) -> int:
    record_id = collector.register(
        Upload(
            collector.settings.group_id, file_id, 102, user,
            "run-20260905-120000-000001.zip", size, uploaded_at, nickname,
        )
    )
    assert record_id is not None
    with collector.db:
        collector.db.execute("UPDATE uploads SET status=? WHERE id=?", (status, record_id))
        if status == "archived":
            collector.db.execute(
                "UPDATE uploads SET sha256=?, collected_at=? WHERE id=?",
                (hashlib.sha256(content.encode()).hexdigest(), START + record_id, record_id),
            )
    return record_id


async def test_report_totals_stop_at_day_boundary_and_thank_all_daily_uploaders(
    tmp_path: Path,
) -> None:
    api = AsyncMock()
    collector = Collector(Settings(group_id=123, data_dir=tmp_path), api=api)
    mib = 1024**2
    try:
        add_record(
            collector, "yesterday", uploaded_at=START - 1, user=1, nickname="昨日成员",
            content="old", size=2 * mib,
        )
        # This member contributes only a duplicate today, and must still be thanked.
        add_record(collector, "duplicate-old", content="old", size=2 * mib, nickname="  小张\n")
        add_record(collector, "new-a", user=789, nickname="旧昵称", content="a", size=3 * mib)
        add_record(
            collector, "duplicate-a", user=789, nickname=" 小李\n改名 ", content="a", size=3 * mib,
        )
        add_record(collector, "new-b", user=789, nickname="", content="b", size=4 * mib)
        for user, status in enumerate(("queued", "invalid", "failed"), start=900):
            add_record(collector, status, user=user, nickname=status, status=status)
        add_record(
            collector, "tomorrow", uploaded_at=START + 86400, user=1000, nickname="明日成员",
            content="future", size=5 * mib,
        )
        text = await collector.send_report(DAY)
        assert text is not None
        assert "数据总量：3 个包，共 9.0 MiB。" in text
        assert "当日新增：2 个包，共 7.0 MiB。" in text
        assert "感谢当日上传数据的 MAA 训练家：小张、小李 改名。" in text
        assert text.count("小李 改名") == 1
        assert "昨日成员" not in text
        assert "明日成员" not in text
        for status in ("queued", "invalid", "failed"):
            assert status not in text
        api.send_message.assert_awaited_once_with(text)
        assert collector.report(DAY - timedelta(days=2)) is None
    finally:
        collector.close()


@pytest.mark.parametrize("case", ["empty", "queued", "invalid", "failed", "duplicate_only"])
async def test_no_new_content_never_sends_a_report(tmp_path: Path, case: str) -> None:
    api = AsyncMock()
    collector = Collector(Settings(group_id=123, data_dir=tmp_path), api=api)
    try:
        if case == "duplicate_only":
            add_record(collector, "old", uploaded_at=START - 1)
            add_record(collector, "same-content-new-upload", user=789, nickname="小李")
        elif case != "empty":
            add_record(collector, "not-collected", status=case)
        assert collector.report(DAY) is None
        assert await collector.send_report(DAY) is None
        api.send_message.assert_not_awaited()
        collector.api = None  # Silence does not require a live connection.
        assert await collector.send_report(DAY) is None
    finally:
        collector.close()


def test_reports_keep_first_collection_credit_after_restart_and_ignore_other_groups(
    tmp_path: Path,
) -> None:
    settings = Settings(group_id=123, data_dir=tmp_path)
    collector = Collector(settings)
    add_record(collector, "first", nickname="", uploaded_at=START, content="first")
    add_record(collector, "repeat-tomorrow", uploaded_at=START + 86400, content="first")
    collector.close()
    other = Collector(settings.model_copy(update={"group_id": 999}))
    add_record(other, "other-group", nickname="其他群", content="other")
    other.close()
    collector = Collector(settings)
    try:
        text = collector.report(DAY)
        assert text is not None
        assert "数据总量：1 个包，共 1.0 MiB。" in text
        assert "当日新增：1 个包，共 1.0 MiB。" in text
        assert "其他群" not in text
        assert collector.report(DAY + timedelta(days=1)) is None
    finally:
        collector.close()
