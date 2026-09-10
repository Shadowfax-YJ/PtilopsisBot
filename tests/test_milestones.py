import asyncio
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
from test_collector import FakeGroup, zip_bytes
from test_reports import add_record

from ptilopsisbot.collector import Collector, Upload
from ptilopsisbot.config import Settings
from ptilopsisbot.milestones import Milestones


def settings(tmp_path: Path, interval: int | None = 2) -> Settings:
    return Settings(
        group_id=123, data_dir=tmp_path, milestone_interval=interval,
        send_receipts=False, min_free_gib=0,
    )


async def test_milestones_count_unique_archived_content_and_survive_restart(tmp_path: Path) -> None:
    config = settings(tmp_path)
    api = AsyncMock(online=True)
    collector = Collector(config)
    try:
        milestones = Milestones(config, collector.db, api=api)
        milestones.initialize()
        first = add_record(collector, "first", content="a")
        add_record(collector, "same-a", content="a")
        for status in ("queued", "failed", "invalid"):
            add_record(collector, status, content=status, status=status)
        assert milestones.total_count() == 1 and not await milestones.check()
        add_record(collector, "second", content="b")
        assert await milestones.check()
        api.send_message.assert_awaited_once_with(
            "白面鸮确认：对局数据累计收录已达 2 份。\n感谢各位 MAA 训练家提供数据。辛苦了。"
        )
        with collector.db:
            collector.db.execute("UPDATE uploads SET deleted_at=1 WHERE id=?", (first,))
        add_record(collector, "reupload", content="b")
        assert not await milestones.check()
        collector.close()
        collector = Collector(config)
        milestones = Milestones(config, collector.db, api=api)
        milestones.initialize()
        assert not await milestones.check()
        assert milestones.status()["next_threshold"] == 4
        assert milestones.status()["latest_announcement"]["status"] == "sent"
        for content in ("c", "d"):
            add_record(collector, content, content=content)
        assert await milestones.check()
        assert api.send_message.await_count == 2
        assert "累计收录已达 4 份" in api.send_message.call_args.args[0]
    finally:
        collector.close()


async def test_enablement_baseline_is_not_reset_if_threshold_crossed_before_restart(
    tmp_path: Path,
) -> None:
    config = settings(tmp_path)
    api = AsyncMock(online=True)
    collector = Collector(config)
    try:
        for number in range(3):
            add_record(collector, str(number), content=str(number))
        milestones = Milestones(config, collector.db, api=api)
        milestones.initialize(baseline_count=3)
        assert not await milestones.check()  # Do not announce the old 2-file milestone.
        add_record(collector, "next", content="next")
        collector.close()
        collector = Collector(config)
        milestones = Milestones(config, collector.db, api=api)
        milestones.initialize()
        assert milestones.status()["next_threshold"] == 4
        assert await milestones.check()
        assert "累计收录已达 4 份" in api.send_message.call_args.args[0]
    finally:
        collector.close()


async def test_offline_catches_up_with_only_highest_threshold(tmp_path: Path) -> None:
    config = settings(tmp_path)
    api = AsyncMock(online=False)
    collector = Collector(config)
    try:
        milestones = Milestones(config, collector.db, api=api)
        milestones.initialize()
        for number in range(7):
            add_record(collector, str(number), content=str(number))
        assert not await milestones.check()
        assert milestones.status()["latest_announcement"] is None
        api.online = True
        assert await milestones.check()
        assert "累计收录已达 6 份" in api.send_message.call_args.args[0]
        assert not await milestones.check()
        assert api.send_message.await_count == 1
        assert milestones.status()["next_threshold"] == 8
    finally:
        collector.close()


async def test_disabled_milestones_do_not_send_or_create_group_baseline(tmp_path: Path) -> None:
    config = settings(tmp_path, None)
    api = AsyncMock(online=True)
    collector = Collector(config)
    try:
        milestones = Milestones(config, collector.db, api=api)
        milestones.initialize()
        add_record(collector, "first")
        assert not await milestones.check()
        api.send_message.assert_not_awaited()
        assert milestones.status()["next_threshold"] is None
        assert collector.db.execute("SELECT COUNT(*) FROM milestone_state").fetchone()[0] == 0
    finally:
        collector.close()


async def test_concurrent_checks_claim_the_threshold_only_once(tmp_path: Path) -> None:
    config = settings(tmp_path, 1)
    api = AsyncMock(online=True)
    entered, release = asyncio.Event(), asyncio.Event()

    async def send(text: str) -> None:
        entered.set()
        await release.wait()

    api.send_message.side_effect = send
    collector = Collector(config)
    tasks = []
    try:
        first = Milestones(config, collector.db, api=api)
        second = Milestones(config, collector.db, api=api)
        first.initialize()
        second.initialize()
        add_record(collector, "first")
        tasks.append(asyncio.create_task(first.check()))
        await asyncio.wait_for(entered.wait(), 2)
        tasks.append(asyncio.create_task(first.check()))
        assert not await second.check()  # Persistent claim works across instances, too.
        release.set()
        assert await asyncio.gather(*tasks) == [True, False]
        api.send_message.assert_awaited_once()
    finally:
        release.set()
        await asyncio.gather(*tasks, return_exceptions=True)
        collector.close()


@pytest.mark.parametrize("outcome", ["timeout", "cancelled", "lost_commit"])
async def test_uncertain_delivery_does_not_repeat_after_restart(
    tmp_path: Path, outcome: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = settings(tmp_path, 1)
    api = AsyncMock(online=True)
    collector = Collector(config)
    try:
        milestones = Milestones(config, collector.db, api=api)
        milestones.initialize()
        add_record(collector, "first")
        if outcome == "timeout":
            api.send_message.side_effect = TimeoutError("https://secret.invalid/?token=hidden")
            assert not await milestones.check()
        elif outcome == "cancelled":
            api.send_message.side_effect = asyncio.CancelledError()
            with pytest.raises(asyncio.CancelledError):
                await milestones.check()
        else:
            def fail_commit(*args: object) -> None:
                raise OSError("database write unavailable after send")

            monkeypatch.setattr(milestones, "_finish", fail_commit)
            with pytest.raises(OSError):
                await milestones.check()
        collector.close()
        collector = Collector(config)
        milestones = Milestones(config, collector.db, api=api)
        milestones.initialize()
        state = milestones.status()
        assert state["latest_announcement"]["status"] == "uncertain"
        assert "hidden" not in state["latest_announcement"]["error"]
        assert not await milestones.check()
        assert api.send_message.await_count == 1
        # A failed milestone must not prevent future milestones.
        api.send_message.side_effect = None
        add_record(collector, "second", content="other")
        assert await milestones.check()
        assert api.send_message.await_count == 2
    finally:
        collector.close()


async def test_slow_announcement_does_not_block_collection_when_receipts_disabled(
    tmp_path: Path,
) -> None:
    entered, release = asyncio.Event(), asyncio.Event()

    class SlowGroup(FakeGroup):
        async def send_message(self, text: str) -> None:
            entered.set()
            await release.wait()
            self.messages.append(text)

    group = SlowGroup()
    body = zip_bytes()
    config = settings(tmp_path, 1)
    async with httpx.AsyncClient(transport=httpx.MockTransport(
        lambda request: httpx.Response(200, content=body)
    )) as http:
        collector = Collector(config, api=group, http=http)
        milestones = Milestones(config, collector.db, api=group)
        milestones.initialize()
        add_record(collector, "already-archived")
        task = asyncio.create_task(milestones.check())
        try:
            await asyncio.wait_for(entered.wait(), 2)
            record_id = collector.register(Upload(
                123, "file-a", 102, 456, "run-20260905-120000-000002.zip", len(body), 1788580800,
            ))
            assert record_id is not None
            assert await asyncio.wait_for(collector.process_once(), 2)
            assert collector.record(record_id)["status"] == "archived"
            assert not task.done()
            release.set()
            assert await task
            assert len(group.messages) == 1
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)
            collector.close()


async def test_group_isolation_and_interval_change_preserve_progress(tmp_path: Path) -> None:
    config = settings(tmp_path, 2)
    api = AsyncMock(online=True)
    collector = Collector(config)
    try:
        milestones = Milestones(config, collector.db, api=api)
        milestones.initialize()
        add_record(collector, "first", content="a")
        add_record(collector, "second", content="b")
        assert await milestones.check()
        other_config = config.model_copy(update={"group_id": 999})
        other_collector = Collector(other_config)
        try:
            other = Milestones(other_config, other_collector.db, api=api)
            other.initialize()
            add_record(other_collector, "other", content="a")
            assert other.total_count() == 1 and not await other.check()
        finally:
            other_collector.close()
        milestones.settings = config.model_copy(update={"milestone_interval": 1})
        milestones.initialize()
        assert not await milestones.check()
        add_record(collector, "third", content="c")
        assert await milestones.check()
        assert api.send_message.await_count == 2
    finally:
        collector.close()


@pytest.mark.parametrize("value", [0, -1, 1.5])
def test_invalid_interval_rejected(value: int | float) -> None:
    with pytest.raises(ValueError):
        Settings(group_id=123, milestone_interval=value)
