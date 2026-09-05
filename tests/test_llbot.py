import pytest
from nonebot.adapters.onebot.v11 import ActionFailed

from databot.collector import OfflineError
from databot.llbot import LLBot, upload_from_notice


class RPC:
    online = True

    async def call_api(self, api: str, **data: object) -> object:
        if api == "get_status":
            return {"online": self.online, "good": True}
        assert api == "delete_group_file"
        raise ActionFailed(message="permission denied", retcode=1200)


async def test_delete_failure_is_not_reported_as_success() -> None:
    group = LLBot(123)
    group.bot = RPC()
    with pytest.raises(ActionFailed):
        await group.delete_file("source-a", 102)


async def test_qq_offline_with_live_websocket_pauses_operations() -> None:
    group = LLBot(123)
    rpc = RPC()
    group.bot = rpc
    assert await group.refresh_status()
    rpc.online = False
    with pytest.raises(OfflineError):
        await group.file_url("source-a", 102)
    assert not group.online
    rpc.online = True
    assert await group.refresh_status()


def test_incomplete_notice_is_ignored_for_later_scan() -> None:
    assert upload_from_notice({"group_id": 123, "file": {"name": "run.zip"}}) is None


def test_null_file_identity_is_ignored() -> None:
    notice = {
        "group_id": 123,
        "user_id": 456,
        "time": 1788580800,
        "file": {"id": None, "busid": 102, "size": 100, "name": "run-20260905-120000-000001.zip"},
    }
    assert upload_from_notice(notice) is None
