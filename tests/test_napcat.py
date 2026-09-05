import pytest

from databot.napcat import NapCat, upload_from_notice


class RPC:
    async def call_api(self, api: str, **data: object) -> object:
        assert api == "delete_group_file"
        return {"result": 1, "errMsg": "permission denied"}


async def test_delete_checks_napcat_result_inside_successful_onebot_response() -> None:
    group = NapCat(123)
    group.bot = RPC()
    with pytest.raises(RuntimeError, match="permission denied"):
        await group.delete_file("source-a", 102)


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
