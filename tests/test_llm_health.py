import unittest

import pymysql
from botocore.exceptions import ClientError

from boxer_company_adapter_slack.company import _build_dependency_failure_reply, _format_ping_llm_status
from boxer.core.llm import _check_claude_health


class _FakeMessages:
    def __init__(self, *, error: Exception | None = None) -> None:
        self._error = error

    def create(self, **_: object) -> object:
        if self._error is not None:
            raise self._error
        return object()


class _FakeClaudeClient:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.messages = _FakeMessages(error=error)


class PingStatusTests(unittest.TestCase):
    def test_formats_ping_llm_status(self) -> None:
        self.assertEqual(_format_ping_llm_status(True), "가능")
        self.assertEqual(_format_ping_llm_status(False), "불가")
        self.assertEqual(_format_ping_llm_status(None), "미설정")


class ClaudeHealthTests(unittest.TestCase):
    def test_reports_ok_for_successful_claude_call(self) -> None:
        result = _check_claude_health(_FakeClaudeClient())

        self.assertTrue(result["ok"])
        self.assertIn("정상", str(result["summary"]))

    def test_reports_generic_error_for_unexpected_failure(self) -> None:
        result = _check_claude_health(_FakeClaudeClient(error=RuntimeError("boom")))

        self.assertFalse(result["ok"])
        self.assertIn("응답 오류", str(result["summary"]))


class DependencyFailureReplyTests(unittest.TestCase):
    def test_maps_db_errors_to_db_reply(self) -> None:
        message = _build_dependency_failure_reply("바코드 로그 분석", pymysql.MySQLError("db down"))

        self.assertEqual(
            message,
            "바코드 로그 분석 중 오류가 발생했어. DB 연결 또는 조회에 실패했어",
        )

    def test_maps_s3_access_denied_to_permission_reply(self) -> None:
        error = ClientError({"Error": {"Code": "AccessDenied"}}, "HeadObject")
        message = _build_dependency_failure_reply("바코드 로그 분석", error)

        self.assertEqual(
            message,
            "바코드 로그 분석 중 오류가 발생했어. S3 접근 권한을 확인해줘",
        )


if __name__ == "__main__":
    unittest.main()
