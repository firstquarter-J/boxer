import unittest
from unittest.mock import patch

import pymysql
from botocore.exceptions import ClientError

from boxer_company_adapter_slack.company import _build_dependency_failure_reply, _format_ping_llm_status
from boxer.core.llm import _build_claude_client, _check_claude_health, _resolve_anthropic_auth_token


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
    def test_builds_oauth_claude_client_without_api_key_header(self) -> None:
        with (
            patch("boxer.core.llm.s.ANTHROPIC_API_KEY", "sk-ant-low-credit"),
            patch("boxer.core.llm.s.ANTHROPIC_AUTH_TOKEN", "oauth-token"),
            patch("boxer.core.llm.s.ANTHROPIC_AUTH_TOKEN_COMMAND", ""),
        ):
            client = _build_claude_client(timeout_sec=1)

        self.assertEqual(client.auth_headers, {"Authorization": "Bearer oauth-token"})

    def test_oauth_token_command_empty_output_is_explicit_error(self) -> None:
        # Helper command가 설정됐는데 빈 토큰이면 API key fallback보다 원인을 바로 드러낸다.
        completed = type("Completed", (), {"returncode": 0, "stdout": "\n", "stderr": ""})()
        with (
            patch("boxer.core.llm.s.ANTHROPIC_AUTH_TOKEN", ""),
            patch("boxer.core.llm.s.ANTHROPIC_AUTH_TOKEN_COMMAND", "helper"),
            patch("boxer.core.llm.subprocess.run", return_value=completed),
        ):
            with self.assertRaisesRegex(RuntimeError, "empty token"):
                _resolve_anthropic_auth_token()

    def test_oauth_token_command_failure_does_not_leak_output(self) -> None:
        completed = type("Completed", (), {"returncode": 1, "stdout": "secret-token", "stderr": "secret-error"})()
        with (
            patch("boxer.core.llm.s.ANTHROPIC_AUTH_TOKEN", ""),
            patch("boxer.core.llm.s.ANTHROPIC_AUTH_TOKEN_COMMAND", "helper"),
            patch("boxer.core.llm.subprocess.run", return_value=completed),
        ):
            with self.assertRaises(RuntimeError) as context:
                _resolve_anthropic_auth_token()

        self.assertIn("exit code 1", str(context.exception))
        self.assertNotIn("secret-token", str(context.exception))
        self.assertNotIn("secret-error", str(context.exception))

    def test_health_check_skips_second_refresh_for_new_client(self) -> None:
        fake_client = _FakeClaudeClient()
        with (
            patch("boxer.core.llm._build_claude_client", return_value=fake_client),
            patch("boxer.core.llm._refresh_claude_oauth_token") as refresh_mock,
        ):
            result = _check_claude_health()

        self.assertTrue(result["ok"])
        refresh_mock.assert_not_called()

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
