from __future__ import annotations

import logging
from unittest.mock import Mock, patch

import pytest
from slack_sdk.errors import SlackApiError
from slack_sdk.web.slack_response import SlackResponse

from boxer_company_adapter_slack import daily_device_round_reporter as daily

_REQUEST_ID = "0123456789abcdef0123456789abcdef"
_PRIVATE_TEXT = "private-response-body-canary"
_TOKEN = "xoxb-private-token-canary"


def _slack_error(
    *,
    error_code: object = "invalid_blocks",
    status_code: object = 200,
    headers: object = None,
) -> SlackApiError:
    # 실제 SDK 예외의 문자열에는 전체 응답이 들어가므로 logger가 예외를
    # 문자열화하거나 응답 원문을 추가해 버리는 회귀도 함께 검증한다.
    response = SlackResponse(
        client=Mock(),
        http_verb="POST",
        api_url="https://slack.com/api/chat.postMessage",
        req_args={"headers": {"Authorization": f"Bearer {_TOKEN}"}},
        data={
            "ok": False,
            "error": error_code,
            "text": _PRIVATE_TEXT,
            "response_metadata": {"messages": [_PRIVATE_TEXT, _TOKEN]},
        },
        headers=(
            {
                "x-slack-req-id": _REQUEST_ID,
                "authorization": f"Bearer {_TOKEN}",
                "private-header": _PRIVATE_TEXT,
            }
            if headers is None
            else headers
        ),
        status_code=status_code,
    )
    with pytest.raises(SlackApiError) as caught:
        response.validate()
    return caught.value


def _run_failed_poll(exc: Exception, caplog: pytest.LogCaptureFixture) -> str:
    logger = logging.getLogger("test.daily.slack.diagnostics")
    client = Mock()
    api = Mock()
    # wait는 loop의 예외 처리 밖에 있어서 실제 대기·추가 poll 없이
    # 한 번의 실패 로그만 확인하고 테스트를 종료한다.
    with (
        caplog.at_level(logging.WARNING, logger=logger.name),
        patch.object(daily, "_run_daily_device_round_if_due", side_effect=exc) as poll,
        patch.object(daily.threading, "Event") as event,
    ):
        event.return_value.wait.side_effect = StopIteration
        with pytest.raises(StopIteration):
            daily._daily_device_round_loop(client, logger, api)
    poll.assert_called_once()
    client.chat_postMessage.assert_not_called()
    assert len(caplog.records) == 1
    assert not caplog.records[0].exc_info
    assert _PRIVATE_TEXT not in caplog.text
    assert _TOKEN not in caplog.text
    assert "response_metadata" not in caplog.text
    assert "authorization" not in caplog.text.lower()
    return caplog.records[0].getMessage()


def test_slack_error_logs_only_validated_metadata(
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = _run_failed_poll(_slack_error(), caplog)
    assert message == (
        "Daily device round transport failed error_type=SlackApiError "
        "slack_error_code=invalid_blocks status_code=200 "
        f"slack_request_id={_REQUEST_ID}"
    )


@pytest.mark.parametrize(
    "error_code",
    ["msg_blocks_too_long", "ratelimited", "restricted_action_thread_locked"],
)
def test_common_slack_rejections_remain_distinguishable(
    error_code: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = _run_failed_poll(_slack_error(error_code=error_code), caplog)
    assert f"slack_error_code={error_code} status_code=200" in message


@pytest.mark.parametrize(
    "headers,expected_request_id",
    [
        ({"X-Slack-Req-Id": _REQUEST_ID}, _REQUEST_ID),
        (
            {"x-slack-req-id": "01234567-89ab-cdef-0123-456789abcdef"},
            "01234567-89ab-cdef-0123-456789abcdef",
        ),
        ({}, "none"),
        ({"x-slack-req-id": _TOKEN}, "none"),
        ({"x-slack-req-id": _REQUEST_ID + "\n" + _TOKEN}, "none"),
        ({"x-slack-req-id": "a" * 129}, "none"),
        ({"x-slack-req-id": {_TOKEN: _PRIVATE_TEXT}}, "none"),
        ({"x-slack-req-id": 123}, "none"),
        ({"x-slack-req-id": _REQUEST_ID, "X-Slack-Req-Id": _TOKEN}, "none"),
        ([{"x-slack-req-id": _REQUEST_ID}], "none"),
    ],
)
def test_request_id_rejects_ambiguous_or_unstructured_headers(
    headers: object,
    expected_request_id: str,
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = _run_failed_poll(_slack_error(headers=headers), caplog)
    assert message.endswith(f"slack_request_id={expected_request_id}")
    assert "a" * 129 not in message


@pytest.mark.parametrize(
    "error_code",
    [
        None,
        True,
        123,
        {"error": _TOKEN},
        _TOKEN,
        f"Bearer {_TOKEN}",
        "invalid_blocks\n" + _PRIVATE_TEXT,
        "invalid_blocks\r" + _PRIVATE_TEXT,
        "a" * 65,
        "INVALID_BLOCKS",
        "invalid_blocks ",
        "secret_value",
        "private_response_body_canary",
    ],
)
def test_error_code_rejects_sensitive_multiline_and_oversized_values(
    error_code: object,
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = _run_failed_poll(_slack_error(error_code=error_code), caplog)
    assert "slack_error_code=unknown status_code=200" in message
    assert "a" * 65 not in message
    assert "\n" not in message
    assert "\r" not in message
    assert "secret_value" not in message
    assert "private_response_body_canary" not in message


@pytest.mark.parametrize("status_code", [None, True, "200", 99, 600, -1, _TOKEN])
def test_http_status_requires_an_integer_in_http_range(
    status_code: object,
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = _run_failed_poll(_slack_error(status_code=status_code), caplog)
    assert "status_code=none" in message


@pytest.mark.parametrize("data", [None, _PRIVATE_TEXT, [_TOKEN], b"private-body"])
def test_slack_response_with_unstructured_data_keeps_safe_metadata(
    data: object,
    caplog: pytest.LogCaptureFixture,
) -> None:
    exc = _slack_error()
    exc.response.data = data
    message = _run_failed_poll(exc, caplog)
    assert "slack_error_code=unknown status_code=200" in message
    assert message.endswith(f"slack_request_id={_REQUEST_ID}")


def test_unrecognized_slack_error_response_does_not_read_arbitrary_fields(
    caplog: pytest.LogCaptureFixture,
) -> None:
    response = Mock()
    exc = SlackApiError(_PRIVATE_TEXT, response=response)
    message = _run_failed_poll(exc, caplog)
    assert message == (
        "Daily device round transport failed error_type=SlackApiError "
        "slack_error_code=unknown status_code=none slack_request_id=none"
    )
    assert response.mock_calls == []


def test_diagnostic_extraction_failure_keeps_the_loop_alive(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _BrokenData(dict):
        def get(self, key: object, default: object = None) -> object:
            raise RuntimeError(_PRIVATE_TEXT + _TOKEN)

    # SDK 응답의 비정상 accessor도 원문 없이 fallback하고 다음 wait까지 간다.
    exc = _slack_error()
    exc.response.data = _BrokenData()
    message = _run_failed_poll(exc, caplog)
    assert message == (
        "Daily device round transport failed error_type=SlackApiError "
        "slack_error_code=unknown status_code=none slack_request_id=none"
    )


def test_diagnostic_values_are_not_coerced_to_strings(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class _PrivateValue:
        def __str__(self) -> str:
            raise AssertionError("private value must not be stringified")

        def __repr__(self) -> str:
            raise AssertionError("private value must not be represented")

    exc = _slack_error()
    private_value = _PrivateValue()
    exc.response.data["error"] = private_value
    exc.response.status_code = private_value
    exc.response.headers["x-slack-req-id"] = private_value
    message = _run_failed_poll(exc, caplog)
    assert message == (
        "Daily device round transport failed error_type=SlackApiError "
        "slack_error_code=unknown status_code=none slack_request_id=none"
    )


def test_other_exceptions_keep_existing_type_only_log(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # response라는 속성이 있어도 실제 SlackApiError가 아니면 원래 로그를 유지한다.
    exc = RuntimeError(_PRIVATE_TEXT + _TOKEN)
    exc.response = _slack_error().response
    message = _run_failed_poll(exc, caplog)
    assert message == "Daily device round transport failed error_type=RuntimeError"
