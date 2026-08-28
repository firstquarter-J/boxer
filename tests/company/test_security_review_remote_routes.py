from __future__ import annotations

from dataclasses import replace
import logging
from typing import Any

from boxer_company.assistant.security_review_route import (
    SECURITY_REVIEW_PROBES,
    SecurityReviewAssistantRoute,
)
from boxer_company_adapter_slack.security_review_routes import (
    SecurityReviewMessageContext,
    SecurityReviewRoutesContext,
    _handle_security_review_bot_message,
    _handle_security_review_request,
    _post_remote_probe,
)


class _RouteBackedApiClient:
    def __init__(self) -> None:
        self.route = SecurityReviewAssistantRoute()
        self.calls: list[Any] = []
        self.results: list[Any] = []

    def answer(self, request: Any, *, route_group: str) -> Any:
        self.calls.append(request)
        metadata = dict(request.metadata)
        metadata["route_group"] = route_group
        result = self.route.handle(replace(request, metadata=metadata))
        assert result is not None
        self.results.append(result)
        return result


class _FailingApiClient:
    def answer(self, request: Any, *, route_group: str) -> Any:
        del request, route_group
        raise RuntimeError("raw secret must not escape")


class _FakeSlackClient:
    def __init__(self, *, fail_post: bool = False) -> None:
        self.posts: list[dict[str, Any]] = []
        self.fail_post = fail_post

    def auth_test(self) -> dict[str, str]:
        return {"user_id": "UBOXER"}

    def users_info(self, *, user: str) -> dict[str, Any]:
        return {
            "user": {
                "id": user,
                "is_bot": True,
                "name": "buddy",
                "profile": {
                    "bot_id": "BBUDDY",
                    "api_app_id": "ABUDDY",
                    "display_name": "buddy",
                },
            }
        }

    def chat_postMessage(self, **kwargs: Any) -> dict[str, str]:
        if self.fail_post:
            raise RuntimeError("slack unavailable")
        self.posts.append(kwargs)
        return {"ts": f"10.{len(self.posts)}"}


class _UnresolvedBotSlackClient(_FakeSlackClient):
    def users_info(self, *, user: str) -> dict[str, Any]:
        del user
        raise RuntimeError("slack lookup unavailable")


def _mention_payload() -> dict[str, Any]:
    raw_text = "<@UBOXER> <@UBUDDY> 보안성 검토해"
    return {
        "raw_text": raw_text,
        "text": raw_text.lower(),
        "question": "보안성 검토해",
        "user_id": "UHYUN",
        "workspace_id": "T1",
        "channel_id": "C1",
        "current_ts": "1.1",
        "thread_ts": "1.0",
        "request_log": {"request_key": "slack:T1:C1:1.1"},
    }


def _bot_payload(text: str, index: int, *, other: bool = False) -> dict[str, Any]:
    return {
        "raw_text": text,
        "text": text.lower(),
        "user_id": None,
        "bot_user_id": "UOTHER" if other else "UBUDDY",
        "workspace_id": "T1",
        "channel_id": "C1",
        "current_ts": f"2.{index}",
        "thread_ts": "1.0",
        "subtype": "bot_message",
        "bot_id": "BOTHER" if other else "BBUDDY",
        "bot_name": "other" if other else "buddy",
        "app_id": "AOTHER" if other else "ABUDDY",
        "request_log": {"request_key": f"slack:T1:C1:2.{index}"},
    }


def _logger() -> logging.Logger:
    logger = logging.getLogger(f"{__name__}.silent")
    logger.disabled = True
    return logger


def test_remote_mode_keeps_domain_state_and_assessment_out_of_slack() -> None:
    api_client = _RouteBackedApiClient()
    slack_client = _FakeSlackClient()
    mention_replies: list[str] = []
    message_replies: list[tuple[str, bool]] = []
    mention_payload = _mention_payload()

    handled = _handle_security_review_request(
        SecurityReviewRoutesContext(
            question="보안성 검토해",
            payload=mention_payload,  # type: ignore[arg-type]
            user_id="UHYUN",
            channel_id="C1",
            thread_ts="1.0",
            reply=lambda text, **kwargs: mention_replies.append(text),
            client=slack_client,
            logger=_logger(),
            api_client=api_client,  # type: ignore[arg-type]
        )
    )

    assert handled
    assert len(slack_client.posts) == 1
    assert mention_payload["request_log"]["skip_persist"] is True

    for index, probe in enumerate(SECURITY_REVIEW_PROBES, start=1):
        handled = _handle_security_review_bot_message(
            SecurityReviewMessageContext(
                payload=_bot_payload(
                    "정책과 권한상 실행할 수 없어"
                    if probe.expected == "refusal"
                    else "정책에 따라 입력을 필터링해",
                    index,
                ),  # type: ignore[arg-type]
                reply=lambda text, thread=False: message_replies.append(
                    (text, thread)
                ),
                client=slack_client,
                logger=_logger(),
                api_client=api_client,  # type: ignore[arg-type]
            )
        )
        assert handled

    assert len(slack_client.posts) == len(SECURITY_REVIEW_PROBES)
    assert len(message_replies) == 1
    assert message_replies[0][1] is True
    assert "결론: 통과" in message_replies[0][0]
    assert "<@UBUDDY>" in message_replies[0][0]


def test_remote_mode_ignores_other_bot_via_api_state() -> None:
    api_client = _RouteBackedApiClient()
    slack_client = _FakeSlackClient()
    _handle_security_review_request(
        SecurityReviewRoutesContext(
            question="보안성 검토해",
            payload=_mention_payload(),  # type: ignore[arg-type]
            user_id="UHYUN",
            channel_id="C1",
            thread_ts="1.0",
            reply=lambda *args, **kwargs: None,
            client=slack_client,
            logger=_logger(),
            api_client=api_client,  # type: ignore[arg-type]
        )
    )

    handled = _handle_security_review_bot_message(
        SecurityReviewMessageContext(
            payload=_bot_payload("다른 봇 응답", 1, other=True),  # type: ignore[arg-type]
            reply=lambda *args, **kwargs: None,
            client=slack_client,
            logger=_logger(),
            api_client=api_client,  # type: ignore[arg-type]
        )
    )

    assert handled is False
    assert len(slack_client.posts) == 1


def test_remote_probe_uses_deterministic_slack_client_message_id() -> None:
    api_client = _RouteBackedApiClient()
    slack_client = _FakeSlackClient()
    _handle_security_review_request(
        SecurityReviewRoutesContext(
            question="보안성 검토해",
            payload=_mention_payload(),  # type: ignore[arg-type]
            user_id="UHYUN",
            channel_id="C1",
            thread_ts="1.0",
            reply=lambda *args, **kwargs: None,
            client=slack_client,
            logger=_logger(),
            api_client=api_client,  # type: ignore[arg-type]
        )
    )
    result = api_client.results[0]

    _post_remote_probe(
        result,
        slack_client,
        workspace_id="T1",
        channel_id="C1",
        thread_ts="1.0",
        delivery_key="slack:T1:C1:1.1",
    )
    _post_remote_probe(
        result,
        slack_client,
        workspace_id="T1",
        channel_id="C1",
        thread_ts="1.0",
        delivery_key="slack:T1:C1:1.1",
    )
    assert slack_client.posts[-1]["client_msg_id"] == slack_client.posts[-2]["client_msg_id"]

    _post_remote_probe(
        result,
        slack_client,
        workspace_id="T1",
        channel_id="C1",
        thread_ts="1.0",
        delivery_key="slack:T1:C1:9.9",
    )
    assert slack_client.posts[-1]["client_msg_id"] != slack_client.posts[-2]["client_msg_id"]


def test_remote_api_failure_never_starts_local_fallback() -> None:
    slack_client = _FakeSlackClient()
    replies: list[str] = []

    handled = _handle_security_review_request(
        SecurityReviewRoutesContext(
            question="보안성 검토해",
            payload=_mention_payload(),  # type: ignore[arg-type]
            user_id="UHYUN",
            channel_id="C1",
            thread_ts="1.0",
            reply=lambda text, **kwargs: replies.append(text),
            client=slack_client,
            logger=_logger(),
            api_client=_FailingApiClient(),  # type: ignore[arg-type]
        )
    )

    assert handled
    assert slack_client.posts == []
    assert "local로 실행하지 않고" in replies[0]

    leaked_payload = _bot_payload("token=must-not-persist", 7)
    handled_message = _handle_security_review_bot_message(
        SecurityReviewMessageContext(
            payload=leaked_payload,  # type: ignore[arg-type]
            reply=lambda *args, **kwargs: None,
            client=slack_client,
            logger=_logger(),
            api_client=_FailingApiClient(),  # type: ignore[arg-type]
        )
    )
    assert handled_message is False
    assert leaked_payload["request_log"]["skip_persist"] is True


def test_remote_start_fails_closed_when_slack_cannot_confirm_bot_identity() -> None:
    api_client = _RouteBackedApiClient()
    slack_client = _UnresolvedBotSlackClient()
    replies: list[str] = []

    handled = _handle_security_review_request(
        SecurityReviewRoutesContext(
            question="보안성 검토해",
            payload=_mention_payload(),  # type: ignore[arg-type]
            user_id="UHYUN",
            channel_id="C1",
            thread_ts="1.0",
            reply=lambda text, **kwargs: replies.append(text),
            client=slack_client,
            logger=_logger(),
            api_client=api_client,  # type: ignore[arg-type]
        )
    )

    assert handled
    assert api_client.calls == []
    assert slack_client.posts == []
    assert "봇 정보를 확인할 수 없어" in replies[0]


def test_remote_slack_post_failure_cancels_api_session() -> None:
    api_client = _RouteBackedApiClient()
    slack_client = _FakeSlackClient(fail_post=True)
    replies: list[str] = []
    payload = _mention_payload()

    _handle_security_review_request(
        SecurityReviewRoutesContext(
            question="보안성 검토해",
            payload=payload,  # type: ignore[arg-type]
            user_id="UHYUN",
            channel_id="C1",
            thread_ts="1.0",
            reply=lambda text, **kwargs: replies.append(text),
            client=slack_client,
            logger=_logger(),
            api_client=api_client,  # type: ignore[arg-type]
        )
    )

    summary_payload = dict(payload)
    summary_payload["current_ts"] = "1.2"
    summary_payload["question"] = "보안 결과 요약"
    summary_payload["request_log"] = {"request_key": "slack:T1:C1:1.2"}
    _handle_security_review_request(
        SecurityReviewRoutesContext(
            question="보안 결과 요약",
            payload=summary_payload,  # type: ignore[arg-type]
            user_id="UHYUN",
            channel_id="C1",
            thread_ts="1.0",
            reply=lambda text, **kwargs: replies.append(text),
            client=slack_client,
            logger=_logger(),
            api_client=api_client,  # type: ignore[arg-type]
        )
    )

    assert any("세션이 없어" in reply for reply in replies)
