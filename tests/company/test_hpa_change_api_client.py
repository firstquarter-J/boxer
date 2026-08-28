from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
import requests

from boxer_company.hpa_change_workflow import HpaChangePollState
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiAmbiguousTimeoutError,
    CompanyApiClientSettings,
    CompanyApiContractError,
)
from boxer_company_adapter_slack.hpa_change_api_client import (
    HpaChangeApiClient,
    HpaChangeRemoteDelivery,
    build_hpa_change_remote_routes_config,
)
from boxer_company_adapter_slack.hpa_change_remote_reporter import (
    run_hpa_change_remote_reporter_once,
)
from boxer_company_adapter_slack.hpa_change_routes import (
    HpaChangeAttachment,
    HpaChangeRequest,
    HpaChangeSubmissionStatus,
)


class _Response:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict[str, Any]:
        return self._payload


class _Session:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs: Any) -> _Response:
        self.calls.append({"url": url, **kwargs})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class _Slack:
    def __init__(self, *, fail_at: int = 0) -> None:
        self.fail_at = fail_at
        self.calls: list[dict[str, Any]] = []

    def chat_postMessage(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        if self.fail_at and len(self.calls) == self.fail_at:
            raise RuntimeError("slack failed")
        return {"ok": True, "ts": "1720580999.000001"}


def _settings() -> CompanyApiClientSettings:
    return CompanyApiClientSettings(
        base_url="http://10.40.99.44:8010",
        token="t" * 48,
        connect_timeout_sec=1,
        operations_read_timeout_sec=30,
        max_retries=0,
    )


def _request() -> HpaChangeRequest:
    return HpaChangeRequest(
        request_key="slack:TWORK:C02C08K7YEN:1720580400.000100",
        workspace_id="TWORK",
        channel_id="C02C08K7YEN",
        thread_ts="1720580000.000001",
        thread_url=(
            "https://lifexio.slack.com/archives/C068FVD5V7Y/"
            "p1720580000000001"
        ),
        event_ts="1720580400.000100",
        requester_user_id="U07A5FM5XPD",
        thread_text="Bonus 프롬프트 변경을 검토해줘",
        attachments=(
            HpaChangeAttachment(
                name="handoff.txt",
                size_bytes=11,
                content="prompt body",
            ),
        ),
        initiator_user_id="U07A5FM5XPD",
        source_channel_id="C068FVD5V7Y",
        source_message_ts="1720580000.000001",
        selection_mode="linked_message",
        response_thread_url=(
            "https://lifexio.slack.com/archives/C02C08K7YEN/"
            "p1720580000000001"
        ),
    )


def _delivery() -> HpaChangeRemoteDelivery:
    return HpaChangeRemoteDelivery(
        delivery_id="hpa-delivery:" + "a" * 64,
        task_id="hpa-20260827140000-12345678-12345678",
        workspace_id="TWORK",
        channel_id="C02C08K7YEN",
        thread_ts="1720580000.000001",
        state=HpaChangePollState.NEEDS_CLARIFICATION,
        workflow_phase="review",
        result={"status": "needs_clarification"},
        request_text="Bonus 프롬프트 변경",
    )


def test_submit_sends_collected_context_only_and_never_repo_selector() -> None:
    session = _Session(
        [
            _Response(
                {
                    "requestId": (
                        "hpa:submit:"
                        "9f5da47f8cd311d11c2679ad41a85c35"
                    ),
                    "status": "accepted",
                    "hpaRequestId": "hpa-20260827140000-12345678-12345678",
                    "message": "격리 worker에 전달했어",
                    "autoRetryAllowed": False,
                }
            )
        ]
    )
    # request ID는 event provenance로 결정되므로 fixture와 실제 digest를 맞춘다.
    import hashlib

    digest = hashlib.sha256(b"TWORK:1720580400.000100").hexdigest()[:32]
    session.responses[0]._payload["requestId"] = f"hpa:submit:{digest}"
    client = HpaChangeApiClient(_settings(), workspace_id="TWORK", session=session)

    result = client.submit_request(_request())

    assert result.status is HpaChangeSubmissionStatus.ACCEPTED
    payload = session.calls[0]["json"]
    assert payload["attachments"][0]["content"] == "prompt body"
    assert "repository" not in payload
    assert "workflow" not in payload
    assert len(session.calls) == 1


def test_submit_read_timeout_is_ambiguous_and_not_retried() -> None:
    session = _Session([requests.exceptions.ReadTimeout("timeout")])
    client = HpaChangeApiClient(_settings(), workspace_id="TWORK", session=session)

    with pytest.raises(CompanyApiAmbiguousTimeoutError):
        client.submit_request(_request())

    assert len(session.calls) == 1


def test_workspace_mismatch_is_rejected_before_http() -> None:
    session = _Session([])
    client = HpaChangeApiClient(
        _settings(),
        workspace_id="TOTHER",
        session=session,
    )

    with pytest.raises(
        CompanyApiContractError,
        match="company_api_hpa_workspace_mismatch",
    ):
        client.submit_request(_request())
    with pytest.raises(
        CompanyApiContractError,
        match="company_api_hpa_workspace_mismatch",
    ):
        client.lookup_thread_job(
            "TWORK",
            "C02C08K7YEN",
            "1720580000.000001",
            "1720580400.000100",
        )

    assert session.calls == []


def test_remote_routes_config_preserves_fixed_channel_and_size_policy() -> None:
    config = build_hpa_change_remote_routes_config(
        SimpleNamespace(
            HPA_CHANGE_REQUEST_ENABLED=True,
            HPA_CHANGE_REQUEST_ALLOWED_CHANNEL_IDS={
                "C02C08K7YEN",
                "C068FVD5V7Y",
            },
            HPA_CHANGE_MAX_THREAD_CHARS=30_000,
            HPA_CHANGE_MAX_FILES=5,
            HPA_CHANGE_MAX_FILE_BYTES=131_072,
            HPA_CHANGE_MAX_TOTAL_ATTACHMENT_BYTES=524_288,
        )
    )

    assert config.enabled is True
    assert config.allowed_channel_ids == {
        "C02C08K7YEN",
        "C068FVD5V7Y",
    }

    with pytest.raises(
        CompanyApiContractError,
        match="company_api_hpa_channel_policy_invalid",
    ):
        build_hpa_change_remote_routes_config(
            SimpleNamespace(
                HPA_CHANGE_REQUEST_ENABLED=True,
                HPA_CHANGE_REQUEST_ALLOWED_CHANNEL_IDS={"C02C08K7YEN"},
            )
        )
    with pytest.raises(
        CompanyApiContractError,
        match="company_api_hpa_intake_limits_invalid",
    ):
        build_hpa_change_remote_routes_config(
            SimpleNamespace(
                HPA_CHANGE_REQUEST_ENABLED=True,
                HPA_CHANGE_REQUEST_ALLOWED_CHANNEL_IDS={
                    "C02C08K7YEN",
                    "C068FVD5V7Y",
                },
                HPA_CHANGE_MAX_THREAD_CHARS=30_000,
                HPA_CHANGE_MAX_FILES=5,
                HPA_CHANGE_MAX_FILE_BYTES=524_288,
                HPA_CHANGE_MAX_TOTAL_ATTACHMENT_BYTES=131_072,
            )
        )


def test_remote_reporter_posts_then_acks_without_local_job_state() -> None:
    delivery = _delivery()

    class _Api:
        def __init__(self) -> None:
            self.acks: list[HpaChangeRemoteDelivery] = []

        def pull_pending(self):
            return (delivery,)

        def acknowledge_delivery(self, item):
            self.acks.append(item)

    api = _Api()
    slack = _Slack()
    renderer = lambda _item: ("검토 결과", "추가 질문")

    sent = run_hpa_change_remote_reporter_once(
        api,  # type: ignore[arg-type]
        slack,
        renderer=renderer,
    )

    assert sent == 1
    assert api.acks == [delivery]
    assert [call["text"] for call in slack.calls] == ["검토 결과", "추가 질문"]
    assert slack.calls[0]["client_msg_id"] != slack.calls[1]["client_msg_id"]


def test_remote_reporter_does_not_ack_partial_slack_delivery() -> None:
    delivery = _delivery()

    class _Api:
        def __init__(self) -> None:
            self.acks: list[Any] = []

        def pull_pending(self):
            return (delivery,)

        def acknowledge_delivery(self, item):
            self.acks.append(item)

    api = _Api()
    slack = _Slack(fail_at=2)

    sent = run_hpa_change_remote_reporter_once(
        api,  # type: ignore[arg-type]
        slack,
        renderer=lambda _item: ("검토 결과", "추가 질문"),
    )

    assert sent == 0
    assert api.acks == []
    assert len(slack.calls) == 2
