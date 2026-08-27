from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

import pytest
from pydantic import ValidationError

from boxer_company.hpa_change_coordinator import (
    HpaChangeDelivery,
    HpaChangeDeliveryAckResult,
    HpaChangeSubmissionResult,
    HpaChangeSubmissionState,
    HpaChangeThreadLookup,
    HpaChangeThreadState,
)
from boxer_company.hpa_change_workflow import HpaChangePollState
from boxer_company_api.hpa_change_transport import (
    HpaChangeDeliveryAckInput,
    HpaChangeDeliveryPullInput,
    HpaChangeSubmitInput,
    HpaChangeThreadLookupInput,
    HpaChangeTransportService,
)


_CONTENT = "prompt body"
_SHA256 = hashlib.sha256(_CONTENT.encode()).hexdigest()


@dataclass
class _Coordinator:
    submitted: Any = None
    acked: Any = None

    def submit(self, request):
        self.submitted = request
        return HpaChangeSubmissionResult(
            HpaChangeSubmissionState.ACCEPTED,
            request_id="hpa-20260827140000-12345678-12345678",
            user_message="격리 worker에 전달했어",
        )

    def lookup_thread_job(self, *_args):
        return HpaChangeThreadLookup(
            HpaChangeThreadState.ACTIVE,
            request_id="hpa-20260827140000-12345678-12345678",
            job_status="running",
            event_ts="1720580400.000100",
            current_event=True,
        )

    def pull_pending(self, **_kwargs):
        return (
            HpaChangeDelivery(
                delivery_id="hpa-delivery:" + "a" * 64,
                task_id="hpa-20260827140000-12345678-12345678",
                workspace_id="TWORK",
                channel_id="C02C08K7YEN",
                thread_ts="1720580000.000001",
                state=HpaChangePollState.PR_OPENED,
                workflow_phase="implementation",
                result={"status": "pr_created", "secret": "[REDACTED]"},
                pr_urls=(
                    "https://github.com/mmtalk-app/"
                    "mmb-hospital-admin-server/pull/123",
                ),
                request_text="Bonus 프롬프트 변경",
                attachment_names=("handoff.txt",),
            ),
        )

    def acknowledge_delivery(self, ack):
        self.acked = ack
        return HpaChangeDeliveryAckResult(
            acknowledged=True,
            task_id=ack.task_id,
            job_status="pr_created",
        )


def _submit_payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "workspaceId": "TWORK",
        "requestKey": "slack:TWORK:C02C08K7YEN:1720580400.000100",
        "channelId": "C02C08K7YEN",
        "threadTs": "1720580000.000001",
        "threadUrl": (
            "https://lifexio.slack.com/archives/C068FVD5V7Y/"
            "p1720580000000001"
        ),
        "eventTs": "1720580400.000100",
        "requesterUserId": "U07A5FM5XPD",
        "initiatorUserId": "U07A5FM5XPD",
        "threadText": "Bonus 프롬프트 변경을 검토해줘",
        "attachments": [
            {"name": "handoff.txt", "content": _CONTENT, "sha256": _SHA256}
        ],
        "sourceChannelId": "C068FVD5V7Y",
        "sourceMessageTs": "1720580000.000001",
        "selectionMode": "linked_message",
        "responseThreadUrl": (
            "https://lifexio.slack.com/archives/C02C08K7YEN/"
            "p1720580000000001"
        ),
    }
    payload.update(overrides)
    return payload


def test_submit_contract_converts_slack_collection_to_domain_request() -> None:
    coordinator = _Coordinator()
    service = HpaChangeTransportService(coordinator)  # type: ignore[arg-type]

    response = service.submit(
        "hpa:submit:1",
        HpaChangeSubmitInput.model_validate(_submit_payload()),
    )

    assert response == {
        "requestId": "hpa:submit:1",
        "status": "accepted",
        "hpaRequestId": "hpa-20260827140000-12345678-12345678",
        "message": "격리 worker에 전달했어",
        "autoRetryAllowed": False,
    }
    assert coordinator.submitted.attachments[0].sha256 == _SHA256
    assert coordinator.submitted.source_channel_id == "C068FVD5V7Y"


def test_submit_contract_rejects_actor_mismatch_and_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        HpaChangeSubmitInput.model_validate(
            _submit_payload(initiatorUserId="U07DIFFERENT")
        )
    with pytest.raises(ValidationError):
        HpaChangeSubmitInput.model_validate(_submit_payload(repository="attacker/repo"))


def test_lookup_and_pull_expose_no_github_run_or_error_fields() -> None:
    service = HpaChangeTransportService(_Coordinator())  # type: ignore[arg-type]

    lookup = service.lookup(
        "hpa:lookup:1",
        HpaChangeThreadLookupInput(
            workspaceId="TWORK",
            channelId="C02C08K7YEN",
            threadTs="1720580000.000001",
            eventTs="1720580400.000100",
        ),
    )
    pulled = service.pull(
        "hpa:pull:1",
        HpaChangeDeliveryPullInput(workspaceId="TWORK"),
    )

    assert lookup["state"] == "active"
    delivery = pulled["deliveries"][0]
    assert delivery["state"] == "pr_opened"
    assert delivery["requestSource"]["attachmentNames"] == ["handoff.txt"]
    assert "workflowRunUrl" not in delivery
    assert "errorMessage" not in delivery
    assert pulled["autoRetryAllowed"] is False


def test_ack_contract_passes_exact_delivery_snapshot_without_retry_flag() -> None:
    coordinator = _Coordinator()
    service = HpaChangeTransportService(coordinator)  # type: ignore[arg-type]
    body = HpaChangeDeliveryAckInput(
        workspaceId="TWORK",
        taskId="hpa-20260827140000-12345678-12345678",
        deliveryId="hpa-delivery:" + "a" * 64,
        state="pr_opened",
    )

    response = service.acknowledge("hpa:ack:1", body)

    assert response["acknowledged"] is True
    assert response["autoRetryAllowed"] is False
    assert coordinator.acked.delivery_id == body.deliveryId
    assert coordinator.acked.state is HpaChangePollState.PR_OPENED
