from __future__ import annotations

import unittest

from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantResult,
)
from boxer_company.assistant.device_health_alert_action_route import (
    DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_ROUTE,
    DEVICE_HEALTH_ALERT_VOICE_ROUTE,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiAmbiguousTimeoutError,
    CompanyApiContractError,
)
from boxer_company_adapter_slack.device_health_alert_api import (
    DeviceHealthAlertApiBridge,
    build_device_health_alert_api_target,
    build_device_health_alert_request_id,
)


def _raw_item() -> dict[str, object]:
    return {
        "hospitalSeq": "31",
        "hospitalName": "분당제일병원",
        "hospital": "분당제일병원 (#31)",
        "room": "2진료실",
        "device": "MB2-C00419",
        "issue": "캡처보드 연결 확인 필요",
        "alertCategory": "video_signal",
        "problemComponents": ["캡처보드"],
    }


def _receipt_target() -> dict[str, object]:
    return {
        "hospital": "분당제일병원",
        "room": "2진료실",
        "device": "MB2-C00419",
        "components": ["캡처보드"],
        "issue": "캡처보드 연결 확인 필요",
    }


class _FakeClient:
    def __init__(self, result=None, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.calls = []

    def answer(self, request, *, route_group=None):  # type: ignore[no-untyped-def]
        self.calls.append((request, route_group))
        if self.error is not None:
            raise self.error
        return self.result


class DeviceHealthAlertApiBridgeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.target = build_device_health_alert_api_target(_raw_item())

    def _kwargs(self) -> dict[str, object]:
        return {
            "request_id": "slack-device-alert-123",
            "workspace_id": "T-WORKSPACE",
            "actor_user_id": "U-ACTOR",
            "channel_id": "C-ALERT",
            "conversation_id": "171.0001",
            "target": self.target,
        }

    def test_stable_request_id_uses_full_interaction_identity(self) -> None:
        values = {
            "workspace_id": "T-WORKSPACE",
            "actor_user_id": "U-ACTOR",
            "channel_id": "C-ALERT",
            "message_ts": "171.0001",
            "interaction_id": "action-ts-1",
            "action_name": "device_health_alert_contact_hospital",
            "phase": "prepare",
        }

        first = build_device_health_alert_request_id(**values)
        second = build_device_health_alert_request_id(**values)
        changed = build_device_health_alert_request_id(
            **{**values, "phase": "execute"}
        )

        self.assertEqual(first, second)
        self.assertNotEqual(first, changed)
        self.assertRegex(first, r"^slack-device-alert-[0-9a-f]{40}$")
        self.assertNotIn("U-ACTOR", first)

    def test_prepare_builds_typed_request_and_calls_api_once(self) -> None:
        result = CompanyAssistantResult(
            route=DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
            outcome="answered",
            messages=(
                AssistantMessage(
                    body="병원 문자 입력 정보를 준비했어",
                    delivery_scope="requester",
                ),
            ),
            operation_result={
                "kind": "sms_contact_preparation",
                "deliveryScope": "requester",
                "phoneNumber": "01012345678",
                "message": "안내 문자",
                "templateId": "captureboard_disconnected",
                "target": _receipt_target(),
            },
        )
        client = _FakeClient(result=result)

        prepared = DeviceHealthAlertApiBridge(client).prepare_sms(
            **self._kwargs()
        )

        self.assertEqual(prepared.route, DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE)
        self.assertEqual(prepared.operation_result["phoneNumber"], "01012345678")
        self.assertEqual(len(client.calls), 1)
        request, route_group = client.calls[0]
        self.assertEqual(route_group, "operations")
        self.assertEqual(request.question, "device health alert action")
        action = request.metadata["operation_action"]
        self.assertEqual(action["phase"], "prepare")
        self.assertNotIn("sms", action)
        self.assertEqual(action["target"]["device_name"], "MB2-C00419")

    def test_sms_builds_execute_payload_and_validates_safe_receipt(self) -> None:
        result = CompanyAssistantResult(
            route=DEVICE_HEALTH_ALERT_SMS_ROUTE,
            outcome="answered",
            messages=(AssistantMessage(body="병원 문자 공급자 접수 완료"),),
            operation_result={
                "kind": "sms_delivery",
                "provider": "solapi",
                "deliveryStatus": "accepted",
                "groupId": "GROUP-1",
                "messageId": "MESSAGE-1",
                "acceptedAt": "2026-08-14T00:00:00+00:00",
                "target": _receipt_target(),
            },
        )
        client = _FakeClient(result=result)

        sent = DeviceHealthAlertApiBridge(client).send_sms(
            **self._kwargs(),
            phone_number="01012345678",
            message="직접 작성한 안내 문자",
        )

        self.assertEqual(sent.route, DEVICE_HEALTH_ALERT_SMS_ROUTE)
        self.assertEqual(sent.operation_result["groupId"], "GROUP-1")
        request, route_group = client.calls[0]
        self.assertEqual(route_group, "operations")
        self.assertEqual(
            request.metadata["operation_action"]["sms"],
            {
                "phone_number": "01012345678",
                "message": "직접 작성한 안내 문자",
            },
        )

    def test_transport_error_propagates_after_one_call_without_fallback(self) -> None:
        client = _FakeClient(
            error=CompanyApiAmbiguousTimeoutError(
                "read timeout",
                request_id="slack-device-alert-123",
            )
        )

        with self.assertRaises(CompanyApiAmbiguousTimeoutError):
            DeviceHealthAlertApiBridge(client).send_voice_guide(
                **self._kwargs()
            )

        self.assertEqual(len(client.calls), 1)

    def test_voice_and_mark_done_reject_unexpected_receipt(self) -> None:
        for route, method_name in (
            (DEVICE_HEALTH_ALERT_VOICE_ROUTE, "send_voice_guide"),
            (DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE, "mark_done"),
        ):
            with self.subTest(route=route):
                client = _FakeClient(
                    result=CompanyAssistantResult(
                        route=route,
                        outcome="answered",
                        messages=(AssistantMessage(body="완료"),),
                        operation_result={"kind": "unexpected"},
                    )
                )
                bridge = DeviceHealthAlertApiBridge(client)

                with self.assertRaises(CompanyApiContractError):
                    getattr(bridge, method_name)(**self._kwargs())

                self.assertEqual(len(client.calls), 1)

    def test_receipt_target_mismatch_is_fail_closed(self) -> None:
        mismatched_target = _receipt_target()
        mismatched_target["device"] = "MB2-C00999"
        client = _FakeClient(
            result=CompanyAssistantResult(
                route=DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
                outcome="answered",
                messages=(
                    AssistantMessage(body="준비", delivery_scope="requester"),
                ),
                operation_result={
                    "kind": "sms_contact_preparation",
                    "deliveryScope": "requester",
                    "phoneNumber": "",
                    "message": "",
                    "templateId": "unsupported_issue",
                    "target": mismatched_target,
                },
            )
        )

        with self.assertRaises(CompanyApiContractError):
            DeviceHealthAlertApiBridge(client).prepare_sms(**self._kwargs())


if __name__ == "__main__":
    unittest.main()
