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
    DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE,
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
        "mdaUrl": "https://mda.example/monitoring?focusDevice=MB2-C00419",
    }


def _receipt_target() -> dict[str, object]:
    return {
        "hospital": "분당제일병원",
        "room": "2진료실",
        "device": "MB2-C00419",
        "components": ["캡처보드"],
        "issue": "캡처보드 연결 확인 필요",
    }


def _ack_receipt(*, created: bool = True) -> dict[str, object]:
    return {
        "kind": "device_health_alert_ack",
        "created": created,
        "actorUserId": "U-ACTOR",
        "acknowledgedAt": "2026-08-31T09:07:12+09:00",
        "target": _receipt_target(),
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

        mark_done = build_device_health_alert_request_id(
            **{
                **values,
                "action_name": "device_health_alert_mark_done",
                "phase": "execute",
            }
        )
        self.assertRegex(
            mark_done,
            r"^slack-device-alert-ack-v1-[0-9a-f]{40}$",
        )

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

    def test_modal_result_is_sent_as_typed_ui_receipt_once(self) -> None:
        client = _FakeClient(
            result=CompanyAssistantResult(
                route=DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE,
                outcome="answered",
                messages=(AssistantMessage(body="UI 결과를 기록했어"),),
            )
        )

        recorded = DeviceHealthAlertApiBridge(client).record_modal_receipt(
            **self._kwargs(),
            action_id="device_health_alert_contact_hospital",
            mode="send",
            message_ts="171.0001",
            thread_ts="171.0001",
            occurred_at="2026-08-14T09:30:00+09:00",
            status="modal_opened",
            ok=True,
        )

        self.assertEqual(recorded.route, DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE)
        self.assertEqual(len(client.calls), 1)
        request, route_group = client.calls[0]
        self.assertEqual(route_group, "operations")
        action = request.metadata["operation_action"]
        self.assertEqual(action["name"], "device_health_alert_ui_receipt")
        self.assertEqual(action["phase"], "receipt")
        self.assertEqual(
            action["event_type"],
            "alert_contact_sms_modal_requested",
        )
        self.assertEqual(action["status"], "modal_opened")
        self.assertTrue(action["ok"])
        self.assertEqual(
            action["target"]["hospital_label"],
            "분당제일병원 (#31)",
        )
        self.assertEqual(
            action["target"]["mda_url"],
            "https://mda.example/monitoring?focusDevice=MB2-C00419",
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

    def test_voice_rejects_unexpected_receipt(self) -> None:
        client = _FakeClient(
            result=CompanyAssistantResult(
                route=DEVICE_HEALTH_ALERT_VOICE_ROUTE,
                outcome="answered",
                messages=(AssistantMessage(body="완료"),),
                operation_result={"kind": "unexpected"},
            )
        )

        with self.assertRaises(CompanyApiContractError):
            DeviceHealthAlertApiBridge(client).send_voice_guide(
                **self._kwargs()
            )

        self.assertEqual(len(client.calls), 1)

    def test_mark_done_accepts_created_and_existing_ack_receipts(self) -> None:
        # 최초 claim과 재클릭 응답 모두 최초 담당자·시간을 담은 같은 typed
        # acknowledgement 계약으로 Slack UI까지 전달돼야 한다.
        for created in (True, False):
            with self.subTest(created=created):
                receipt = _ack_receipt(created=created)
                client = _FakeClient(
                    result=CompanyAssistantResult(
                        route=DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
                        outcome="answered",
                        messages=(AssistantMessage(body="확인 완료"),),
                        operation_result=receipt,
                    )
                )

                acknowledged = DeviceHealthAlertApiBridge(client).mark_done(
                    **self._kwargs()
                )

                self.assertEqual(
                    acknowledged.route,
                    DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
                )
                self.assertEqual(acknowledged.operation_result, receipt)
                self.assertEqual(len(client.calls), 1)
                request, route_group = client.calls[0]
                self.assertEqual(route_group, "operations")
                self.assertEqual(
                    request.metadata["operation_action"]["name"],
                    "device_health_alert_mark_done",
                )
                self.assertEqual(
                    request.metadata["operation_action"]["phase"],
                    "execute",
                )

    def test_mark_done_failure_keeps_message_without_ack_receipt(self) -> None:
        # 저장·대상 검증 실패는 완료 영수증이 없어야 하고 Slack은 원래
        # 실패 안내를 전달한 뒤 버튼을 남겨 재시도할 수 있어야 한다.
        client = _FakeClient(
            result=CompanyAssistantResult(
                route=DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
                outcome="failed",
                messages=(AssistantMessage(body="완료 상태 저장 실패"),),
            )
        )

        failed = DeviceHealthAlertApiBridge(client).mark_done(
            **self._kwargs()
        )

        self.assertEqual(failed.outcome, "failed")
        self.assertEqual(failed.messages, ("완료 상태 저장 실패",))
        self.assertIsNone(failed.operation_result)

    def test_mark_done_accepts_legacy_api_response_without_receipt(self) -> None:
        # API를 먼저 배포하지 못한 순차 배포 창에는 구 응답을 그대로
        # 전달하고 카드 UI만 다음 신 API 응답까지 유지한다.
        client = _FakeClient(
            result=CompanyAssistantResult(
                route=DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
                outcome="answered",
                messages=(AssistantMessage(body="확인 완료"),),
            )
        )

        legacy = DeviceHealthAlertApiBridge(client).mark_done(
            **self._kwargs()
        )

        self.assertEqual(legacy.outcome, "answered")
        self.assertIsNone(legacy.operation_result)

    def test_mark_done_rejects_invalid_ack_receipts(self) -> None:
        valid = _ack_receipt()
        missing_created = dict(valid)
        missing_created.pop("created")
        mismatched_target = _receipt_target()
        mismatched_target["device"] = "MB2-C00999"
        cases = (
            (
                "wrong_kind",
                {**valid, "kind": "unexpected"},
                "device_health_alert_ack_receipt_invalid",
            ),
            (
                "missing_field",
                missing_created,
                "device_health_alert_ack_receipt_invalid",
            ),
            (
                "unexpected_field",
                {**valid, "unexpected": True},
                "device_health_alert_ack_receipt_invalid",
            ),
            (
                "created_not_boolean",
                {**valid, "created": 1},
                "device_health_alert_ack_receipt_invalid",
            ),
            (
                "actor_missing",
                {**valid, "actorUserId": "  "},
                "device_health_alert_ack_receipt_invalid",
            ),
            (
                "actor_markup_injection",
                {**valid, "actorUserId": "U1> <!channel"},
                "device_health_alert_ack_receipt_invalid",
            ),
            (
                "timestamp_without_timezone",
                {**valid, "acknowledgedAt": "2026-08-31T09:07:12"},
                "device_health_alert_ack_receipt_invalid",
            ),
            (
                "timestamp_invalid",
                {**valid, "acknowledgedAt": "not-a-timestamp"},
                "device_health_alert_ack_receipt_invalid",
            ),
            (
                "target_mismatch",
                {**valid, "target": mismatched_target},
                "device_health_alert_receipt_target_mismatch",
            ),
        )

        # 형식이 모호한 receipt를 일부 수용하면 잘못된 담당자·시간으로
        # 버튼을 제거할 수 있으므로 모든 계약 위반을 fail-closed한다.
        for name, receipt, error_code in cases:
            with self.subTest(name=name):
                client = _FakeClient(
                    result=CompanyAssistantResult(
                        route=DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
                        outcome="answered",
                        messages=(AssistantMessage(body="확인 완료"),),
                        operation_result=receipt,
                    )
                )

                with self.assertRaises(CompanyApiContractError) as raised:
                    DeviceHealthAlertApiBridge(client).mark_done(
                        **self._kwargs()
                    )

                self.assertEqual(str(raised.exception), error_code)
                self.assertEqual(len(client.calls), 1)

    def test_new_mark_done_ack_must_name_current_actor(self) -> None:
        client = _FakeClient(
            result=CompanyAssistantResult(
                route=DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
                outcome="answered",
                messages=(AssistantMessage(body="확인 완료"),),
                operation_result={
                    **_ack_receipt(created=True),
                    "actorUserId": "U-OTHER",
                },
            )
        )

        with self.assertRaises(CompanyApiContractError) as raised:
            DeviceHealthAlertApiBridge(client).mark_done(**self._kwargs())

        self.assertEqual(
            str(raised.exception),
            "device_health_alert_ack_actor_mismatch",
        )

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
