from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import json
import logging
import unittest
from unittest.mock import Mock
from unittest.mock import patch

from pydantic import ValidationError

from boxer_company import settings as company_settings
from boxer_company.assistant import device_health_alert_action_route as action_route
from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.device_health_alert_action_route import (
    DEVICE_HEALTH_ALERT_MARK_DONE_ACTION,
    DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_ACTION,
    DEVICE_HEALTH_ALERT_SMS_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
    DEVICE_HEALTH_ALERT_VOICE_ACTION,
    DEVICE_HEALTH_ALERT_VOICE_ROUTE,
    DeviceHealthAlertActionAssistantRoute,
    DeviceHealthAlertActionRouteDeps,
    match_device_health_alert_action_route,
)
from boxer_company.assistant.operations import (
    company_operation_route_names,
    match_company_operation_route,
)
from boxer_company.routers.device_ssh_security import (
    company_api_device_ssh_context,
)
from boxer_company_api.auth import CallerPrincipal
from boxer_company_api.policies import authorize_turn
from boxer_company_api.problems import CompanyApiProblem
from boxer_company_api.schemas import AssistantTurnInput, serialize_result
from boxer_company_adapter_slack import device_health_monitor_reporter


def _action_metadata(
    name: str,
    *,
    include_sms: bool = False,
) -> dict[str, object]:
    action: dict[str, object] = {
        "name": name,
        "phase": (
            "execute"
            if name != DEVICE_HEALTH_ALERT_SMS_ACTION or include_sms
            else "prepare"
        ),
        "target": {
            "hospital_seq": 31,
            "hospital_name": "분당제일병원",
            "room_name": "2진료실",
            "device_name": "MB2-C00419",
            "issue": "캡처보드 연결 확인 필요",
            "alert_category": "video_signal",
            "problem_components": ["캡처보드"],
        },
    }
    if include_sms:
        action["sms"] = {
            "phone_number": "01012345678",
            "message": "직접 작성한 안내 문자입니다.",
        }
    return {"route_group": "operations", "operation_action": action}


def _request(name: str, *, include_sms: bool = False) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="REQ-ALERT-ACTION-1",
        tenant_id="TENANT-1",
        actor_id="ACTOR-1",
        channel="slack",
        conversation_id="THREAD-1",
        # Typed action route가 질문 문구를 실행 명령으로 해석하지 않는지 고정한다.
        question="이 문구는 실행 명령이 아니야",
        locale="ko",
        metadata=_action_metadata(name, include_sms=include_sms),
    )


def _exact_target() -> dict[str, object]:
    return {
        "hospitalSeq": 31,
        "hospitalName": "분당제일병원",
        "roomName": "2진료실",
        "deviceName": "MB2-C00419",
        "telephone": "0310000000",
        "deviceAlertPhone": "01012345678",
    }


class DeviceHealthAlertActionRouteTests(unittest.TestCase):
    def test_matcher_uses_typed_action_instead_of_question(self) -> None:
        for action_name, expected_route in (
            (DEVICE_HEALTH_ALERT_SMS_ACTION, DEVICE_HEALTH_ALERT_SMS_ROUTE),
            (DEVICE_HEALTH_ALERT_VOICE_ACTION, DEVICE_HEALTH_ALERT_VOICE_ROUTE),
            (DEVICE_HEALTH_ALERT_MARK_DONE_ACTION, DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE),
        ):
            with self.subTest(action_name=action_name):
                request = _request(
                    action_name,
                    include_sms=action_name == DEVICE_HEALTH_ALERT_SMS_ACTION,
                )
                self.assertEqual(
                    match_device_health_alert_action_route(request),
                    expected_route,
                )
                self.assertEqual(
                    match_company_operation_route(request),
                    expected_route,
                )

        self.assertEqual(
            match_device_health_alert_action_route(
                _request(DEVICE_HEALTH_ALERT_SMS_ACTION)
            ),
            DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
        )

    def test_sms_prepare_reads_db_without_sending_and_returns_private_defaults(self) -> None:
        load_target = Mock(return_value=_exact_target())
        send_sms = Mock()
        deps = replace(
            DeviceHealthAlertActionRouteDeps(),
            load_exact_target=load_target,
            send_sms=send_sms,
        )

        result = DeviceHealthAlertActionAssistantRoute(deps).handle(
            _request(DEVICE_HEALTH_ALERT_SMS_ACTION)
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.route, DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE)
        self.assertEqual(result.messages[0].delivery_scope, "requester")
        self.assertEqual(
            result.operation_result["kind"],
            "sms_contact_preparation",
        )
        self.assertEqual(
            result.operation_result["phoneNumber"],
            "01012345678",
        )
        self.assertNotIn("01012345678", result.messages[0].body)
        load_target.assert_called_once()
        send_sms.assert_not_called()

    def test_sms_action_calls_exact_target_and_provider_once(self) -> None:
        load_target = Mock(return_value=_exact_target())
        send_sms = Mock(
            return_value={
                "status": "sent",
                "ok": True,
                "provider": "solapi",
                "groupId": "GROUP-1",
                "messageId": "MESSAGE-1",
                "smsDeliveryStatus": "accepted",
            }
        )
        remember_sms_delivery = Mock(return_value=True)
        deps = replace(
            DeviceHealthAlertActionRouteDeps(),
            load_exact_target=load_target,
            send_sms=send_sms,
            remember_sms_delivery=remember_sms_delivery,
            now=lambda: datetime(2026, 8, 14, tzinfo=timezone.utc),
        )

        result = DeviceHealthAlertActionAssistantRoute(deps).handle(
            _request(DEVICE_HEALTH_ALERT_SMS_ACTION, include_sms=True)
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.route, DEVICE_HEALTH_ALERT_SMS_ROUTE)
        self.assertEqual(result.outcome, "answered")
        load_target.assert_called_once()
        send_sms.assert_called_once()
        payload = send_sms.call_args.args[0]
        self.assertEqual(payload["sms"]["to"], "01012345678")
        self.assertEqual(payload["origin"]["requestId"], "REQ-ALERT-ACTION-1")
        self.assertNotIn("slack", payload)
        self.assertEqual(result.operation_result["groupId"], "GROUP-1")
        serialized_receipt = json.dumps(result.operation_result, ensure_ascii=False)
        self.assertNotIn("01012345678", serialized_receipt)
        self.assertNotIn("직접 작성한", serialized_receipt)
        self.assertNotIn("GROUP-1", result.messages[0].body)
        remember_sms_delivery.assert_called_once()
        remembered_item = remember_sms_delivery.call_args.args[0]
        self.assertEqual(remembered_item["smsGroupId"], "GROUP-1")
        self.assertEqual(remembered_item["device"], "MB2-C00419")

    def test_target_mismatch_blocks_every_side_effect(self) -> None:
        load_target = Mock(return_value=None)
        get_mda = Mock()
        send_command = Mock()
        send_sms = Mock()
        deps = replace(
            DeviceHealthAlertActionRouteDeps(),
            load_exact_target=load_target,
            get_mda_device=get_mda,
            send_mda_command=send_command,
            send_sms=send_sms,
        )

        result = DeviceHealthAlertActionAssistantRoute(deps).handle(
            _request(DEVICE_HEALTH_ALERT_VOICE_ACTION)
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "denied")
        self.assertEqual(
            result.fallback_reason,
            "device_health_alert_target_mismatch",
        )
        get_mda.assert_not_called()
        send_command.assert_not_called()
        send_sms.assert_not_called()

    def test_sms_receipt_persist_failure_never_retries_provider(self) -> None:
        send_sms = Mock(
            return_value={
                "status": "sent",
                "ok": True,
                "provider": "solapi",
                "groupId": "GROUP-UNCERTAIN",
                "messageId": "MESSAGE-UNCERTAIN",
                "smsDeliveryStatus": "accepted",
            }
        )
        remember_sms_delivery = Mock(return_value=False)
        deps = replace(
            DeviceHealthAlertActionRouteDeps(),
            load_exact_target=Mock(return_value=_exact_target()),
            send_sms=send_sms,
            remember_sms_delivery=remember_sms_delivery,
            now=lambda: datetime(2026, 8, 14, tzinfo=timezone.utc),
        )

        result = DeviceHealthAlertActionAssistantRoute(deps).handle(
            _request(DEVICE_HEALTH_ALERT_SMS_ACTION, include_sms=True)
        )

        assert result is not None
        self.assertEqual(result.outcome, "failed")
        self.assertEqual(
            result.fallback_reason,
            "sms_delivery_receipt_persist_failed",
        )
        send_sms.assert_called_once()
        remember_sms_delivery.assert_called_once()

    def test_voice_action_checks_mda_then_dispatches_exactly_once(self) -> None:
        get_mda = Mock(
            return_value={"version": "2.11.308", "deviceIsConnected": True}
        )
        send_command = Mock(return_value={"status": True, "affected": 1})
        claim = Mock(return_value={"claimed": True})
        deps = replace(
            DeviceHealthAlertActionRouteDeps(),
            load_exact_target=Mock(return_value=_exact_target()),
            get_mda_device=get_mda,
            send_mda_command=send_command,
            claim_voice_guide=claim,
        )

        result = DeviceHealthAlertActionAssistantRoute(deps).handle(
            _request(DEVICE_HEALTH_ALERT_VOICE_ACTION)
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.route, DEVICE_HEALTH_ALERT_VOICE_ROUTE)
        self.assertEqual(result.outcome, "answered")
        get_mda.assert_called_once_with("MB2-C00419")
        claim.assert_called_once()
        send_command.assert_called_once_with(
            "MB2-C00419",
            command="voice_guide",
        )

    def test_voice_unauthorized_or_timeout_never_retries_mutation(self) -> None:
        for failure in (
            RuntimeError("Unauthorized"),
            TimeoutError("timeout"),
        ):
            with self.subTest(failure=type(failure).__name__):
                send_command = Mock(side_effect=failure)
                deps = replace(
                    DeviceHealthAlertActionRouteDeps(),
                    load_exact_target=Mock(return_value=_exact_target()),
                    get_mda_device=Mock(
                        return_value={
                            "version": "2.11.308",
                            "deviceIsConnected": True,
                        }
                    ),
                    send_mda_command=send_command,
                    claim_voice_guide=Mock(return_value={"claimed": True}),
                )

                result = DeviceHealthAlertActionAssistantRoute(
                    deps,
                    logger=logging.getLogger("test.alert.voice"),
                ).handle(_request(DEVICE_HEALTH_ALERT_VOICE_ACTION))

                self.assertIsNotNone(result)
                assert result is not None
                self.assertEqual(result.outcome, "failed")
                self.assertEqual(
                    result.fallback_reason,
                    "voice_guide_dispatch_uncertain",
                )
                send_command.assert_called_once_with(
                    "MB2-C00419",
                    command="voice_guide",
                )

    def test_mark_done_does_not_call_mda_or_sms(self) -> None:
        get_mda = Mock()
        send_command = Mock()
        send_sms = Mock()
        deps = replace(
            DeviceHealthAlertActionRouteDeps(),
            load_exact_target=Mock(return_value=_exact_target()),
            get_mda_device=get_mda,
            send_mda_command=send_command,
            send_sms=send_sms,
        )

        result = DeviceHealthAlertActionAssistantRoute(deps).handle(
            _request(DEVICE_HEALTH_ALERT_MARK_DONE_ACTION)
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "answered")
        get_mda.assert_not_called()
        send_command.assert_not_called()
        send_sms.assert_not_called()

    def test_sms_transport_timeout_is_called_once_and_marked_uncertain(self) -> None:
        payload = {
            "sms": {
                "to": "01012345678",
                "message": "안내 문자",
            }
        }
        with (
            patch.object(company_settings, "DEVICE_HEALTH_MONITOR_SMS_PROVIDER", "solapi"),
            patch.object(company_settings, "SOLAPI_API_KEY", "test-key"),
            patch.object(company_settings, "SOLAPI_API_SECRET", "test-secret"),
            patch.object(company_settings, "SOLAPI_FROM_NUMBER", "01099998888"),
            patch.object(
                action_route,
                "_build_solapi_authorization_header",
                return_value="test-authorization",
            ),
            patch.object(
                action_route.requests,
                "post",
                side_effect=TimeoutError("timeout"),
            ) as post_mock,
            company_api_device_ssh_context() as mutation_state,
        ):
            result = action_route._send_device_health_alert_sms(
                payload,
                logger=logging.getLogger("test.alert.sms"),
            )

        self.assertEqual(result["smsDeliveryStatus"], "confirm_required")
        self.assertTrue(mutation_state.mutation_attempted)
        post_mock.assert_called_once()

    def test_sms_authorization_preflight_failure_is_not_marked_as_sent(
        self,
    ) -> None:
        payload = {
            "sms": {
                "to": "01012345678",
                "message": "안내 문자",
            }
        }
        with (
            patch.object(company_settings, "DEVICE_HEALTH_MONITOR_SMS_PROVIDER", "solapi"),
            patch.object(company_settings, "SOLAPI_API_KEY", "test-key"),
            patch.object(company_settings, "SOLAPI_API_SECRET", "test-secret"),
            patch.object(company_settings, "SOLAPI_FROM_NUMBER", "01099998888"),
            patch.object(
                action_route,
                "_build_solapi_authorization_header",
                side_effect=RuntimeError("header unavailable"),
            ),
            patch.object(action_route.requests, "post") as post_mock,
            company_api_device_ssh_context() as mutation_state,
        ):
            result = action_route._send_device_health_alert_sms(
                payload,
                logger=logging.getLogger("test.alert.sms.preflight"),
            )

        self.assertEqual(result["smsDeliveryStatus"], "request_failed")
        self.assertFalse(mutation_state.mutation_attempted)
        post_mock.assert_not_called()

    def test_view_auto_sms_modal_performs_no_db_mda_or_sms_call(self) -> None:
        item = {
            "hospital": "분당제일병원",
            "hospitalSeq": "31",
            "room": "2진료실",
            "device": "MB2-C00419",
            "issue": "캡처보드 연결 확인 필요",
            "smsPhoneNumber": "01012345678",
            "smsMessage": "이미 발송한 문자",
        }
        with (
            patch.object(
                device_health_monitor_reporter,
                "_lookup_device_health_monitor_hospital_contact",
            ) as db_mock,
            patch.object(
                device_health_monitor_reporter,
                "_post_device_health_monitor_sms_payload",
            ) as sms_mock,
            patch.object(
                device_health_monitor_reporter,
                "_dispatch_device_health_monitor_voice_guide",
            ) as mda_mock,
        ):
            view = device_health_monitor_reporter._build_device_health_monitor_sms_modal_view(
                item=item,
                actor_user_id="ACTOR-1",
                channel_id="CHANNEL-1",
                message_ts="100.1",
                thread_ts="100.1",
                mode="view_auto_sent",
            )

        self.assertEqual(
            view["callback_id"],
            "device_health_alert_auto_sms_view_modal",
        )
        db_mock.assert_not_called()
        sms_mock.assert_not_called()
        mda_mock.assert_not_called()

    def test_operation_route_allowlist_contains_all_action_results(self) -> None:
        self.assertTrue(
            {
                DEVICE_HEALTH_ALERT_SMS_ROUTE,
                DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
                DEVICE_HEALTH_ALERT_VOICE_ROUTE,
                DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
            }.issubset(company_operation_route_names())
        )


class DeviceHealthAlertActionApiContractTests(unittest.TestCase):
    def _turn_payload(self) -> dict[str, object]:
        return {
            "tenantId": "TENANT-1",
            "actorId": "ACTOR-1",
            "channel": "slack",
            "conversationId": "THREAD-1",
            "question": "typed action",
            "locale": "ko",
            "routeGroup": "operations",
            "operationAction": {
                "name": DEVICE_HEALTH_ALERT_SMS_ACTION,
                "phase": "execute",
                "target": {
                    "hospitalSeq": 31,
                    "hospitalName": "분당제일병원",
                    "roomName": "2진료실",
                    "deviceName": "MB2-C00419",
                    "issue": "캡처보드 연결 확인 필요",
                    "alertCategory": "video_signal",
                    "problemComponents": ["캡처보드"],
                },
                "sms": {
                    "phoneNumber": "01012345678",
                    "message": "안내 문자",
                },
            },
        }

    def test_schema_maps_typed_action_to_domain_metadata(self) -> None:
        turn = AssistantTurnInput.model_validate(self._turn_payload())
        request = turn.to_company_request("REQ-1")

        action = request.metadata["operation_action"]
        self.assertEqual(action["name"], DEVICE_HEALTH_ALERT_SMS_ACTION)
        self.assertEqual(action["phase"], "execute")
        self.assertEqual(action["target"]["device_name"], "MB2-C00419")
        self.assertEqual(action["sms"]["phone_number"], "01012345678")

    def test_schema_rejects_wrong_route_or_missing_sms(self) -> None:
        wrong_route = self._turn_payload()
        wrong_route["routeGroup"] = "structured"
        with self.assertRaises(ValidationError):
            AssistantTurnInput.model_validate(wrong_route)

        missing_sms = self._turn_payload()
        del missing_sms["operationAction"]["sms"]  # type: ignore[index]
        with self.assertRaises(ValidationError):
            AssistantTurnInput.model_validate(missing_sms)

    def test_schema_accepts_prepare_without_sms(self) -> None:
        prepare = self._turn_payload()
        prepare["operationAction"]["phase"] = "prepare"  # type: ignore[index]
        del prepare["operationAction"]["sms"]  # type: ignore[index]

        turn = AssistantTurnInput.model_validate(prepare)

        self.assertIsNone(turn.operationAction.sms)

    def test_policy_requires_specific_alert_action_capability(self) -> None:
        turn = AssistantTurnInput.model_validate(self._turn_payload())
        base_principal = CallerPrincipal(
            caller_id="slack-prod",
            tenant_ids=frozenset({"TENANT-1"}),
            channels=frozenset({"slack"}),
            actor_ids=frozenset({"ACTOR-1"}),
            allow_anonymous_actor=False,
            capabilities=frozenset(
                {"assistant.turn.read", "assistant.operation.execute"}
            ),
        )

        with self.assertRaises(CompanyApiProblem) as raised:
            authorize_turn(base_principal, turn, "REQ-1")
        self.assertEqual(raised.exception.status, 403)

        authorized = replace(
            base_principal,
            capabilities=frozenset(
                {
                    "assistant.turn.read",
                    "assistant.operation.execute",
                    "assistant.device.alert.execute",
                }
            ),
        )
        self.assertIs(authorize_turn(authorized, turn, "REQ-1"), authorized)

    def test_api_receipt_excludes_phone_message_and_ids_from_body(self) -> None:
        deps = replace(
            DeviceHealthAlertActionRouteDeps(),
            load_exact_target=Mock(return_value=_exact_target()),
            send_sms=Mock(
                return_value={
                    "status": "sent",
                    "ok": True,
                    "provider": "solapi",
                    "groupId": "GROUP-1",
                    "messageId": "MESSAGE-1",
                    "smsDeliveryStatus": "accepted",
                }
            ),
            remember_sms_delivery=Mock(return_value=True),
            now=lambda: datetime(2026, 8, 14, tzinfo=timezone.utc),
        )
        result = DeviceHealthAlertActionAssistantRoute(deps).handle(
            _request(DEVICE_HEALTH_ALERT_SMS_ACTION, include_sms=True)
        )
        assert result is not None

        payload = serialize_result(result, "REQ-ALERT-ACTION-1")

        self.assertEqual(payload["operationResult"]["groupId"], "GROUP-1")
        self.assertNotIn("01012345678", json.dumps(payload["operationResult"], ensure_ascii=False))
        self.assertNotIn("직접 작성한", json.dumps(payload["operationResult"], ensure_ascii=False))
        self.assertNotIn("GROUP-1", payload["messages"][0]["body"])

    def test_prepare_receipt_is_requester_private_and_not_in_body(self) -> None:
        result = DeviceHealthAlertActionAssistantRoute(
            replace(
                DeviceHealthAlertActionRouteDeps(),
                load_exact_target=Mock(return_value=_exact_target()),
            )
        ).handle(_request(DEVICE_HEALTH_ALERT_SMS_ACTION))
        assert result is not None

        payload = serialize_result(result, "REQ-ALERT-PREPARE-1")

        self.assertEqual(payload["messages"][0]["deliveryScope"], "requester")
        self.assertEqual(
            payload["operationResult"]["kind"],
            "sms_contact_preparation",
        )
        self.assertEqual(
            payload["operationResult"]["phoneNumber"],
            "01012345678",
        )
        self.assertNotIn("01012345678", payload["messages"][0]["body"])


if __name__ == "__main__":
    unittest.main()
