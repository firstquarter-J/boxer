import logging
import unittest
from unittest.mock import patch

import anthropic
import httpx

from boxer_company_adapter_slack.admin_routes import (
    AdminRoutesContext,
    AdminRoutesDeps,
    _handle_admin_routes,
)
from boxer_company_adapter_slack.barcode_query_routes import (
    BarcodeQueryRoutesContext,
    BarcodeQueryRoutesDeps,
    _handle_barcode_query_routes,
)
from boxer_company_adapter_slack.device_routes import (
    DeviceRoutesContext,
    DeviceRoutesDeps,
    _handle_device_routes,
)
from boxer_company_adapter_slack.knowledge_routes import (
    KnowledgeRoutesContext,
    KnowledgeRoutesDeps,
    _handle_knowledge_routes,
)
from boxer_company_adapter_slack.structured_routes import (
    StructuredRoutesContext,
    _handle_structured_routes,
)


def _payload() -> dict[str, object]:
    return {
        "text": "핑",
        "question": "핑",
        "user_id": "U123",
        "workspace_id": "W123",
        "channel_id": "C123",
        "current_ts": "1.1",
        "thread_ts": "1.0",
    }


def _anthropic_auth_error() -> anthropic.AuthenticationError:
    response = httpx.Response(
        401,
        request=httpx.Request("POST", "https://api.anthropic.com/v1/messages"),
    )
    return anthropic.AuthenticationError("invalid api key", response=response, body=None)


def _anthropic_credit_error() -> anthropic.RateLimitError:
    response = httpx.Response(
        429,
        request=httpx.Request("POST", "https://api.anthropic.com/v1/messages"),
    )
    body = {
        "type": "error",
        "error": {
            "type": "rate_limit_error",
            "message": "Your credit balance is too low",
        },
    }
    return anthropic.RateLimitError("rate limit", response=response, body=body)


def _anthropic_credit_bad_request_error() -> anthropic.BadRequestError:
    response = httpx.Response(
        400,
        request=httpx.Request("POST", "https://api.anthropic.com/v1/messages"),
    )
    body = {
        "type": "error",
        "error": {
            "type": "invalid_request_error",
            "message": "Your credit balance is too low to access the Anthropic API.",
        },
    }
    return anthropic.BadRequestError("bad request", response=response, body=body)


def _silent_logger() -> logging.Logger:
    logger = logging.getLogger(f"{__name__}.silent")
    logger.disabled = True
    return logger


class RouteModulesSmokeTests(unittest.TestCase):
    def test_admin_routes_returns_false_for_unrelated_question(self) -> None:
        handled = _handle_admin_routes(
            AdminRoutesContext(
                question="핑",
                payload=_payload(),  # type: ignore[arg-type]
                user_id="U123",
                thread_ts="1.0",
                reply=lambda *args, **kwargs: None,
                logger=logging.getLogger(__name__),
            ),
            AdminRoutesDeps(
                get_s3_client=lambda: None,
                reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
            ),
        )

        self.assertFalse(handled)

    def test_structured_routes_returns_false_for_unrelated_question(self) -> None:
        handled = _handle_structured_routes(
            StructuredRoutesContext(
                question="핑",
                barcode=None,
                payload=_payload(),  # type: ignore[arg-type]
                thread_ts="1.0",
                reply=lambda *args, **kwargs: None,
                logger=logging.getLogger(__name__),
            )
        )

        self.assertFalse(handled)

    def test_structured_routes_skips_recording_streaming_restore_question(self) -> None:
        replies: list[str] = []

        with patch(
            "boxer_company_adapter_slack.structured_routes._query_recordings_by_filters",
            return_value="잘못된 영상 조회 응답",
        ) as recordings_query_mock:
            handled = _handle_structured_routes(
                StructuredRoutesContext(
                    question="35033165423 2024년 4월 영상 복원",
                    barcode="35033165423",
                    payload=_payload(),  # type: ignore[arg-type]
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    logger=logging.getLogger(__name__),
                )
            )

        self.assertFalse(handled)
        self.assertEqual(replies, [])
        recordings_query_mock.assert_not_called()

    def test_barcode_query_routes_returns_false_for_unrelated_question(self) -> None:
        handled = _handle_barcode_query_routes(
            BarcodeQueryRoutesContext(
                question="핑",
                barcode=None,
                user_id="U123",
                thread_ts="1.0",
                reply=lambda *args, **kwargs: None,
                logger=logging.getLogger(__name__),
            ),
            BarcodeQueryRoutesDeps(
                get_recordings_context=lambda: {},
                attach_recordings_context_to_evidence=lambda evidence, context: None,
                reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
            ),
        )

        self.assertFalse(handled)

    def test_barcode_query_routes_handles_validation_status_question(self) -> None:
        replies: list[str] = []

        with patch(
            "boxer_company_adapter_slack.barcode_query_routes._query_barcode_validation_status",
            return_value="*바코드 유효성 검사 확인*\n• 결론: 테스트",
        ):
            handled = _handle_barcode_query_routes(
                BarcodeQueryRoutesContext(
                    question="10255657857 이건 유효성 검사에 걸리는 바코드냐",
                    barcode="10255657857",
                    user_id="U123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    logger=logging.getLogger(__name__),
                ),
                BarcodeQueryRoutesDeps(
                    get_recordings_context=lambda: {},
                    attach_recordings_context_to_evidence=lambda evidence, context: None,
                    reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
                ),
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["*바코드 유효성 검사 확인*\n• 결론: 테스트"])

    def test_barcode_query_routes_handles_pink_classification_reason_question(self) -> None:
        replies: list[str] = []

        with patch(
            "boxer_company_adapter_slack.barcode_query_routes._query_barcode_pink_classification_reason",
            return_value="*핑크/환불 바코드 분류 근거*\n• 판단: 테스트",
        ):
            handled = _handle_barcode_query_routes(
                BarcodeQueryRoutesContext(
                    question="58291583958 왜 핑크바코드로 분류되지 않았어?",
                    barcode="58291583958",
                    user_id="U123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    logger=logging.getLogger(__name__),
                ),
                BarcodeQueryRoutesDeps(
                    get_recordings_context=lambda: {},
                    attach_recordings_context_to_evidence=lambda evidence, context: None,
                    reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
                ),
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["*핑크/환불 바코드 분류 근거*\n• 판단: 테스트"])

    def test_barcode_query_routes_handles_recording_streaming_restore_request(self) -> None:
        replies: list[str] = []

        with (
            patch("boxer_company_adapter_slack.barcode_query_routes.cs.RECORDING_STREAMING_RESTORE_ENABLED", True),
            patch(
                "boxer_company_adapter_slack.barcode_query_routes.cs.RECORDING_STREAMING_RESTORE_ALLOWED_USER_IDS",
                {"U123"},
            ),
            patch(
                "boxer_company_adapter_slack.barcode_query_routes._query_recording_streaming_restore_by_barcode_month",
                return_value="*스트리밍 종료 영상 복원 결과*\n• 결과: 테스트",
            ) as restore_mock,
        ):
            handled = _handle_barcode_query_routes(
                BarcodeQueryRoutesContext(
                    question="35033165423 2024년 4월 영상 블라인드를 해제해줘",
                    barcode="35033165423",
                    user_id="U123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    logger=logging.getLogger(__name__),
                ),
                BarcodeQueryRoutesDeps(
                    get_recordings_context=lambda: {},
                    attach_recordings_context_to_evidence=lambda evidence, context: None,
                    reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
                    resolve_user_name=lambda user_id: "Rosa",
                ),
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["*스트리밍 종료 영상 복원 결과*\n• 결과: 테스트"])
        restore_mock.assert_called_once_with(
            "35033165423",
            "35033165423 2024년 4월 영상 블라인드를 해제해줘",
            requester="U123",
            requester_name="Rosa",
        )

    def test_device_routes_returns_false_for_unrelated_question(self) -> None:
        handled = _handle_device_routes(
            DeviceRoutesContext(
                question="핑",
                barcode=None,
                phase2_hospital_name=None,
                phase2_room_name=None,
                payload=_payload(),  # type: ignore[arg-type]
                user_id="U123",
                workspace_id="W123",
                channel_id="C123",
                thread_ts="1.0",
                reply=lambda *args, **kwargs: None,
                client=None,
                logger=logging.getLogger(__name__),
            ),
            DeviceRoutesDeps(
                get_s3_client=lambda: None,
                get_recordings_context=lambda: {},
                has_recordings_device_mapping=lambda context: False,
                send_dm_message=lambda user_id, text: False,
                build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
                reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
            ),
        )

        self.assertFalse(handled)

    def test_device_routes_handles_led_pattern_help_before_freeform(self) -> None:
        replies: list[str] = []
        synth_calls: list[tuple[str, dict[str, object], str]] = []

        with patch(
            "boxer_company_adapter_slack.device_routes._select_notion_references",
            return_value=[{"title": "LED playbook"}],
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="LED 증상은 어떨 때 나타나?",
                    barcode=None,
                    phase2_hospital_name=None,
                    phase2_room_name=None,
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U123",
                    workspace_id="W123",
                    channel_id="C123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    client=None,
                    logger=logging.getLogger(__name__),
                ),
                DeviceRoutesDeps(
                    get_s3_client=lambda: None,
                    get_recordings_context=lambda: {},
                    has_recordings_device_mapping=lambda context: False,
                    send_dm_message=lambda user_id, text: False,
                    build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
                    reply_with_retrieval_synthesis=lambda fallback_text, evidence_payload, route_name, **kwargs: synth_calls.append(
                        (fallback_text, evidence_payload, route_name)
                    ),
                ),
            )

        self.assertTrue(handled)
        self.assertEqual(replies, [])
        self.assertEqual(len(synth_calls), 1)
        self.assertIn("LED 증상 안내", synth_calls[0][0])
        self.assertEqual(synth_calls[0][2], "device led pattern guide")
        self.assertIn("notionPlaybooks", synth_calls[0][1])

    def test_device_routes_handles_remote_access_probe_before_freeform(self) -> None:
        replies: list[str] = []

        with (
            patch("boxer_company_adapter_slack.device_routes.cs.MDA_GRAPHQL_URL", "https://mda.example/graphql"),
            patch("boxer_company_adapter_slack.device_routes.cs.MDA_ADMIN_USER_PASSWORD", "secret"),
            patch(
                "boxer_company_adapter_slack.device_routes._probe_device_remote_access",
                return_value=("*장비 원격 접속 점검*\n• 판단: 테스트", {"route": "device_remote_access_probe"}),
            ),
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="MB2-C00419 ssh 연결 안 돼",
                    barcode=None,
                    phase2_hospital_name=None,
                    phase2_room_name=None,
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U123",
                    workspace_id="W123",
                    channel_id="C123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    client=None,
                    logger=logging.getLogger(__name__),
                ),
                DeviceRoutesDeps(
                    get_s3_client=lambda: None,
                    get_recordings_context=lambda: {},
                    has_recordings_device_mapping=lambda context: False,
                    send_dm_message=lambda user_id, text: False,
                    build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
                    reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
                ),
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["*장비 원격 접속 점검*\n• 판단: 테스트"])

    def test_device_routes_handles_connected_status_probe_before_freeform(self) -> None:
        replies: list[str] = []

        with (
            patch("boxer_company_adapter_slack.device_routes.cs.MDA_GRAPHQL_URL", "https://mda.example/graphql"),
            patch("boxer_company_adapter_slack.device_routes.cs.MDA_ADMIN_USER_PASSWORD", "secret"),
            patch("boxer_company_adapter_slack.device_routes.cs.DEVICE_SSH_PASSWORD", "secret"),
            patch(
                "boxer_company_adapter_slack.device_routes._probe_device_status_overview",
                return_value=("*장비 상태 점검*\n• 판단: 테스트", {"route": "device_status_probe"}),
            ),
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="MB2-C00072 장비연결상태 확인",
                    barcode=None,
                    phase2_hospital_name=None,
                    phase2_room_name=None,
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U123",
                    workspace_id="W123",
                    channel_id="C123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    client=None,
                    logger=logging.getLogger(__name__),
                ),
                DeviceRoutesDeps(
                    get_s3_client=lambda: None,
                    get_recordings_context=lambda: {},
                    has_recordings_device_mapping=lambda context: False,
                    send_dm_message=lambda user_id, text: False,
                    build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
                    reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
                ),
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["*장비 상태 점검*\n• 판단: 테스트"])

    def test_device_routes_handles_power_off_before_freeform(self) -> None:
        replies: list[str] = []

        with (
            patch("boxer_company_adapter_slack.device_routes.cs.MDA_GRAPHQL_URL", "https://mda.example/graphql"),
            patch("boxer_company_adapter_slack.device_routes.cs.MDA_ADMIN_USER_PASSWORD", "secret"),
            patch("boxer_company_adapter_slack.device_routes.cs.DEVICE_SSH_PASSWORD", "secret"),
            patch(
                "boxer_company_adapter_slack.device_routes._request_device_power_off",
                return_value=("*장비 전원 종료*\n• 결과: 완료", {"route": "device_power_off", "dispatch": {"status": True}}),
            ),
            patch(
                "boxer_company_adapter_slack.device_routes._log_device_update_activity",
                return_value=True,
            ),
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="MB2-C00419 장비 종료",
                    barcode=None,
                    phase2_hospital_name=None,
                    phase2_room_name=None,
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U123",
                    workspace_id="W123",
                    channel_id="C123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    client=None,
                    logger=logging.getLogger(__name__),
                ),
                DeviceRoutesDeps(
                    get_s3_client=lambda: None,
                    get_recordings_context=lambda: {},
                    has_recordings_device_mapping=lambda context: False,
                    send_dm_message=lambda user_id, text: False,
                    build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
                    reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
                ),
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["*장비 전원 종료*\n• 결과: 완료"])

    def test_device_routes_handles_device_log_upload_check_before_freeform(self) -> None:
        replies: list[str] = []

        with (
            patch("boxer_company_adapter_slack.device_routes.s.S3_QUERY_ENABLED", True),
            patch(
                "boxer_company_adapter_slack.device_routes._check_and_request_device_log_upload",
                return_value=("*장비 로그 업로드 확인*\n• 결과: 테스트", {"route": "device_log_upload_check"}),
            ),
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="MB2-C00419 로그 업로드 확인해줘",
                    barcode=None,
                    phase2_hospital_name=None,
                    phase2_room_name=None,
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U123",
                    workspace_id="W123",
                    channel_id="C123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    client=None,
                    logger=logging.getLogger(__name__),
                ),
                DeviceRoutesDeps(
                    get_s3_client=lambda: None,
                    get_recordings_context=lambda: {},
                    has_recordings_device_mapping=lambda context: False,
                    send_dm_message=lambda user_id, text: False,
                    build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
                    reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
                ),
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["*장비 로그 업로드 확인*\n• 결과: 테스트"])

    def test_device_routes_handles_hospital_room_log_upload_check(self) -> None:
        replies: list[str] = []

        with (
            patch("boxer_company_adapter_slack.device_routes.s.S3_QUERY_ENABLED", True),
            patch(
                "boxer_company_adapter_slack.device_routes._lookup_device_contexts_by_hospital_room",
                return_value=[
                    {
                        "deviceName": "MB2-C00419",
                        "hospitalName": "분당서울여성의원(성남)",
                        "roomName": "초음파실1",
                    }
                ],
            ),
            patch(
                "boxer_company_adapter_slack.device_routes._check_and_request_device_log_upload",
                return_value=("*장비 로그 업로드 확인*\n• 결과: 테스트", {"route": "device_log_upload_check"}),
            ),
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="분당서울여성의원(성남) / 초음파실1 로그 업로드 확인",
                    barcode=None,
                    phase2_hospital_name=None,
                    phase2_room_name=None,
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U123",
                    workspace_id="W123",
                    channel_id="C123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    client=None,
                    logger=logging.getLogger(__name__),
                ),
                DeviceRoutesDeps(
                    get_s3_client=lambda: None,
                    get_recordings_context=lambda: {},
                    has_recordings_device_mapping=lambda context: False,
                    send_dm_message=lambda user_id, text: False,
                    build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
                    reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
                ),
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["*장비 로그 업로드 확인*\n• 결과: 테스트"])

    def test_device_routes_recovers_hospital_room_log_upload_scope_from_thread(self) -> None:
        replies: list[str] = []

        with (
            patch("boxer_company_adapter_slack.device_routes.s.S3_QUERY_ENABLED", True),
            patch(
                "boxer_company_adapter_slack.device_routes._load_slack_thread_context",
                return_value="U1: 분당서울여성의원(성남) / 초음파실1 / 마미박스/전원",
            ),
            patch(
                "boxer_company_adapter_slack.device_routes._lookup_device_contexts_by_hospital_room",
                return_value=[
                    {
                        "deviceName": "MB2-C00419",
                        "hospitalName": "분당서울여성의원(성남)",
                        "roomName": "초음파실1",
                    }
                ],
            ),
            patch(
                "boxer_company_adapter_slack.device_routes._check_and_request_device_log_upload",
                return_value=("*장비 로그 업로드 확인*\n• 결과: 테스트", {"route": "device_log_upload_check"}),
            ),
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="4월 11일 로그 업로드 확인해줘",
                    barcode=None,
                    phase2_hospital_name=None,
                    phase2_room_name=None,
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U123",
                    workspace_id="W123",
                    channel_id="C123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    client=None,
                    logger=logging.getLogger(__name__),
                ),
                DeviceRoutesDeps(
                    get_s3_client=lambda: None,
                    get_recordings_context=lambda: {},
                    has_recordings_device_mapping=lambda context: False,
                    send_dm_message=lambda user_id, text: False,
                    build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
                    reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
                ),
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["*장비 로그 업로드 확인*\n• 결과: 테스트"])

    def test_knowledge_routes_returns_false_when_no_route_matches(self) -> None:
        with patch("boxer_company_adapter_slack.knowledge_routes.s.LLM_PROVIDER", ""):
            handled = _handle_knowledge_routes(
                KnowledgeRoutesContext(
                    question="핑",
                    barcode=None,
                    user_id="U123",
                    payload=_payload(),  # type: ignore[arg-type]
                    thread_ts="",
                    channel_id="C123",
                    current_ts="1.1",
                    reply=lambda *args, **kwargs: None,
                    logger=logging.getLogger(__name__),
                    client=None,
                    claude_client=None,
                ),
                KnowledgeRoutesDeps(
                    reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
                    timeout_reply_text=lambda: "timeout",
                    llm_unavailable_reply_text=lambda summary=None: "down",
                    is_timeout_error=lambda exc: False,
                    is_claude_allowed_user=lambda user_id: True,
                    build_barcode_fallback_evidence=lambda: None,
                ),
            )

        self.assertFalse(handled)

    def test_knowledge_routes_reports_missing_claude_api_key_when_client_unavailable(self) -> None:
        replies: list[str] = []

        with patch("boxer_company_adapter_slack.knowledge_routes.s.LLM_PROVIDER", "claude"):
            handled = _handle_knowledge_routes(
                KnowledgeRoutesContext(
                    question="아직도 조회 불가능하니?",
                    barcode=None,
                    user_id="U123",
                    payload=_payload(),  # type: ignore[arg-type]
                    thread_ts="",
                    channel_id="C123",
                    current_ts="1.1",
                    reply=lambda text, **kwargs: replies.append(text),
                    logger=_silent_logger(),
                    client=None,
                    claude_client=None,
                ),
                KnowledgeRoutesDeps(
                    reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
                    timeout_reply_text=lambda: "timeout",
                    llm_unavailable_reply_text=lambda summary=None: "down",
                    is_timeout_error=lambda exc: False,
                    is_claude_allowed_user=lambda user_id: True,
                    build_barcode_fallback_evidence=lambda: None,
                ),
            )

        self.assertTrue(handled)
        self.assertEqual(len(replies), 1)
        self.assertIn("인증값이 설정되지 않아", replies[0])
        self.assertNotIn("Claude", replies[0])
        self.assertIn("ANTHROPIC_API_KEY", replies[0])
        self.assertIn("ANTHROPIC_AUTH_TOKEN", replies[0])

    def test_knowledge_routes_reports_claude_auth_error_as_api_key_issue(self) -> None:
        replies: list[str] = []

        with (
            patch("boxer_company_adapter_slack.knowledge_routes.s.LLM_PROVIDER", "claude"),
            patch("boxer_company_adapter_slack.knowledge_routes._load_slack_thread_context", return_value=""),
            patch(
                "boxer_company_adapter_slack.knowledge_routes._ask_claude",
                side_effect=_anthropic_auth_error(),
            ),
        ):
            handled = _handle_knowledge_routes(
                KnowledgeRoutesContext(
                    question="아직도 조회 불가능하니?",
                    barcode=None,
                    user_id="U123",
                    payload=_payload(),  # type: ignore[arg-type]
                    thread_ts="1.0",
                    channel_id="C123",
                    current_ts="1.1",
                    reply=lambda text, **kwargs: replies.append(text),
                    logger=_silent_logger(),
                    client=None,
                    claude_client=object(),
                ),
                KnowledgeRoutesDeps(
                    reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
                    timeout_reply_text=lambda: "timeout",
                    llm_unavailable_reply_text=lambda summary=None: "down",
                    is_timeout_error=lambda exc: False,
                    is_claude_allowed_user=lambda user_id: True,
                    build_barcode_fallback_evidence=lambda: None,
                ),
            )

        self.assertTrue(handled)
        self.assertEqual(len(replies), 1)
        self.assertIn("인증값이 유효하지 않아", replies[0])
        self.assertNotIn("Claude", replies[0])
        self.assertNotIn("AI 응답 중 오류", replies[0])

    def test_knowledge_routes_reports_claude_credit_error_as_payment_issue(self) -> None:
        replies: list[str] = []

        with (
            patch("boxer_company_adapter_slack.knowledge_routes.s.LLM_PROVIDER", "claude"),
            patch("boxer_company_adapter_slack.knowledge_routes._load_slack_thread_context", return_value=""),
            patch(
                "boxer_company_adapter_slack.knowledge_routes._ask_claude",
                side_effect=_anthropic_credit_error(),
            ),
        ):
            handled = _handle_knowledge_routes(
                KnowledgeRoutesContext(
                    question="아직도 조회 불가능하니?",
                    barcode=None,
                    user_id="U123",
                    payload=_payload(),  # type: ignore[arg-type]
                    thread_ts="1.0",
                    channel_id="C123",
                    current_ts="1.1",
                    reply=lambda text, **kwargs: replies.append(text),
                    logger=_silent_logger(),
                    client=None,
                    claude_client=object(),
                ),
                KnowledgeRoutesDeps(
                    reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
                    timeout_reply_text=lambda: "timeout",
                    llm_unavailable_reply_text=lambda summary=None: "down",
                    is_timeout_error=lambda exc: False,
                    is_claude_allowed_user=lambda user_id: True,
                    build_barcode_fallback_evidence=lambda: None,
                ),
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["토큰이 충전되지 않아 답변할 수 없어. 추가 결제가 필요해."])
        self.assertNotIn("Claude", replies[0])
        self.assertNotIn("ANTHROPIC_API_KEY", replies[0])

    def test_knowledge_routes_reports_claude_credit_bad_request_as_payment_issue(self) -> None:
        replies: list[str] = []

        with (
            patch("boxer_company_adapter_slack.knowledge_routes.s.LLM_PROVIDER", "claude"),
            patch("boxer_company_adapter_slack.knowledge_routes._load_slack_thread_context", return_value=""),
            patch(
                "boxer_company_adapter_slack.knowledge_routes._ask_claude",
                side_effect=_anthropic_credit_bad_request_error(),
            ),
        ):
            handled = _handle_knowledge_routes(
                KnowledgeRoutesContext(
                    question="마크 분석해봐",
                    barcode=None,
                    user_id="U123",
                    payload=_payload(),  # type: ignore[arg-type]
                    thread_ts="1.0",
                    channel_id="C123",
                    current_ts="1.1",
                    reply=lambda text, **kwargs: replies.append(text),
                    logger=_silent_logger(),
                    client=None,
                    claude_client=object(),
                ),
                KnowledgeRoutesDeps(
                    reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
                    timeout_reply_text=lambda: "timeout",
                    llm_unavailable_reply_text=lambda summary=None: "down",
                    is_timeout_error=lambda exc: False,
                    is_claude_allowed_user=lambda user_id: True,
                    build_barcode_fallback_evidence=lambda: None,
                ),
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["토큰이 충전되지 않아 답변할 수 없어. 추가 결제가 필요해."])
        self.assertNotIn("AI 응답 중 오류", replies[0])


if __name__ == "__main__":
    unittest.main()
