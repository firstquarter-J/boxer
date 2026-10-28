from __future__ import annotations

import logging
import os
import unittest
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock, patch

from boxer_company import team_chat_context
from boxer_company.assistant import (
    AssistantMessage,
    CompanyAssistantResult,
)
from boxer_company.base_access import BaseAccessSeedMember, LocalFileBaseAccessStore
from boxer_company_adapter_slack import company
from boxer_company_adapter_slack.access_routes import (
    BASE_ACCESS_DENIED_REPLY,
    BASE_ACCESS_HELP_REPLY,
    SlackBaseAccessRuntime,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiClientSettings,
)

_TOKEN = "service-token-" + ("x" * 40)
_READ_STAGES = (
    "notion",
    "device",
    "failure",
    "log",
    "structured",
    "barcode",
    "knowledge",
)


def _result(
    route: str,
    *,
    outcome: str = "answered",
    body: str = "API 답변",
    fallback_reason: str | None = None,
    operation_result: dict[str, Any] | None = None,
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route=route,
        outcome=outcome,  # type: ignore[arg-type]
        messages=(AssistantMessage(body=body),),
        fallback_reason=fallback_reason,
        operation_result=operation_result,
    )


class _ReadService:
    def __init__(self, result: CompanyAssistantResult | None) -> None:
        self.result = result
        self.calls: list[Any] = []

    def answer(self, request: Any) -> CompanyAssistantResult | None:
        self.calls.append(request)
        return self.result


class _OperationService:
    def __init__(
        self,
        result: CompanyAssistantResult | None,
    ) -> None:
        self.result = result
        self.answer_calls: list[Any] = []
        self.progress_calls: list[Any] = []

    def answer(self, request: Any) -> CompanyAssistantResult | None:
        self.answer_calls.append(request)
        action = dict(request.metadata).get("operation_action")
        if isinstance(action, dict) and action.get("name"):
            # 일반 knowledge route 전에 실행하는 API snapshot probe miss다.
            return _result(
                "device_diagnostic_followup",
                outcome="no_evidence",
                fallback_reason="diagnostic_snapshot_missing",
            )
        return self.result

    def answer_with_progress(
        self,
        request: Any,
        on_partial_result: Any,
    ) -> CompanyAssistantResult | None:
        self.progress_calls.append(request)
        return self.result


@dataclass
class _Invocation:
    replies: list[tuple[str, dict[str, Any]]]
    payload: dict[str, Any]
    read_services: dict[str, _ReadService]
    operation_service: _OperationService
    api_client: Mock
    render: Mock
    render_download: Mock
    context_loader: Mock


class CompanyRouteContractTests(unittest.TestCase):
    def _invoke_mention(
        self,
        *,
        question: str = "일반 질문",
        read_stage: str | None = None,
        read_result: CompanyAssistantResult | None = None,
        operation_route: str | None = None,
        operation_stage: str | None = None,
        operation_result: CompanyAssistantResult | None = None,
        access_allowed: bool = True,
        render_error: Exception | None = None,
        operation_ack_error: Exception | None = None,
        download_ack_error: Exception | None = None,
        raw_text: str = "",
        actor_user_id: str = "U1",
        current_ts: str = "1785312000.000001",
        base_access_runtime: SlackBaseAccessRuntime | None = None,
        slack_client: Any = None,
        channel_id: str = "C123456",
        message_event: bool = False,
        use_real_context_loader: bool = False,
    ) -> _Invocation:
        settings = CompanyApiClientSettings(
            base_url="http://127.0.0.1:8010",
            token=_TOKEN,
            automation_tenant_id="lifex",
        )
        read_services = {
            stage: _ReadService(
                read_result if stage == read_stage else None
            )
            for stage in _READ_STAGES
        }
        operation_service = _OperationService(operation_result)
        api_client = Mock(name="company_api_client")
        api_client.acknowledge_request_log_delivery.return_value = _result(
            "request_log_delivery"
        )
        api_client.acknowledge_device_operation_delivery.return_value = (
            _result("device_operation_delivery")
        )
        api_client.acknowledge_device_file_download.return_value = _result(
            "device_file_download_activity"
        )
        if operation_ack_error is not None:
            api_client.acknowledge_device_operation_delivery.side_effect = (
                operation_ack_error
            )
        if download_ack_error is not None:
            api_client.acknowledge_device_file_download.side_effect = (
                download_ack_error
            )

        replies: list[tuple[str, dict[str, Any]]] = []
        payload: dict[str, Any] = {
            "raw_text": raw_text,
            "text": question,
            "question": question,
            "user_id": actor_user_id,
            "workspace_id": "T1",
            "channel_id": channel_id,
            "current_ts": current_ts,
            "thread_ts": "1785312000.000001",
            "request_log": {
                "request_key": "slack:T1:C123456:1785312000.000001",
                "reply_count": 0,
            },
        }

        def reply(text: str, **kwargs: Any) -> None:
            replies.append((text, kwargs))
            payload["request_log"]["reply_count"] += 1

        def render_side_effect(
            result: CompanyAssistantResult,
            **_kwargs: Any,
        ) -> int:
            if render_error is not None:
                raise render_error
            _kwargs["reply"](result.messages[0].body)
            return 1

        captured_handlers: dict[str, Any] = {}

        def create_app(mention: Any, message: Any) -> Any:
            captured_handlers["mention"] = mention
            captured_handlers["message"] = message
            return SimpleNamespace(client=Mock())

        render = Mock(side_effect=render_side_effect)
        render_download = Mock(return_value=True)
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(company, "_validate_ec2_runtime_aws_env")
            )
            stack.enter_context(
                patch.object(
                    company,
                    "load_company_api_client_settings",
                    return_value=settings,
                )
            )
            stack.enter_context(
                patch.dict(
                    os.environ,
                    {"BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED": "true"},
                    clear=False,
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "validate_automation_delivery_journal_preflight",
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "build_slack_base_access_runtime",
                    return_value=base_access_runtime or SimpleNamespace(
                        is_allowed=Mock(return_value=access_allowed)
                    ),
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "CompanyAssistantApiClient",
                    return_value=api_client,
                )
            )
            stack.enter_context(
                patch.object(company, "CompanyAutomationApiClient")
            )
            stack.enter_context(
                patch.object(
                    company,
                    "_build_remote_read_service",
                    return_value=read_services,
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "wrap_company_operations_service",
                    return_value=operation_service,
                )
            )
            stack.enter_context(
                patch.object(company, "DeviceHealthAlertApiBridge")
            )
            stack.enter_context(
                patch.object(
                    company,
                    "build_hpa_change_remote_routes_config",
                    return_value=SimpleNamespace(enabled=False),
                )
            )
            stack.enter_context(
                patch.object(company, "create_slack_app", side_effect=create_app)
            )
            for name in (
                "attach_weekly_recordings_reporter",
                "attach_device_health_monitor_reporter",
                "attach_device_notification_alert_reporter",
                "attach_daily_device_round_reporter",
            ):
                stack.enter_context(patch.object(company, name))
            stack.enter_context(
                patch.object(
                    company,
                    "_handle_hpa_change_request",
                    return_value=False,
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "_handle_security_review_request",
                    return_value=False,
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "_is_device_scanner_abi_patch_intent",
                    return_value=False,
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "needs_device_file_operation_context",
                    return_value=False,
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "match_company_operation_route",
                    return_value=operation_route,
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "company_operation_legacy_stage",
                    return_value=operation_stage,
                )
            )
            context_loader = stack.enter_context(
                patch.object(
                    company,
                    "load_slack_thread_context_entries",
                    **({"wraps": company.load_slack_thread_context_entries}
                       if use_real_context_loader else {"return_value": ()}),
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "_load_slack_user_name",
                    return_value="Tester",
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "_load_slack_permalink",
                    return_value=(
                        "https://workspace.slack.com/archives/"
                        "C123456/p1785312000000001"
                    ),
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "render_company_assistant_result",
                    render,
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "render_device_file_download_delivery",
                    render_download,
                )
            )

            company.create_app()
            captured_handlers["message" if message_event else "mention"](
                payload,
                reply,
                slack_client if slack_client is not None else Mock(),
                logging.getLogger(f"{__name__}.route"),
            )

        return _Invocation(
            replies=replies,
            payload=payload,
            read_services=read_services,
            operation_service=operation_service,
            api_client=api_client,
            render=render,
            render_download=render_download,
            context_loader=context_loader,
        )

    def test_membership_denial_is_terminal_before_remote_services(self) -> None:
        invocation = self._invoke_mention(access_allowed=False)

        self.assertEqual(invocation.replies[0][0], BASE_ACCESS_DENIED_REPLY)
        self.assertTrue(
            all(not service.calls for service in invocation.read_services.values())
        )
        self.assertFalse(invocation.operation_service.answer_calls)

    def test_access_guidance_stops_before_remote_api_or_llm(self) -> None:
        for question in (
            "<@U2> 로그 조회 권한 부여",
            "<@U2> 장비 조회 권한 부여",
            "<@U2> 로그 조회 권한 주라고. 무슨 권한을 부여했다고 답했니?",
        ):
            with self.subTest(question=question):
                invocation = self._invoke_mention(
                    question=question,
                    raw_text=f"<@U08MWNAD5TM> {question}",
                    read_stage="knowledge",
                    read_result=_result("company_freeform", body="권한 부여했어"),
                )
                self.assertEqual(invocation.replies, [(BASE_ACCESS_HELP_REPLY, {})])
                self.assertEqual(
                    invocation.payload["request_log"]["route_name"],
                    "base_access_management",
                )
                self.assertTrue(
                    all(not service.calls for service in invocation.read_services.values())
                )
                self.assertFalse(invocation.operation_service.answer_calls)
                self.assertFalse(invocation.operation_service.progress_calls)
                self.assertFalse(invocation.api_client.mock_calls)

    def test_grant_persists_and_next_log_query_passes_without_restart(self) -> None:
        # 사용자 대화 전체를 실제 파일 정본과 Slack 진입점으로 재현하고 외부 호출만 mock한다.
        hyun_id = "U0629HDSJHG"
        target_id = "U07L4R7DWAU"
        boxer_id = "U08MWNAD5TM"
        log_question = "54262656384 2026-10-08 로그 분석"
        with (
            TemporaryDirectory() as directory,
            patch.object(company.cs, "HYUN_USER_ID", hyun_id),
        ):
            path = Path(directory) / "access.json"
            store = LocalFileBaseAccessStore(state_path=path)
            store.seed_members(
                "T1", [BaseAccessSeedMember(user_id=hyun_id, display_name="Hyun")], hyun_id,
            )
            runtime = SlackBaseAccessRuntime(
                store=store, logger=logging.getLogger(__name__),
            )
            client = Mock()
            client.auth_test.return_value = {"team_id": "T1", "user_id": boxer_id}
            client.users_info.return_value = {
                "user": {
                    "id": target_id, "team_id": "T1", "profile": {"display_name": "Beau"},
                },
            }

            def invoke(
                question: str,
                *,
                actor: str = target_id,
                ts: str = "1785312000.000001",
            ) -> _Invocation:
                return self._invoke_mention(
                    question=question,
                    raw_text=f"<@{boxer_id}> {question}",
                    actor_user_id=actor,
                    current_ts=ts,
                    base_access_runtime=runtime,
                    slack_client=client,
                    read_stage="log",
                    read_result=_result("barcode_log_analysis", body="로그 분석 결과"),
                )

            denied = invoke(log_question)
            self.assertEqual(denied.replies, [(BASE_ACCESS_DENIED_REPLY, {})])
            self.assertFalse(denied.read_services["log"].calls)
            grant = invoke(f"<@{target_id}> 권한 부여", actor=hyun_id)
            self.assertEqual(grant.replies, [(f"<@{target_id}> 박서 사용을 허용했어", {})])
            self.assertFalse(grant.api_client.mock_calls)
            self.assertTrue(
                LocalFileBaseAccessStore(state_path=path).is_allowed("T1", target_id)
            )

            allowed = invoke(log_question)
            self.assertEqual(allowed.replies, [("로그 분석 결과", {})])
            self.assertEqual(len(allowed.read_services["log"].calls), 1)

            # 권한 부여 뒤 기능명을 섞어 자기 상태를 물어도 membership만 읽고 끝낸다.
            granted_state = path.read_bytes()
            for question in (
                "나한테 무슨 권한이 있니?",
                "내가 장비의 로그 조회 권한 있는지 확인",
            ):
                status = invoke(question)
                self.assertEqual(status.replies, [(
                    "박서 사용 권한이 있어. 장비 조회와 로그 조회·분석을 요청할 수 있어", {},
                )])
                self.assertEqual(status.payload["request_log"]["route_name"], "base_access_status")
                self.assertFalse(status.api_client.mock_calls)
                self.assertFalse(status.operation_service.answer_calls)
                self.assertFalse(status.operation_service.progress_calls)
                self.assertTrue(all(not service.calls for service in status.read_services.values()))
                self.assertEqual(path.read_bytes(), granted_state)
            invoke(f"<@{target_id}> 권한 회수", actor=hyun_id, ts="1785312001.000001")
            stale = invoke(f"<@{target_id}> 권한 부여", actor=hyun_id)
            self.assertIn("현재 상태는 박서 사용 불가", stale.replies[0][0])
            self.assertFalse(
                LocalFileBaseAccessStore(state_path=path).is_allowed("T1", target_id)
            )
            self.assertEqual(invoke(log_question).replies, [(BASE_ACCESS_DENIED_REPLY, {})])
            self.assertEqual(
                invoke("나한테 무슨 권한이 있니?").replies,
                [(BASE_ACCESS_DENIED_REPLY, {})],
            )
            self.assertEqual(
                invoke("내가 장비의 로그 조회 권한 있는지 확인").replies,
                [(BASE_ACCESS_DENIED_REPLY, {})],
            )

    def test_remote_read_stages_keep_existing_order(self) -> None:
        route_by_stage = {
            "notion": "company_notion_search",
            "device": "device_led_log",
            "failure": "recording_failure_analysis",
            "log": "barcode_log_analysis",
            "structured": "hospitals_filter",
            "barcode": "barcode_video_count",
            "knowledge": "company_freeform",
        }
        for stage, route in route_by_stage.items():
            with self.subTest(stage=stage):
                invocation = self._invoke_mention(
                    read_stage=stage,
                    read_result=_result(route),
                )
                stage_index = _READ_STAGES.index(stage)
                called = tuple(
                    item
                    for item in _READ_STAGES
                    if invocation.read_services[item].calls
                )
                self.assertEqual(called, _READ_STAGES[: stage_index + 1])
                invocation.render.assert_called_once()

    def test_operation_stage_runs_before_its_following_read_stage(self) -> None:
        cases = (
            ("pre_notion", ()),
            ("device", ("notion",)),
            (
                "barcode",
                ("notion", "device", "failure", "log", "structured"),
            ),
        )
        for stage, expected_read_stages in cases:
            with self.subTest(stage=stage):
                invocation = self._invoke_mention(
                    operation_route="device_box_update",
                    operation_stage=stage,
                    operation_result=_result("device_box_update"),
                )
                called = tuple(
                    item
                    for item in _READ_STAGES
                    if invocation.read_services[item].calls
                )
                self.assertEqual(called, expected_read_stages)

    def test_diagnostic_probe_miss_continues_to_knowledge_remote(self) -> None:
        invocation = self._invoke_mention(
            read_stage="knowledge",
            read_result=_result("company_freeform"),
        )

        self.assertEqual(len(invocation.operation_service.answer_calls), 1)
        invocation.context_loader.assert_called_once()
        self.assertEqual(invocation.replies[-1][0], "API 답변")

    def test_conversation_questions_skip_entry_slack_context_and_diagnostic_probe(self) -> None:
        # 실제 mention entry가 일반 문맥을 준비하기 전부터 저장 조회의 Slack 읽기를 차단한다.
        for question in ("디디가 올해 욕 몇 번 했어?", "디디가 올해 한 욕 보여줘",
                         "디디가 올해 한 욕",
                         "디디가 올해 어떤 욕 했어?", "오늘 디디가 한 말 요약해줘",
                         "디디가 최근 뭐랬어?", "<@UDD|디디>가 최근 뭐랬어?"):
            with self.subTest(question=question), patch.object(team_chat_context, "TEAM_MEMBER_PROFILES",
                    ({"name": "디디", "aliases": ("DD",), "user_id": "UDD"},)):
                slack = Mock()
                slack.conversations_replies.return_value = {"messages": [{"ts": "1785311999.000001",
                    "user": "U1", "text": "DB에 저장하지 않은 스레드 원문"}]}
                invocation = self._invoke_mention(question=question, read_stage="knowledge",
                    read_result=_result("company_freeform"), slack_client=slack, use_real_context_loader=True)
                invocation.context_loader.assert_not_called()
                slack.conversations_replies.assert_not_called()
                slack.conversations_history.assert_not_called()
                slack.conversations_list.assert_not_called()
                self.assertFalse(invocation.operation_service.answer_calls)
                self.assertTrue(invocation.read_services["knowledge"].calls)
                self.assertTrue(all(request.context_entries == ()
                    for service in invocation.read_services.values() for request in service.calls))

    def test_fun_conversation_questions_reach_stored_read_before_fun_collects_context(self) -> None:
        # 모대 호출어가 붙어도 fun의 스레드 수집과 운영 probe를 건너뛰고 저장 조회로 전달한다.
        for question in ("모대야 디디가 올해 욕 몇 번 했어?", "모대야 디디가 올해 한 욕 보여줘",
                         "모대야 디디가 올해 한 욕",
                         "모대야 오늘 디디가 한 말 요약해줘", "모대야 디디가 최근 뭐랬어?"):
            with (
                self.subTest(question=question),
                patch.object(company, "handle_fun_message") as fun,
                patch.object(team_chat_context, "TEAM_MEMBER_PROFILES",
                    ({"name": "디디", "aliases": ("DD",), "user_id": "UDD"},)),
            ):
                slack = Mock()
                invocation = self._invoke_mention(question=question, raw_text=question, channel_id="C0621TL2HSB",
                    message_event=True, read_stage="knowledge", read_result=_result("company_freeform"),
                    slack_client=slack, use_real_context_loader=True)
                fun.assert_not_called()
                invocation.context_loader.assert_not_called()
                slack.conversations_replies.assert_not_called()
                slack.conversations_history.assert_not_called()
                slack.conversations_list.assert_not_called()
                self.assertFalse(invocation.operation_service.answer_calls)
                (request,) = invocation.read_services["knowledge"].calls
                self.assertEqual(request.question, question)
                self.assertEqual(request.context_entries, ())
                self.assertEqual(invocation.replies[-1][1], {"thread": True})

    def test_named_copywriting_keeps_its_existing_thread_context(self) -> None:
        # 이름이 등장해도 문구 편집 요청은 과거 기록 질문으로 바꾸지 않는다.
        slack = Mock()
        slack.conversations_replies.return_value = {"messages": [{"ts": "1785311999.000001",
            "user": "U1", "text": "생일 축하해 디디"}]}
        with patch.object(team_chat_context, "TEAM_MEMBER_PROFILES",
                ({"name": "디디", "aliases": ("DD",), "user_id": "UDD"},)):
            invocation = self._invoke_mention(question="디디 생일축하 문구 다듬어줘", read_stage="knowledge",
                read_result=_result("company_freeform"), slack_client=slack, use_real_context_loader=True)
        invocation.context_loader.assert_called_once()
        slack.conversations_replies.assert_called_once()
        (request,) = invocation.read_services["knowledge"].calls
        self.assertEqual(request.context_entries[0]["text"], "생일 축하해 디디")

    def test_named_fun_copywriting_keeps_the_fun_handler(self) -> None:
        # fun의 문구 작성은 사람 이름 때문에 DB 기록 조회로 선점하지 않는다.
        with patch.object(company, "handle_fun_message") as fun, patch.object(team_chat_context,
                "TEAM_MEMBER_PROFILES", ({"name": "디디", "aliases": ("DD",), "user_id": "UDD"},)):
            invocation = self._invoke_mention(question="모대 디디 생일축하 문구 다듬어줘",
                raw_text="모대 디디 생일축하 문구 다듬어줘", channel_id="C0621TL2HSB", message_event=True)
        fun.assert_called_once()
        self.assertFalse(any(service.calls for service in invocation.read_services.values()))

    def test_explicit_operation_preserves_its_required_thread_context(self) -> None:
        # 기록 언급이 섞여도 명시 장비 operation은 기존의 필요한 스레드 문맥을 유지한다.
        slack = Mock()
        slack.conversations_replies.return_value = {"messages": [{"ts": "1785311999.000001",
            "user": "U1", "text": "이 장비를 업데이트하기로 했어"}]}
        invocation = self._invoke_mention(question="어제 대화에서 정한 장비 업데이트를 실행해줘",
            operation_route="device_box_update", operation_stage="device",
            operation_result=_result("device_box_update"), slack_client=slack, use_real_context_loader=True)
        invocation.context_loader.assert_called_once()
        slack.conversations_replies.assert_called_once()
        (request,) = invocation.operation_service.progress_calls
        self.assertEqual(request.context_entries[0]["text"], "이 장비를 업데이트하기로 했어")

    def test_operation_receipt_failure_keeps_slack_delivery_success(self) -> None:
        invocation = self._invoke_mention(
            operation_route="device_box_update",
            operation_stage="device",
            operation_result=_result(
                "device_box_update",
                operation_result={
                    "kind": "device_operation_delivery",
                    "status": "pending",
                },
            ),
            operation_ack_error=RuntimeError("receipt unavailable"),
        )

        invocation.api_client.acknowledge_request_log_delivery.assert_called_once()
        self.assertTrue(
            invocation.api_client.acknowledge_request_log_delivery.call_args.kwargs[
                "delivered"
            ]
        )
        self.assertEqual(
            invocation.payload["request_log"]["status"],
            "handled",
        )

    def test_final_slack_failure_does_not_ack_operation_delivery(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "render failed"):
            self._invoke_mention(
                operation_route="device_box_update",
                operation_stage="device",
                operation_result=_result(
                    "device_box_update",
                    operation_result={
                        "kind": "device_operation_delivery",
                        "status": "pending",
                    },
                ),
                render_error=RuntimeError("render failed"),
            )

    def test_download_receipt_failure_preserves_completed_dm(self) -> None:
        invocation = self._invoke_mention(
            operation_route="device_file_download",
            operation_stage="barcode",
            operation_result=_result(
                "device_file_download",
                operation_result={
                    "kind": "device_file_download_delivery",
                    "status": "pending",
                },
            ),
            download_ack_error=RuntimeError("receipt unavailable"),
        )

        invocation.render_download.assert_called_once()
        self.assertIn("완료 내역을 기록하지 못했어", invocation.replies[-1][0])
        self.assertTrue(
            invocation.api_client.acknowledge_request_log_delivery.call_args.kwargs[
                "delivered"
            ]
        )

    def test_unmatched_request_returns_only_usage_guide(self) -> None:
        invocation = self._invoke_mention()

        self.assertEqual(
            invocation.replies[-1],
            ("지원 기능이 궁금하면 `사용법`이라고 보내줘", {"mention_user": False}),
        )


if __name__ == "__main__":
    unittest.main()
