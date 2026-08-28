from __future__ import annotations

from dataclasses import replace
import hashlib
import logging
import os
from typing import Any
from urllib.parse import urlsplit

from slack_bolt import App

from boxer.context.entries import ContextEntry
from boxer_adapter_slack.common import (
    MentionPayload,
    SlackReplyFn,
    _load_slack_permalink,
    _load_slack_user_name,
    _merge_request_log_metadata,
    _set_request_log_route,
    _set_request_log_skip_persist,
    _set_request_log_status,
    create_slack_app,
)
from boxer_adapter_slack.context import (
    load_slack_thread_context_entries,
)
from boxer_company import settings as cs
from boxer_company.transport_contracts import (
    DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION,
    DEVICE_FILE_DOWNLOAD_ROUTE,
    DEVICE_FILE_LOOKUP_ROUTE,
    DEVICE_FILE_RECOVERY_ROUTE,
    DEVICE_OPERATION_DELIVERY_ACTION,
    _is_device_scanner_abi_patch_intent,
    build_trusted_mda_recovery_scope_metadata,
    company_operation_route_names,
    needs_device_file_operation_context,
    resolve_device_file_operation_scope,
)
from boxer_company.operation_routing import (
    company_operation_legacy_stage,
    match_company_operation_route,
)
from boxer_company_adapter_slack.access_routes import (
    BASE_ACCESS_DENIED_REPLY,
    build_slack_base_access_runtime,
    handle_base_access_management_command,
)
from boxer_company_adapter_slack.assistant_bridge import (
    assistant_slack_route_name,
    build_company_assistant_request,
    render_company_assistant_result,
    render_device_file_download_delivery,
)
from boxer_company_adapter_slack.automation_api_client import (
    CompanyAutomationApiClient,
)
from boxer_company_adapter_slack.automation_reporter import (
    validate_automation_delivery_journal_preflight,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiContractError,
    CompanyAssistantApiClient,
    load_company_api_client_settings,
)
from boxer_company_adapter_slack.company_api_rollout import (
    wrap_company_barcode_freeform_service,
    wrap_company_barcode_log_service,
    wrap_company_barcode_residual_service,
    wrap_company_barcode_service,
    wrap_company_barcode_timeline_service,
    wrap_company_device_db_detail_service,
    wrap_company_device_filter_service,
    wrap_company_device_service,
    wrap_company_freeform_service,
    wrap_company_notion_service,
    wrap_company_operations_service,
    wrap_company_playbook_service,
    wrap_company_recording_failure_service,
    wrap_company_structured_service,
    wrap_company_usage_help_service,
    wrap_company_weekly_summary_service,
)
from boxer_company_adapter_slack.daily_device_round_reporter import (
    attach_daily_device_round_reporter,
)
from boxer_company_adapter_slack.device_health_alert_api import (
    DeviceHealthAlertApiBridge,
)
from boxer_company_adapter_slack.device_health_monitor_reporter import (
    attach_device_health_monitor_reporter,
)
from boxer_company_adapter_slack.device_notification_alert_reporter import (
    attach_device_notification_alert_reporter,
)
from boxer_company_adapter_slack.device_file_scope import (
    lookup_device_file_scope_from_mda_recovery_thread,
)
from boxer_company_adapter_slack.fun import (
    handle_fun_message,
    is_human_fun_trigger,
)
from boxer_company_adapter_slack.health import (
    _format_ping_llm_status,
)
from boxer_company_adapter_slack.hpa_change_api_client import (
    HpaChangeApiClient,
    build_hpa_change_remote_routes_config,
)
from boxer_company_adapter_slack.hpa_change_remote_reporter import (
    attach_hpa_change_remote_reporter,
)
from boxer_company_adapter_slack.hpa_change_routes import (
    HpaChangeRoutesContext,
    HpaChangeRoutesDeps,
    _handle_hpa_change_request,
)
from boxer_company_adapter_slack.security_review_routes import (
    SecurityReviewMessageContext,
    SecurityReviewRoutesContext,
    _handle_security_review_bot_message,
    _handle_security_review_request,
)
from boxer_company_adapter_slack.startup_guard import (
    _validate_ec2_runtime_aws_env,
)
from boxer_company_adapter_slack.thread_learning_routes import (
    _load_thread_context_entries_for_learning,
)
from boxer_company_adapter_slack.weekly_recordings_reporter import (
    attach_weekly_recordings_reporter,
)


_PROGRESS_OPERATION_ROUTES = frozenset(
    {"device_box_update", "device_agent_update", "device_power_off"}
)


def _require_transport_only_remote_settings() -> None:
    """API scheduler가 domain cycle을 소유하지 않으면 기동을 거부한다."""

    # API와 Slack이 같은 process import cache가 아니라 각 host env를 직접
    # 읽어 scheduler 소유권을 확인한다.
    if os.getenv(
        "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED",
        "false",
    ).strip().lower() in {"1", "true", "yes", "on"}:
        return
    raise CompanyApiContractError(
        "company_api_transport_only_remote_required"
    )


def _build_remote_read_service(
    api_client: CompanyAssistantApiClient,
    logger: logging.Logger,
) -> dict[str, Any]:
    """기존 operation/read 우선순위를 보존하는 remote stage들을 만든다."""

    # stage를 한 체인으로 합치면 final freeform이 뒤의 operation을 먼저
    # 소비한다. 기존 stage 사이 operation gate를 유지하도록 분리한다.
    structured = wrap_company_structured_service(
        None,
        api_client,
        logger,
    )
    structured = wrap_company_device_filter_service(
        structured,
        api_client,
        logger,
    )
    structured = wrap_company_device_db_detail_service(
        structured,
        api_client,
        logger,
    )
    structured = wrap_company_weekly_summary_service(
        structured,
        api_client,
        logger,
    )

    barcode = wrap_company_barcode_service(None, api_client, logger)
    barcode = wrap_company_barcode_residual_service(
        barcode,
        api_client,
        logger,
    )
    barcode = wrap_company_barcode_timeline_service(
        barcode,
        api_client,
        logger,
    )

    knowledge = wrap_company_playbook_service(None, api_client, logger)
    knowledge = wrap_company_barcode_freeform_service(
        knowledge,
        api_client,
        logger,
    )
    knowledge = wrap_company_usage_help_service(
        knowledge,
        api_client,
        logger,
    )
    knowledge = wrap_company_freeform_service(
        knowledge,
        api_client,
        logger,
    )

    return {
        "notion": wrap_company_notion_service(None, api_client, logger),
        "device": wrap_company_device_service(None, api_client, logger),
        "failure": wrap_company_recording_failure_service(
            None,
            api_client,
            logger,
        ),
        "log": wrap_company_barcode_log_service(None, api_client, logger),
        "structured": structured,
        "barcode": barcode,
        "knowledge": knowledge,
    }


def create_app() -> App:
    _validate_ec2_runtime_aws_env()
    app_logger = logging.getLogger(__name__)
    company_api_settings = load_company_api_client_settings()
    _require_transport_only_remote_settings()
    # 구 no-batch receipt는 poll loop가 시작되기 전에 읽기 전용으로 차단한다.
    validate_automation_delivery_journal_preflight(
        state_path=cs.AUTOMATION_DELIVERY_STATE_PATH,
    )

    base_access_runtime = build_slack_base_access_runtime(
        logger=app_logger
    )
    company_api_client = CompanyAssistantApiClient(
        company_api_settings
    )
    automation_api_client = CompanyAutomationApiClient(
        company_api_settings,
        logger=app_logger,
    )
    remote_read_services = _build_remote_read_service(
        company_api_client,
        app_logger,
    )
    remote_operation_service = wrap_company_operations_service(
        None,
        company_api_client,
        app_logger,
        match_company_operation_route,
        company_operation_route_names(),
    )
    device_health_alert_api_bridge = DeviceHealthAlertApiBridge(
        company_api_client
    )

    hpa_change_routes_config = build_hpa_change_remote_routes_config()
    hpa_change_api_client = (
        HpaChangeApiClient(
            company_api_settings,
            workspace_id=company_api_settings.automation_tenant_id,
            logger=app_logger,
        )
        if hpa_change_routes_config.enabled
        else None
    )

    def _submit_hpa_change_request(request: Any) -> Any:
        if hpa_change_api_client is None:
            raise CompanyApiContractError(
                "company_api_hpa_transport_disabled"
            )
        return hpa_change_api_client.submit_request(request)

    def _lookup_hpa_change_thread(*args: Any, **kwargs: Any) -> Any:
        if hpa_change_api_client is None:
            raise CompanyApiContractError(
                "company_api_hpa_transport_disabled"
            )
        return hpa_change_api_client.lookup_thread_job(*args, **kwargs)

    def _remote_ping_health(payload: MentionPayload) -> bool | None:
        request = build_company_assistant_request(payload)
        result = company_api_client.answer(
            request,
            route_group="health",
        )
        if (
            result.route != "company_llm_health"
            or result.outcome != "answered"
            or result.used_llm
            or len(result.messages) != 1
            or result.sources
            or result.messages[0].delivery_scope != "conversation"
            or result.messages[0].mention_actor
            or result.messages[0].private_links
        ):
            raise CompanyApiContractError(
                "company_api_health_result_invalid"
            )
        status = str(result.messages[0].body or "").strip()
        if status == "available":
            return True
        if status == "unavailable":
            return False
        if status == "unconfigured":
            return None
        raise CompanyApiContractError(
            "company_api_health_result_invalid"
        )

    def _remote_fun_reply(
        payload: Any,
        raw_text: str,
        thread_context: str,
        speaker_user_id: str,
    ) -> tuple[str, str, bool]:
        if speaker_user_id != str(
            payload.get("user_id") or ""
        ).strip():
            raise CompanyApiContractError(
                "company_api_fun_actor_invalid"
            )
        request_payload = dict(payload)
        request_payload["question"] = raw_text
        request = build_company_assistant_request(
            request_payload,
            metadata={"team_fun_context": thread_context},
        )
        result = company_api_client.answer(
            request,
            route_group="fun",
        )
        valid_message = bool(
            len(result.messages) == 1
            and not result.sources
            and result.messages[0].delivery_scope == "conversation"
            and not result.messages[0].private_links
            and str(result.messages[0].body or "").strip()
        )
        if result.route != "company_team_fun" or not valid_message:
            raise CompanyApiContractError(
                "company_api_fun_result_invalid"
            )
        body = str(result.messages[0].body or "").strip()
        if result.outcome == "answered" and isinstance(
            result.used_llm,
            bool,
        ):
            return body, "company_api", True
        if (
            result.outcome == "denied"
            and result.fallback_reason == "prompt_security"
            and result.used_llm is False
        ):
            return body, "company_api_prompt_security", False
        raise CompanyApiContractError(
            "company_api_fun_result_invalid"
        )

    def _remote_fortune_reply(
        payload: Any,
        raw_text: str,
        thread_root_text: str,
        speaker_user_id: str,
    ) -> str | None:
        expected_actor = str(
            payload.get("user_id")
            or payload.get("bot_user_id")
            or payload.get("bot_id")
            or payload.get("app_id")
            or ""
        ).strip()
        if not speaker_user_id or speaker_user_id != expected_actor:
            raise CompanyApiContractError(
                "company_api_fun_actor_invalid"
            )
        request_payload = dict(payload)
        request_payload["question"] = raw_text
        request_payload["user_id"] = speaker_user_id
        context_entries: tuple[ContextEntry, ...] = (
            (
                {
                    "kind": "message",
                    "source": "slack",
                    "text": thread_root_text,
                },
            )
            if thread_root_text
            else ()
        )
        result = company_api_client.answer(
            build_company_assistant_request(
                request_payload,
                context_entries=context_entries,
            ),
            route_group="fun",
        )
        if (
            result.route == "unhandled"
            and result.outcome == "no_evidence"
            and not result.used_llm
            and result.fallback_reason == "no_matching_route"
        ):
            return None
        if (
            result.route != "company_daily_fortune"
            or result.outcome != "answered"
            or result.used_llm
            or result.fallback_reason is not None
            or len(result.messages) != 1
            or result.sources
            or result.messages[0].delivery_scope != "conversation"
            or result.messages[0].mention_actor
            or result.messages[0].private_links
        ):
            raise CompanyApiContractError(
                "company_api_fortune_result_invalid"
            )
        body = str(result.messages[0].body or "").strip()
        if not body:
            raise CompanyApiContractError(
                "company_api_fortune_result_invalid"
            )
        return body

    def _handle_company_mention(
        payload: MentionPayload,
        reply: SlackReplyFn,
        client: Any,
        logger: logging.Logger,
    ) -> None:
        text = str(payload.get("text") or "")
        question = str(payload.get("question") or "").strip()
        user_id = str(payload.get("user_id") or "").strip() or None
        workspace_id = str(
            payload.get("workspace_id") or ""
        ).strip()
        channel_id = str(payload.get("channel_id") or "").strip()
        current_ts = str(payload.get("current_ts") or "").strip()
        thread_ts = str(
            payload.get("thread_ts") or current_ts
        ).strip()

        if handle_base_access_management_command(
            payload,
            reply,
            client,
            logger,
            runtime=base_access_runtime,
        ):
            return
        if not base_access_runtime.is_allowed(
            workspace_id,
            user_id,
        ):
            _set_request_log_route(payload, "base_access")
            _set_request_log_status(payload, "denied")
            reply(BASE_ACCESS_DENIED_REPLY)
            return

        scanner_abi_patch_intent = (
            _is_device_scanner_abi_patch_intent(question)
        )
        if (
            not scanner_abi_patch_intent
            and _handle_hpa_change_request(
                HpaChangeRoutesContext(
                    question=question,
                    payload=payload,
                    user_id=user_id,
                    workspace_id=workspace_id,
                    channel_id=channel_id,
                    current_ts=current_ts,
                    thread_ts=thread_ts,
                    reply=reply,
                    client=client,
                    logger=logger,
                ),
                hpa_change_routes_config,
                HpaChangeRoutesDeps(
                    submit_request=_submit_hpa_change_request,
                    lookup_thread_job=_lookup_hpa_change_thread,
                ),
            )
        ):
            return

        if not scanner_abi_patch_intent and "ping" in text:
            _set_request_log_route(
                payload,
                "ping",
                handler_type="company_api",
                route_mode="remote",
            )
            try:
                health_ok = _remote_ping_health(payload)
            except Exception as exc:
                logger.warning(
                    "Company API ping health failed thread_ts=%s "
                    "error_type=%s",
                    thread_ts,
                    type(exc).__name__,
                )
                health_ok = False
            reply(
                f"🏓 pong\n• llm: {_format_ping_llm_status(health_ok)}"
            )
            return

        if _handle_security_review_request(
            SecurityReviewRoutesContext(
                question=question,
                payload=payload,
                user_id=user_id,
                channel_id=channel_id,
                thread_ts=thread_ts,
                reply=reply,
                client=client,
                logger=logger,
                api_client=company_api_client,
            )
        ):
            return

        context_entries: tuple[ContextEntry, ...] | None = None

        def get_context_entries() -> tuple[ContextEntry, ...]:
            nonlocal context_entries
            if context_entries is None:
                context_entries = tuple(
                    load_slack_thread_context_entries(
                        client,
                        logger,
                        channel_id,
                        thread_ts,
                        current_ts,
                    )
                )
            return context_entries

        operation_probe_request = build_company_assistant_request(
            payload,
            context_entries=(
                get_context_entries()
                if needs_device_file_operation_context(question)
                else ()
            ),
        )
        operation_route = match_company_operation_route(
            operation_probe_request
        )
        operation_stage = (
            company_operation_legacy_stage(operation_probe_request)
            if operation_route is not None
            else None
        )
        operation_audit_context: dict[str, Any] | None = None

        def safe_permalink(value: str | None) -> str | None:
            normalized = str(value or "").strip()
            try:
                parsed = urlsplit(normalized)
            except ValueError:
                return None
            if (
                parsed.scheme != "https"
                or not str(parsed.hostname or "").casefold().endswith(
                    ".slack.com"
                )
                or parsed.username is not None
                or parsed.password is not None
                or not parsed.path.startswith(
                    f"/archives/{channel_id}/p"
                )
                or parsed.fragment
            ):
                return None
            return normalized

        def get_operation_audit_context() -> dict[str, Any]:
            nonlocal operation_audit_context
            if operation_audit_context is not None:
                return dict(operation_audit_context)
            request_log = payload.get("request_log")
            log_context = (
                request_log if isinstance(request_log, dict) else {}
            )
            user_name = str(
                log_context.get("user_name") or ""
            ).strip() or _load_slack_user_name(
                client,
                workspace_id,
                user_id or "",
                logger,
            )
            permalink = safe_permalink(
                str(log_context.get("permalink") or "").strip()
                or _load_slack_permalink(
                    client,
                    channel_id,
                    current_ts,
                    logger,
                )
            )
            thread_permalink = (
                safe_permalink(
                    str(
                        log_context.get("thread_permalink") or ""
                    ).strip()
                    or _load_slack_permalink(
                        client,
                        channel_id,
                        thread_ts,
                        logger,
                    )
                )
                if thread_ts != current_ts
                else None
            )
            operation_audit_context = {
                "event_type": "app_mention",
                "channel_id": channel_id,
                "message_id": current_ts,
                "thread_id": thread_ts,
                "is_thread_root": current_ts == thread_ts,
            }
            if user_name:
                operation_audit_context["user_name"] = user_name
            if permalink:
                operation_audit_context["permalink"] = permalink
            if thread_permalink:
                operation_audit_context["thread_permalink"] = (
                    thread_permalink
                )
            return dict(operation_audit_context)

        def acknowledge_request_log_delivery(
            request: Any,
            *,
            delivered: bool,
            error_type: str | None = None,
        ) -> None:
            request_log = payload.get("request_log")
            log_context = (
                request_log if isinstance(request_log, dict) else {}
            )
            reply_count = max(
                0,
                int(log_context.get("reply_count") or 0),
            )
            try:
                receipt = (
                    company_api_client
                    .acknowledge_request_log_delivery(
                        request,
                        delivered=delivered,
                        reply_count=reply_count,
                        first_replied_at_utc=(
                            log_context.get("first_replied_at_utc")
                            if reply_count > 0
                            else None
                        ),
                        error_type=error_type,
                    )
                )
                if (
                    receipt.route != "request_log_delivery"
                    or receipt.outcome != "answered"
                ):
                    logger.warning(
                        "Company API request-log delivery receipt "
                        "rejected request_id=%s",
                        request.request_id,
                    )
            except Exception as exc:
                logger.warning(
                    "Company API request-log delivery receipt failed "
                    "request_id=%s error_type=%s",
                    request.request_id,
                    type(exc).__name__,
                )
            _set_request_log_status(
                payload,
                "handled" if delivered else "error",
                error_type=error_type,
            )

        def render_operation_result(
            request: Any,
            result: Any,
        ) -> bool:
            operation_receipt = result.operation_result
            if (
                result.route == DEVICE_FILE_DOWNLOAD_ROUTE
                and isinstance(operation_receipt, dict)
                and operation_receipt.get("kind")
                == "device_file_download_delivery"
            ):
                delivered = render_device_file_download_delivery(
                    result,
                    reply=reply,
                    actor_id=user_id,
                    client=client,
                    logger=logger,
                )
                if delivered:
                    try:
                        delivered_result = (
                            company_api_client
                            .acknowledge_device_file_download(
                                request,
                                operation_receipt,
                            )
                        )
                    except Exception as exc:
                        # DM은 이미 전달됐다. activity receipt 실패는 Slack
                        # write나 재시도로 보상하지 않고 전달 성공을 보존한다.
                        logger.warning(
                            "Company API download delivery receipt failed "
                            "request_id=%s error_type=%s",
                            request.request_id,
                            type(exc).__name__,
                        )
                        reply(
                            "다운로드 링크는 DM으로 보냈지만 완료 내역을 "
                            "기록하지 못했어. 잠시 후 관리자에게 확인해줘"
                        )
                        return True
                    render_company_assistant_result(
                        delivered_result,
                        reply=reply,
                        actor_id=user_id,
                        client=client,
                        logger=logger,
                    )
                return True

            sent_count = render_company_assistant_result(
                result,
                reply=reply,
                actor_id=user_id,
                client=client,
                logger=logger,
            )
            if (
                result.route in _PROGRESS_OPERATION_ROUTES
                and isinstance(operation_receipt, dict)
                and operation_receipt.get("kind")
                == DEVICE_OPERATION_DELIVERY_ACTION
                and operation_receipt.get("status") == "pending"
                and sent_count > 0
            ):
                # 최종 Slack 전달 뒤 같은 request ID의 activity receipt만 보낸다.
                try:
                    delivery_ack = (
                        company_api_client
                        .acknowledge_device_operation_delivery(
                            request,
                            operation_receipt,
                        )
                    )
                    if (
                        delivery_ack.route
                        != DEVICE_OPERATION_DELIVERY_ACTION
                        or delivery_ack.outcome != "answered"
                    ):
                        logger.warning(
                            "Company API device operation delivery "
                            "receipt rejected request_id=%s",
                            request.request_id,
                        )
                except Exception as exc:
                    # 최종 응답은 이미 전송됐으므로 receipt 오류를 같은
                    # Slack 전송의 실패 상태로 되돌리지 않는다.
                    logger.warning(
                        "Company API device operation delivery receipt "
                        "failed request_id=%s error_type=%s",
                        request.request_id,
                        type(exc).__name__,
                    )
            return True

        def handle_operation(expected_stage: str) -> bool:
            if (
                operation_route is None
                or operation_stage != expected_stage
                or (
                    expected_stage == "knowledge"
                    and operation_route == "device_diagnostic_followup"
                )
            ):
                return False
            _set_request_log_skip_persist(payload, True)
            metadata: dict[str, Any] = {
                "audit_context": get_operation_audit_context()
            }
            execution_context = get_context_entries()
            if operation_route == "thread_playbook_learning":
                execution_context = tuple(
                    _load_thread_context_entries_for_learning(
                        client,
                        logger,
                        channel_id=channel_id,
                        thread_ts=thread_ts,
                        current_ts=current_ts,
                    )
                )
                metadata["thread_permalink"] = (
                    _load_slack_permalink(
                        client,
                        channel_id,
                        thread_ts,
                        logger,
                    )
                )
            if operation_route in {
                "recording_streaming_restore",
                "device_box_update",
                "device_agent_update",
                "device_power_off",
                DEVICE_FILE_DOWNLOAD_ROUTE,
            }:
                metadata["actor_name"] = _load_slack_user_name(
                    client,
                    workspace_id,
                    user_id or "",
                    logger,
                )
            if operation_route in {
                DEVICE_FILE_LOOKUP_ROUTE,
                DEVICE_FILE_DOWNLOAD_ROUTE,
                DEVICE_FILE_RECOVERY_ROUTE,
            }:
                barcode, log_date = (
                    resolve_device_file_operation_scope(
                        operation_probe_request
                    )
                )
                if barcode and log_date:
                    trusted_contexts = (
                        lookup_device_file_scope_from_mda_recovery_thread(
                            client=client,
                            logger=logger,
                            channel_id=channel_id,
                            thread_ts=thread_ts,
                            requested_barcode=barcode,
                            requested_date=log_date,
                        )
                    )
                    if len(trusted_contexts) == 1:
                        metadata.update(
                            build_trusted_mda_recovery_scope_metadata(
                                barcode=barcode,
                                log_date=log_date,
                                device_context=trusted_contexts[0],
                            )
                        )
            request = build_company_assistant_request(
                payload,
                context_entries=execution_context,
                metadata=metadata,
            )
            _set_request_log_route(
                payload,
                assistant_slack_route_name(operation_route),
                handler_type="company_api",
                route_mode="remote",
            )

            def render_partial(partial_result: Any) -> None:
                try:
                    render_company_assistant_result(
                        partial_result,
                        reply=reply,
                        actor_id=user_id,
                        client=client,
                        logger=logger,
                    )
                except Exception as exc:
                    logger.warning(
                        "Remote operation progress delivery failed "
                        "request_id=%s error_type=%s",
                        request.request_id,
                        type(exc).__name__,
                    )

            result = (
                remote_operation_service.answer_with_progress(
                    request,
                    render_partial,
                )
                if operation_route in _PROGRESS_OPERATION_ROUTES
                else remote_operation_service.answer(request)
            )
            if result is None:
                return False
            _set_request_log_status(payload, result.outcome)
            try:
                render_operation_result(request, result)
            except Exception as exc:
                acknowledge_request_log_delivery(
                    request,
                    delivered=False,
                    error_type=type(exc).__name__,
                )
                raise
            acknowledge_request_log_delivery(
                request,
                delivered=True,
            )
            return True

        def handle_diagnostic_probe() -> bool:
            if not question:
                return False
            digest = hashlib.sha256(
                operation_probe_request.request_id.encode("utf-8")
            ).hexdigest()[:32]
            request = replace(
                build_company_assistant_request(
                    payload,
                    context_entries=get_context_entries(),
                    metadata={
                        "audit_context": get_operation_audit_context(),
                        "operation_action": {
                            "name": (
                                DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION
                            )
                        },
                    },
                ),
                request_id=f"diag-probe:{digest}",
            )
            _set_request_log_skip_persist(payload, True)
            result = remote_operation_service.answer(request)
            if (
                result is not None
                and result.route == "device_diagnostic_followup"
                and result.outcome == "no_evidence"
                and result.fallback_reason
                == "diagnostic_snapshot_missing"
            ):
                request_log = payload.get("request_log")
                if isinstance(request_log, dict):
                    request_log.pop("skip_persist", None)
                return False
            if result is None:
                raise CompanyApiContractError(
                    "company_api_diagnostic_probe_result_invalid",
                    request_id=request.request_id,
                )
            _set_request_log_route(
                payload,
                assistant_slack_route_name(result.route),
                handler_type="company_api",
                route_mode="remote",
            )
            try:
                render_company_assistant_result(
                    result,
                    reply=reply,
                    actor_id=user_id,
                    client=client,
                    logger=logger,
                )
            except Exception as exc:
                acknowledge_request_log_delivery(
                    request,
                    delivered=False,
                    error_type=type(exc).__name__,
                )
                raise
            acknowledge_request_log_delivery(
                request,
                delivered=True,
            )
            return True

        if handle_operation("pre_notion"):
            return

        # read wrapper는 matcher만 Slack에서 판정하고 근거 조회는 API에
        # 맡긴다. operation gate를 stage 사이에 그대로 둔다.
        request = build_company_assistant_request(
            payload,
            context_entries=get_context_entries(),
        )

        def handle_read_stage(stage: str) -> bool:
            result = remote_read_services[stage].answer(request)
            if result is None:
                return False
            _set_request_log_route(
                payload,
                assistant_slack_route_name(result.route),
                handler_type="company_api",
                route_mode="remote",
            )
            _merge_request_log_metadata(
                payload,
                assistantOutcome=result.outcome,
                assistantFallbackReason=result.fallback_reason,
                assistantUsedLlm=result.used_llm,
            )
            render_company_assistant_result(
                result,
                reply=reply,
                actor_id=user_id,
                client=client,
                logger=logger,
            )
            return True

        if handle_read_stage("notion"):
            return
        if handle_operation("device"):
            return
        for stage in ("device", "failure", "log", "structured"):
            if handle_read_stage(stage):
                return
        if handle_operation("barcode"):
            return
        if handle_read_stage("barcode"):
            return
        if handle_diagnostic_probe():
            return
        if handle_operation("knowledge"):
            return
        if handle_read_stage("knowledge"):
            return

        reply(
            "지원 기능이 궁금하면 `사용법`이라고 보내줘",
            mention_user=False,
        )

    def _handle_company_message(
        payload: Any,
        reply: Any,
        client: Any,
        logger: logging.Logger,
    ) -> None:
        if _handle_security_review_bot_message(
            SecurityReviewMessageContext(
                payload=payload,
                reply=reply,
                client=client,
                logger=logger,
                api_client=company_api_client,
            )
        ):
            return
        if is_human_fun_trigger(
            payload
        ) and not base_access_runtime.is_allowed(
            payload.get("workspace_id"),
            payload.get("user_id"),
        ):
            _set_request_log_route(payload, "base_access")
            _set_request_log_status(payload, "denied")
            reply(BASE_ACCESS_DENIED_REPLY, thread=True)
            return
        handle_fun_message(
            payload,
            reply,
            client,
            logger,
            remote_reply_generator=(
                lambda raw_text, thread_context, speaker_user_id: (
                    _remote_fun_reply(
                        payload,
                        raw_text,
                        thread_context,
                        speaker_user_id,
                    )
                )
            ),
            remote_fortune_reply_generator=(
                lambda raw_text, thread_root_text, speaker_user_id: (
                    _remote_fortune_reply(
                        payload,
                        raw_text,
                        thread_root_text,
                        speaker_user_id,
                    )
                )
            ),
        )

    app = create_slack_app(
        _handle_company_mention,
        _handle_company_message,
    )
    if hpa_change_api_client is not None:
        attach_hpa_change_remote_reporter(
            app,
            hpa_change_api_client,
            poll_interval_sec=cs.HPA_CHANGE_POLL_INTERVAL_SEC,
            logger=app_logger,
        )
    attach_weekly_recordings_reporter(
        app,
        logger=app_logger,
        automation_client=automation_api_client,
    )
    attach_device_health_monitor_reporter(
        app,
        logger=app_logger,
        base_access_checker=base_access_runtime.is_allowed,
        action_api_bridge=device_health_alert_api_bridge,
        automation_client=automation_api_client,
        notification_automation_client=automation_api_client,
    )
    attach_device_notification_alert_reporter(
        app,
        logger=app_logger,
        automation_client=automation_api_client,
    )
    attach_daily_device_round_reporter(
        app,
        logger=app_logger,
        automation_client=automation_api_client,
    )
    return app
