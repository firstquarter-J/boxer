from __future__ import annotations

# 순수 분류 정본은 provider-free 모듈이 소유하고 이 실행 모듈은 재수출한다.
from boxer_company.operation_routing import (
    _DEVICE_HEALTH_ALERT_ACTION_ROUTES,
    match_device_health_alert_action_route,
)

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import logging
import re
import threading
from typing import Any, Callable, Mapping

import requests

from boxer.core import settings as core_settings
from boxer.retrieval.connectors.db import _create_db_connection
from boxer_company import settings as company_settings
from boxer_company.device_health_alert_ack import (
    DeviceHealthAlertAcknowledgement,
    claim_device_health_alert_acknowledgement,
)
from boxer_company.device_health_event_log import (
    append_device_health_monitor_event,
)
from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.routers.device_voice_control import (
    _dispatch_device_voice_guide,
)
from boxer_company.routers.device_ssh_security import (
    _mark_company_api_mutation_attempted,
)
from boxer_company.routers.mda_graphql import (
    _get_mda_device_agent_ssh,
    _send_mda_device_command,
)
from boxer_company.sms_delivery import (
    _SMS_DELIVERY_ACCEPTED,
    _SMS_DELIVERY_CONFIRM_REQUIRED,
    _SMS_DELIVERY_DELIVERED,
    _SMS_DELIVERY_FAILED,
    _SMS_DELIVERY_REQUEST_FAILED,
    _build_solapi_authorization_header,
)
from boxer_company.sms_delivery_cycle import (
    remember_sms_delivery_sheet_record,
)
from boxer_company.transport_contracts import (
    DEVICE_HEALTH_ALERT_ACK_REQUEST_ID_PREFIX,
    DEVICE_HEALTH_ALERT_MARK_DONE_ACTION,
    DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_ACTION,
    DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_ROUTE,
    DEVICE_HEALTH_ALERT_UI_RECEIPT_ACTION,
    DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE,
    DEVICE_HEALTH_ALERT_VOICE_ACTION,
    DEVICE_HEALTH_ALERT_VOICE_ROUTE,
)


_DEVICE_HEALTH_ALERT_ACTION_LABELS = {
    DEVICE_HEALTH_ALERT_SMS_ACTION: "병원 문자 보내기",
    DEVICE_HEALTH_ALERT_VOICE_ACTION: "장비 음성 안내",
    DEVICE_HEALTH_ALERT_MARK_DONE_ACTION: "확인 완료",
}

_DEVICE_HEALTH_ALERT_VOICE_MINIMUM_VERSION = (2, 11, 308)
_DEVICE_HEALTH_ALERT_VOICE_MINIMUM_VERSION_TEXT = "2.11.308"
_DEVICE_HEALTH_ALERT_SMS_GREETING = "안녕하세요 마미톡입니다. 🌷"
_DEVICE_HEALTH_ALERT_VOICE_CATEGORIES = frozenset(
    {"recording", "recording_processing", "video_signal"}
)
_DEVICE_HEALTH_ALERT_PHONE_PATTERN = re.compile(r"^(?:\+82|82|0)1[016789]\d{7,8}$")
_DEVICE_HEALTH_ALERT_VOICE_CLAIMS: dict[str, datetime] = {}
_DEVICE_HEALTH_ALERT_VOICE_CLAIMS_LOCK = threading.Lock()


@dataclass(frozen=True, slots=True)
class DeviceHealthAlertActionTarget:
    """Slack 표현을 제거한 장비 이상 알림의 exact 실행 대상이다."""

    hospital_seq: int
    hospital_name: str
    room_name: str
    device_name: str
    issue: str
    alert_category: str
    problem_components: tuple[str, ...]
    hospital_label: str = ""
    mda_url: str = ""


@dataclass(frozen=True, slots=True)
class DeviceHealthAlertAction:
    """질문 parser가 아니라 HTTP typed metadata에서만 생성되는 액션이다."""

    name: str
    phase: str
    target: DeviceHealthAlertActionTarget
    sms_phone_number: str = ""
    sms_message: str = ""


@dataclass(frozen=True, slots=True)
class DeviceHealthAlertUiReceipt:
    """Slack modal transport 결과를 legacy event payload로 재현한다."""

    event_type: str
    action_id: str
    mode: str
    target: DeviceHealthAlertActionTarget
    message_ts: str
    thread_ts: str
    occurred_at: datetime
    status: str
    ok: bool
    error_type: str = ""


@dataclass(frozen=True, slots=True)
class DeviceHealthAlertActionRouteDeps:
    """DB/MDA/SMS port를 주입해 transport와 실행 경계를 분리한다."""

    load_exact_target: Callable[
        [DeviceHealthAlertActionTarget],
        dict[str, Any] | None,
    ] = lambda target: _load_exact_device_health_alert_target(target)
    get_mda_device: Callable[[str], dict[str, Any] | None] = (
        _get_mda_device_agent_ssh
    )
    send_mda_command: Callable[..., dict[str, Any]] = _send_mda_device_command
    send_sms: Callable[..., dict[str, Any]] = (
        lambda payload, logger: _send_device_health_alert_sms(
            payload,
            logger=logger,
        )
    )
    remember_sms_delivery: Callable[..., bool] = (
        remember_sms_delivery_sheet_record
    )
    claim_voice_guide: Callable[[str, datetime], dict[str, Any]] = (
        lambda claim_key, now: _claim_device_health_alert_voice_guide(
            claim_key,
            now,
        )
    )
    now: Callable[[], datetime] = lambda: datetime.now(timezone.utc)
    write_event: Callable[
        [str, datetime, Mapping[str, Any]], bool
    ] = lambda event_type, now, payload: append_device_health_monitor_event(
        event_type,
        payload,
        now=now,
    )
    claim_mark_done: Callable[..., DeviceHealthAlertAcknowledgement] = (
        claim_device_health_alert_acknowledgement
    )


class DeviceHealthAlertActionAssistantRoute:
    """승인된 Slack action을 API 프로세스에서 정확히 한 번 실행한다."""

    name = "device_health_alert_action"

    def __init__(
        self,
        deps: DeviceHealthAlertActionRouteDeps | None = None,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._deps = deps or DeviceHealthAlertActionRouteDeps()
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        route = match_device_health_alert_action_route(request)
        if route is None:
            return None

        if route == DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE:
            receipt = _parse_device_health_alert_ui_receipt(request.metadata)
            if receipt is None:
                return _result(
                    route=route,
                    outcome="denied",
                    body="장비 이상 알림 UI receipt가 올바르지 않아 기록하지 않았어",
                    fallback_reason="invalid_device_health_alert_ui_receipt",
                )
            if not str(request.actor_id or "").strip():
                return _result(
                    route=route,
                    outcome="denied",
                    body="실행 요청자를 확인할 수 없어 기록하지 않았어",
                    fallback_reason="missing_operation_actor",
                )
            return self._record_ui_receipt(request, receipt)

        action = _parse_device_health_alert_action(request.metadata)
        if action is None:
            return _result(
                route=route,
                outcome="denied",
                body="장비 이상 알림 작업 대상이 올바르지 않아 실행하지 않았어",
                fallback_reason="invalid_device_health_alert_action",
            )
        if not str(request.actor_id or "").strip():
            # HTTP 정책을 우회해 runtime을 직접 부르는 경우도 익명 mutation은 막는다.
            return _result(
                route=route,
                outcome="denied",
                body="실행 요청자를 확인할 수 없어 작업하지 않았어",
                fallback_reason="missing_operation_actor",
            )

        try:
            exact_target = self._deps.load_exact_target(action.target)
        except Exception as exc:
            self._logger.warning(
                "장비 이상 알림 exact target 조회 실패 error_type=%s",
                type(exc).__name__,
            )
            return self._record_action_result(
                request,
                action,
                _result(
                    route=route,
                    outcome="failed",
                    body=(
                        "장비 이상 알림 대상을 확인하지 못해 실행하지 않았어"
                    ),
                    fallback_reason=(
                        "device_health_alert_target_lookup_failed"
                    ),
                ),
            )
        if exact_target is None:
            return self._record_action_result(
                request,
                action,
                _result(
                    route=route,
                    outcome="denied",
                    body=(
                        "현재 DB 정보와 알림 대상이 일치하지 않아 "
                        "실행하지 않았어"
                    ),
                    fallback_reason="device_health_alert_target_mismatch",
                ),
            )

        if action.name == DEVICE_HEALTH_ALERT_SMS_ACTION:
            if action.phase == "prepare":
                # legacy modal open은 UI receipt 이벤트 하나만 남겼다. DB 기본값
                # 준비 단계는 action 실행 이벤트로 중복 기록하지 않는다.
                return self._prepare_sms(action, exact_target)
            return self._record_action_result(
                request,
                action,
                self._send_sms(request, action, exact_target),
            )
        if action.name == DEVICE_HEALTH_ALERT_VOICE_ACTION:
            return self._record_action_result(
                request,
                action,
                self._send_voice(request, action),
            )
        if action.name == DEVICE_HEALTH_ALERT_MARK_DONE_ACTION:
            acknowledged_at = self._deps.now()
            try:
                acknowledgement = self._deps.claim_mark_done(
                    workspace_id=request.tenant_id,
                    channel_id=str(
                        request.metadata.get("channel_id") or ""
                    ),
                    # 새 Slack은 mark-done 요청의 conversation ID에 실제
                    # 클릭 메시지 ts를 보내고 구 Slack은 기존 root ts를 보낸다.
                    message_ts=request.conversation_id,
                    target=_ack_target(action.target),
                    actor_user_id=str(request.actor_id or ""),
                    acknowledged_at=acknowledged_at,
                )
            except Exception as exc:
                self._logger.warning(
                    "Device health alert acknowledgement failed error_type=%s",
                    type(exc).__name__,
                )
                return self._record_action_result(
                    request,
                    action,
                    _result(
                        route=route,
                        outcome="failed",
                        body=(
                            "확인 완료 상태를 저장하지 못해 처리하지 않았어"
                        ),
                        fallback_reason=(
                            "device_health_alert_ack_store_failed"
                        ),
                    ),
                )
            recorded = self._record_action_result(
                request,
                action,
                _result(
                    route=route,
                    outcome="answered",
                    body=(
                        "**장비 이상 알림 처리**\n"
                        f"• 작업: **확인 완료**\n"
                        f"• 대상: `{_format_target(action.target)}`"
                    ),
                    operation_result=_ack_operation_result(
                        action.target,
                        acknowledgement,
                    ),
                ),
            )
            if not request.request_id.startswith(
                DEVICE_HEALTH_ALERT_ACK_REQUEST_ID_PREFIX
            ):
                # 구 Slack은 operationResult가 있으면 계약 오류로 막으므로
                # capability prefix가 없는 요청에는 기존 응답 모양을 유지한다.
                return replace(recorded, operation_result=None)
            return recorded
        return None

    def _record_action_result(
        self,
        request: CompanyAssistantRequest,
        action: DeviceHealthAlertAction,
        result: CompanyAssistantResult,
    ) -> CompanyAssistantResult:
        """API가 실행한 action을 health JSONL 정본에 best-effort로 남긴다."""

        if (
            action.name == DEVICE_HEALTH_ALERT_MARK_DONE_ACTION
            and isinstance(result.operation_result, Mapping)
            and result.operation_result.get("kind")
            == "device_health_alert_ack"
            and result.operation_result.get("created") is False
        ):
            # 영속 claim이 기존 완료를 돌려준 재클릭은 최초 담당자·시간만
            # 재사용하고 JSONL 완료 이벤트를 한 줄 더 만들지 않는다.
            return result
        occurred_at = self._deps.now()
        try:
            self._deps.write_event(
                "alert_action_requested",
                occurred_at,
                {
                    "actionId": action.name,
                    "actionLabel": _DEVICE_HEALTH_ALERT_ACTION_LABELS.get(
                        action.name,
                        action.name,
                    ),
                    "actorUserId": str(request.actor_id or ""),
                    "channelId": str(
                        request.metadata.get("channel_id") or ""
                    ),
                    "messageTs": str(request.conversation_id or ""),
                    "threadTs": str(request.conversation_id or ""),
                    "hospital": (
                        action.target.hospital_label
                        or action.target.hospital_name
                    ),
                    "room": action.target.room_name,
                    "device": action.target.device_name,
                    "issue": action.target.issue,
                    "mdaUrl": action.target.mda_url,
                    "result": {
                        "status": result.outcome,
                        "ok": result.outcome == "answered",
                        "route": result.route,
                        "fallbackReason": str(
                            result.fallback_reason or ""
                        ),
                    },
                },
            )
        except Exception as exc:
            self._logger.warning(
                "Device health action event write failed error_type=%s",
                type(exc).__name__,
            )
        return result

    def _record_ui_receipt(
        self,
        request: CompanyAssistantRequest,
        receipt: DeviceHealthAlertUiReceipt,
    ) -> CompanyAssistantResult:
        """API가 알 수 없던 Slack modal 결과를 중앙 JSONL에 그대로 쓴다."""

        event_result: dict[str, Any] = {
            "status": receipt.status,
            "ok": receipt.ok,
        }
        if receipt.error_type:
            event_result["error"] = receipt.error_type
        try:
            self._deps.write_event(
                receipt.event_type,
                receipt.occurred_at,
                {
                    "actionId": receipt.action_id,
                    "mode": receipt.mode,
                    "actorUserId": str(request.actor_id or ""),
                    "channelId": str(
                        request.metadata.get("channel_id") or ""
                    ),
                    "messageTs": receipt.message_ts,
                    "threadTs": receipt.thread_ts,
                    "hospital": (
                        receipt.target.hospital_label
                        or receipt.target.hospital_name
                    ),
                    "room": receipt.target.room_name,
                    "device": receipt.target.device_name,
                    "issue": receipt.target.issue,
                    "mdaUrl": receipt.target.mda_url,
                    "result": event_result,
                },
            )
        except Exception as exc:
            # legacy local writer도 Slack action 성공 여부와 event I/O를 분리했다.
            self._logger.warning(
                "Device health UI receipt event write failed error_type=%s",
                type(exc).__name__,
            )
        return _result(
            route=DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE,
            outcome="answered",
            body="장비 이상 알림 UI 결과를 기록했어",
        )

    def _prepare_sms(
        self,
        action: DeviceHealthAlertAction,
        exact_target: Mapping[str, Any],
    ) -> CompanyAssistantResult:
        """버튼 click의 DB 조회 결과를 requester 전용 모달 입력값으로 돌려준다."""

        guide = _build_device_health_alert_sms_guide(action.target)
        phone_number = _normalize_phone_number(
            exact_target.get("deviceAlertPhone")
        )
        if not _is_mobile_phone_number(phone_number):
            phone_number = ""
        operation_result = {
            "kind": "sms_contact_preparation",
            "deliveryScope": "requester",
            "phoneNumber": phone_number,
            "message": str(guide.get("message") or "").strip(),
            "templateId": str(
                guide.get("templateId") or "unsupported_issue"
            ).strip(),
            "target": {
                "hospital": action.target.hospital_name,
                "room": action.target.room_name,
                "device": action.target.device_name,
                "components": list(action.target.problem_components),
                "issue": action.target.issue,
            },
        }
        return _result(
            route=DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
            outcome="answered",
            body="병원 문자 입력 정보를 준비했어",
            delivery_scope="requester",
            operation_result=operation_result,
        )

    def _send_sms(
        self,
        request: CompanyAssistantRequest,
        action: DeviceHealthAlertAction,
        exact_target: Mapping[str, Any],
    ) -> CompanyAssistantResult:
        phone_number = _normalize_phone_number(action.sms_phone_number)
        if not _is_mobile_phone_number(phone_number):
            return _result(
                route=DEVICE_HEALTH_ALERT_SMS_ROUTE,
                outcome="needs_input",
                body="병원 문자 수신 번호가 휴대전화 형식이 아니라 보내지 않았어",
                fallback_reason="invalid_sms_phone_number",
            )
        message = str(action.sms_message or "").strip()
        if not message:
            return _result(
                route=DEVICE_HEALTH_ALERT_SMS_ROUTE,
                outcome="needs_input",
                body="병원 문자 내용이 비어 있어 보내지 않았어",
                fallback_reason="missing_sms_message",
            )

        guide = _build_device_health_alert_sms_guide(action.target)
        default_message = str(guide.get("message") or "").strip()
        template_id = str(guide.get("templateId") or "manual").strip()
        if not default_message or message != default_message:
            template_id = "manual"
        payload = {
            "actionId": action.name,
            "requestType": "sms",
            "createdAt": self._deps.now().isoformat(),
            "actorUserId": str(request.actor_id or ""),
            "hospital": {
                "seq": int(action.target.hospital_seq),
                "name": str(exact_target.get("hospitalName") or ""),
                "phoneNumber": phone_number,
            },
            "device": {
                "name": action.target.device_name,
                "room": str(exact_target.get("roomName") or ""),
                "issue": action.target.issue,
            },
            "sms": {
                "to": phone_number,
                "templateId": template_id,
                "message": message,
                "testMode": False,
            },
            # 외부 provider payload에는 Slack ts 대신 중립적인 요청 원천만 보낸다.
            "origin": {
                "channel": request.channel,
                "conversationId": request.conversation_id,
                "requestId": request.request_id,
            },
        }
        send_result = self._deps.send_sms(payload, logger=self._logger)
        status = str(send_result.get("status") or "").strip()
        operation_result = _build_sms_operation_result(
            action,
            send_result,
            accepted_at=str(payload["createdAt"]),
        )
        group_id = str(operation_result.get("groupId") or "").strip()
        if group_id:
            try:
                # provider receipt는 발송을 실행한 API 프로세스의 outbox에
                # 먼저 저장한다. Slack callback은 이 상태를 복제하지 않고
                # 사용자 메시지만 전달한다.
                remembered = self._deps.remember_sms_delivery(
                    {
                        "hospital": action.target.hospital_name,
                        "room": action.target.room_name,
                        "device": action.target.device_name,
                        "problemComponents": list(
                            action.target.problem_components
                        ),
                        "issue": action.target.issue,
                        "smsDeliveryStatus": operation_result.get(
                            "deliveryStatus"
                        ),
                        "smsGroupId": group_id,
                        "smsAcceptedAt": operation_result.get(
                            "acceptedAt"
                        ),
                    },
                    detected_at=str(payload["createdAt"]),
                    sms_accepted_at=str(
                        operation_result.get("acceptedAt") or ""
                    ),
                )
            except Exception as exc:
                self._logger.warning(
                    "장비 이상 문자 receipt 저장 실패 error_type=%s",
                    type(exc).__name__,
                )
                remembered = False
            if not remembered:
                return _result(
                    route=DEVICE_HEALTH_ALERT_SMS_ROUTE,
                    outcome="failed",
                    body=(
                        "문자 발송 결과 보관에 실패했어. "
                        "중복 발송하지 말고 운영 로그를 확인해줘"
                    ),
                    fallback_reason="sms_delivery_receipt_persist_failed",
                    operation_result=operation_result,
                )
        if bool(send_result.get("ok")) and status == "sent":
            delivery_status = str(
                send_result.get("smsDeliveryStatus") or ""
            ).strip()
            qualifier = (
                "공급자 접수 완료"
                if delivery_status == _SMS_DELIVERY_ACCEPTED
                else (
                    "전달 완료"
                    if delivery_status == _SMS_DELIVERY_DELIVERED
                    else "발송 요청 완료"
                )
            )
            return _result(
                route=DEVICE_HEALTH_ALERT_SMS_ROUTE,
                outcome="answered",
                body=(
                    "**장비 이상 알림 처리**\n"
                    f"• 작업: **병원 문자 {qualifier}**\n"
                    f"• 대상: `{_format_target(action.target)}`\n"
                    f"• 템플릿: `{template_id}`"
                ),
                operation_result=operation_result,
            )
        if str(send_result.get("smsDeliveryStatus") or "") == (
            _SMS_DELIVERY_CONFIRM_REQUIRED
        ):
            return _result(
                route=DEVICE_HEALTH_ALERT_SMS_ROUTE,
                outcome="failed",
                body=(
                    "문자 발송 결과를 확인하지 못했어. "
                    "중복 발송 방지를 위해 자동 재시도하지 않았어"
                ),
                fallback_reason="sms_delivery_confirmation_required",
                operation_result=operation_result,
            )
        return _result(
            route=DEVICE_HEALTH_ALERT_SMS_ROUTE,
            outcome="failed",
            body="병원 문자 발송 요청이 실패했어. 자동 재시도하지 않았어",
            fallback_reason="sms_delivery_failed",
            operation_result=operation_result,
        )

    def _send_voice(
        self,
        request: CompanyAssistantRequest,
        action: DeviceHealthAlertAction,
    ) -> CompanyAssistantResult:
        del request
        if not _is_voice_guide_supported(action.target):
            return _result(
                route=DEVICE_HEALTH_ALERT_VOICE_ROUTE,
                outcome="denied",
                body="이 알림은 장비 음성 안내 대상이 아니야",
                fallback_reason="unsupported_voice_guide_issue",
            )
        try:
            device_detail = self._deps.get_mda_device(
                action.target.device_name
            )
        except Exception as exc:
            self._logger.warning(
                "장비 이상 음성 안내 MDA 사전 확인 실패 device=%s error_type=%s",
                action.target.device_name,
                type(exc).__name__,
            )
            return _result(
                route=DEVICE_HEALTH_ALERT_VOICE_ROUTE,
                outcome="failed",
                body="MDA에서 장비 상태를 확인하지 못해 음성 안내를 보내지 않았어",
                fallback_reason="voice_guide_precheck_failed",
            )
        if not isinstance(device_detail, dict):
            return _result(
                route=DEVICE_HEALTH_ALERT_VOICE_ROUTE,
                outcome="denied",
                body="MDA에서 대상 장비를 찾지 못했어",
                fallback_reason="voice_guide_device_not_found",
            )
        version = str(device_detail.get("version") or "").strip()
        parsed_version = _parse_version(version)
        if (
            parsed_version is None
            or parsed_version < _DEVICE_HEALTH_ALERT_VOICE_MINIMUM_VERSION
        ):
            return _result(
                route=DEVICE_HEALTH_ALERT_VOICE_ROUTE,
                outcome="denied",
                body=(
                    "장비 음성 안내는 "
                    f"`{_DEVICE_HEALTH_ALERT_VOICE_MINIMUM_VERSION_TEXT}` 이상에서 가능해"
                ),
                fallback_reason="voice_guide_unsupported_version",
            )
        if not bool(device_detail.get("deviceIsConnected")):
            return _result(
                route=DEVICE_HEALTH_ALERT_VOICE_ROUTE,
                outcome="denied",
                body="장비가 MDA에 연결되어 있지 않아 음성 안내를 보내지 않았어",
                fallback_reason="voice_guide_device_offline",
            )

        # command 직전에 claim을 먼저 잡아 timeout/불명 응답에도 같은 장비를
        # 짧은 시간 다시 누르지 못하게 한다. mutation은 자동 재시도하지 않는다.
        claim = self._deps.claim_voice_guide(
            action.target.device_name.casefold(),
            self._deps.now(),
        )
        if not bool(claim.get("claimed")):
            remaining = max(1, int(claim.get("remainingSeconds") or 0))
            return _result(
                route=DEVICE_HEALTH_ALERT_VOICE_ROUTE,
                outcome="denied",
                body=f"최근에 같은 장비에 음성 안내를 보냈어. 약 {remaining}초 뒤 다시 가능해",
                fallback_reason="voice_guide_cooldown",
            )
        try:
            dispatch = _dispatch_device_voice_guide(
                action.target.device_name,
                command_dispatcher=self._deps.send_mda_command,
            )
        except Exception as exc:
            self._logger.warning(
                "장비 이상 음성 안내 결과 불명 device=%s error_type=%s",
                action.target.device_name,
                type(exc).__name__,
            )
            return _result(
                route=DEVICE_HEALTH_ALERT_VOICE_ROUTE,
                outcome="failed",
                body=(
                    "음성 안내 명령의 처리 결과를 확인하지 못했어. "
                    "중복 재생 방지를 위해 자동 재시도하지 않았어"
                ),
                fallback_reason="voice_guide_dispatch_uncertain",
            )
        if not isinstance(dispatch, dict) or not bool(dispatch.get("status")):
            return _result(
                route=DEVICE_HEALTH_ALERT_VOICE_ROUTE,
                outcome="failed",
                body="MDA가 음성 안내 명령을 처리하지 못했어",
                fallback_reason="voice_guide_dispatch_failed",
            )
        return _result(
            route=DEVICE_HEALTH_ALERT_VOICE_ROUTE,
            outcome="answered",
            body=(
                "**장비 이상 알림 처리**\n"
                "• 작업: **장비 음성 안내 명령 전송 완료**\n"
                f"• 대상: `{_format_target(action.target)}`"
            ),
        )


def _parse_device_health_alert_action(
    metadata: Mapping[str, Any],
) -> DeviceHealthAlertAction | None:
    raw_action = metadata.get("operation_action")
    if not isinstance(raw_action, Mapping):
        return None
    name = str(raw_action.get("name") or "").strip()
    if name not in {
        DEVICE_HEALTH_ALERT_SMS_ACTION,
        *_DEVICE_HEALTH_ALERT_ACTION_ROUTES,
    }:
        return None
    phase = str(raw_action.get("phase") or "").strip()
    if (
        name == DEVICE_HEALTH_ALERT_SMS_ACTION
        and phase not in {"prepare", "execute"}
    ) or (
        name != DEVICE_HEALTH_ALERT_SMS_ACTION
        and phase != "execute"
    ):
        return None
    raw_target = raw_action.get("target")
    if not isinstance(raw_target, Mapping):
        return None
    try:
        hospital_seq = int(raw_target.get("hospital_seq") or 0)
    except (TypeError, ValueError):
        return None
    hospital_name = " ".join(
        str(raw_target.get("hospital_name") or "").split()
    )
    room_name = " ".join(str(raw_target.get("room_name") or "").split())
    device_name = str(raw_target.get("device_name") or "").strip()
    issue = " ".join(str(raw_target.get("issue") or "").split())
    alert_category = str(raw_target.get("alert_category") or "").strip()
    raw_components = raw_target.get("problem_components")
    components = (
        tuple(
            " ".join(str(component or "").split())
            for component in raw_components
            if " ".join(str(component or "").split())
        )
        if isinstance(raw_components, (list, tuple))
        else ()
    )
    if (
        hospital_seq <= 0
        or not hospital_name
        or not room_name
        or not device_name
        or not company_settings.S3_DEVICE_NAME_PATTERN.fullmatch(device_name)
        or not issue
    ):
        return None
    raw_sms = raw_action.get("sms")
    sms = raw_sms if isinstance(raw_sms, Mapping) else {}
    return DeviceHealthAlertAction(
        name=name,
        phase=phase,
        target=DeviceHealthAlertActionTarget(
            hospital_seq=hospital_seq,
            hospital_name=hospital_name,
            room_name=room_name,
            device_name=device_name,
            issue=issue,
            alert_category=alert_category,
            problem_components=components,
            hospital_label=" ".join(
                str(raw_target.get("hospital_label") or "").split()
            ),
            mda_url=str(raw_target.get("mda_url") or "").strip(),
        ),
        sms_phone_number=str(sms.get("phone_number") or "").strip(),
        sms_message=str(sms.get("message") or "").strip(),
    )


def _parse_device_health_alert_ui_receipt(
    metadata: Mapping[str, Any],
) -> DeviceHealthAlertUiReceipt | None:
    """HTTP schema를 우회한 runtime 호출에서도 receipt 필드를 재검증한다."""

    raw_action = metadata.get("operation_action")
    if not isinstance(raw_action, Mapping):
        return None
    if (
        str(raw_action.get("name") or "").strip()
        != DEVICE_HEALTH_ALERT_UI_RECEIPT_ACTION
        or str(raw_action.get("phase") or "").strip() != "receipt"
        or str(raw_action.get("event_type") or "").strip()
        != "alert_contact_sms_modal_requested"
    ):
        return None
    action_id = str(raw_action.get("action_id") or "").strip()
    mode = str(raw_action.get("mode") or "").strip()
    status = str(raw_action.get("status") or "").strip()
    ok = raw_action.get("ok")
    error_type = str(raw_action.get("error_type") or "").strip()
    if (
        action_id
        not in {
            DEVICE_HEALTH_ALERT_SMS_ACTION,
            "device_health_alert_view_auto_sms",
        }
        or mode not in {"send", "view_auto_sent"}
        or status
        not in {
            "missing_trigger_id",
            "modal_opened",
            "modal_open_failed",
        }
        or not isinstance(ok, bool)
        or ok != (status == "modal_opened")
        or (error_type and status != "modal_open_failed")
        or len(error_type) > 160
    ):
        return None
    raw_target = raw_action.get("target")
    if not isinstance(raw_target, Mapping):
        return None
    try:
        hospital_seq = int(raw_target.get("hospital_seq") or 0)
        occurred_at = datetime.fromisoformat(
            str(raw_action.get("occurred_at") or "")
            .strip()
            .replace("Z", "+00:00")
        )
    except (TypeError, ValueError):
        return None
    hospital_name = " ".join(
        str(raw_target.get("hospital_name") or "").split()
    )
    room_name = " ".join(
        str(raw_target.get("room_name") or "").split()
    )
    device_name = str(raw_target.get("device_name") or "").strip()
    issue = " ".join(str(raw_target.get("issue") or "").split())
    message_ts = str(raw_action.get("message_ts") or "").strip()
    thread_ts = str(raw_action.get("thread_ts") or "").strip()
    timestamp_pattern = r"^\d{1,20}(?:\.\d{1,9})?$"
    if (
        occurred_at.tzinfo is None
        or hospital_seq <= 0
        or not hospital_name
        or not room_name
        or not company_settings.S3_DEVICE_NAME_PATTERN.fullmatch(device_name)
        or not issue
        or not re.fullmatch(timestamp_pattern, message_ts)
        or not re.fullmatch(timestamp_pattern, thread_ts)
    ):
        return None
    raw_components = raw_target.get("problem_components")
    components = (
        tuple(
            " ".join(str(component or "").split())
            for component in raw_components
            if " ".join(str(component or "").split())
        )
        if isinstance(raw_components, (list, tuple))
        else ()
    )
    return DeviceHealthAlertUiReceipt(
        event_type="alert_contact_sms_modal_requested",
        action_id=action_id,
        mode=mode,
        target=DeviceHealthAlertActionTarget(
            hospital_seq=hospital_seq,
            hospital_name=hospital_name,
            room_name=room_name,
            device_name=device_name,
            issue=issue,
            alert_category=str(
                raw_target.get("alert_category") or ""
            ).strip(),
            problem_components=components,
            hospital_label=" ".join(
                str(raw_target.get("hospital_label") or "").split()
            ),
            mda_url=str(raw_target.get("mda_url") or "").strip(),
        ),
        message_ts=message_ts,
        thread_ts=thread_ts,
        occurred_at=occurred_at,
        status=status,
        ok=ok,
        error_type=error_type,
    )


def _load_exact_device_health_alert_target(
    target: DeviceHealthAlertActionTarget,
) -> dict[str, Any] | None:
    """버튼 payload의 병원/병실/장비가 현재 DB 한 row와 정확히 맞는지 확인한다."""

    connection = _create_db_connection(core_settings.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT d.deviceName, hr.roomName, h.seq AS hospitalSeq, "
                "h.hospitalName, h.telephone, h.deviceAlertPhone "
                "FROM devices d "
                "INNER JOIN hospitals h ON d.hospitalSeq = h.seq "
                "LEFT JOIN hospital_rooms hr ON d.hospitalRoomSeq = hr.seq "
                "WHERE d.deviceName = %s AND h.seq = %s LIMIT 2",
                (target.device_name, int(target.hospital_seq)),
            )
            rows = cursor.fetchall() or []
    finally:
        connection.close()
    if len(rows) != 1:
        return None
    row = rows[0]
    if (
        str(row.get("deviceName") or "").casefold()
        != target.device_name.casefold()
        or " ".join(str(row.get("hospitalName") or "").split())
        != target.hospital_name
        or " ".join(str(row.get("roomName") or "").split())
        != target.room_name
    ):
        return None
    return dict(row)


def _build_device_health_alert_sms_guide(
    target: DeviceHealthAlertActionTarget,
) -> dict[str, Any]:
    """알림 범주를 병원용 고정 문자 템플릿으로만 변환한다."""

    issue = target.issue
    lowered = issue.lower()
    components = {component.replace(" ", "") for component in target.problem_components}
    prefix = (
        f"{_DEVICE_HEALTH_ALERT_SMS_GREETING}\n\n"
        f"{target.room_name} {target.device_name}"
    )
    if target.alert_category == "recording_processing" or any(
        marker in lowered for marker in ("병합", "ffmpeg", "merge")
    ):
        return {
            "supported": True,
            "templateId": "recording_merge_failed",
            "message": (
                f"{prefix}에서 녹화 파일 저장 처리 문제를 감지했습니다.\n\n"
                "마미톡 담당자가 확인할 예정이며 병원에서 별도로 조치하실 내용은 없습니다.\n"
                "추가 확인이 필요하면 담당자가 다시 연락드리겠습니다."
            ),
        }
    if target.alert_category == "recording" or any(
        marker in lowered
        for marker in ("녹화 파일 증가 정지", "파일 증가 속도", "recording stalled")
    ):
        return {
            "supported": True,
            "templateId": "recording_stalled",
            "message": (
                f"{prefix}에서 녹화 영상 입력이 정상적으로 이어지지 않아 연결 확인이 필요합니다.\n\n"
                "초음파 진단기와 캡처보드를 연결한 영상 케이블과 캡처보드와 마미박스를 연결한 USB 케이블을 각각 분리했다가 다시 단단히 연결해 주세요.\n"
                "연결 후에도 같은 문제가 반복되면 마미톡 담당자에게 알려 주세요."
            ),
        }
    if (
        target.alert_category == "video_signal"
        or any(marker in issue for marker in ("캡처보드", "캡쳐보드", "비디오 장치", "영상"))
        or "캡처보드" in components
    ):
        return {
            "supported": True,
            "templateId": "captureboard_disconnected",
            "message": (
                f"{prefix}에서 초음파 영상 입력 장치 연결 확인이 필요합니다.\n\n"
                "초음파 진단기와 캡처보드를 연결한 영상 케이블과 캡처보드와 마미박스를 연결한 USB 케이블을 각각 분리했다가 다시 단단히 연결해 주세요.\n"
                "케이블이 빠져 있거나 헐거우면 영상이 정상적으로 들어오지 않을 수 있습니다."
            ),
        }
    if (
        target.alert_category == "led"
        or "LED" in components
        or "led" in lowered
        or "엘이디" in issue
    ):
        return {
            "supported": True,
            "templateId": "led_disconnected",
            "message": (
                f"{prefix}에서 장비 상태 표시등 연결 확인이 필요합니다.\n\n"
                "먼저 LED USB 케이블을 분리했다가 다시 단단히 연결해 주세요.\n"
                "케이블이 빠져 있거나 헐거우면 상태 표시등이 정상적으로 동작하지 않을 수 있습니다."
            ),
        }
    if any(token in lowered for token in ("audio", "sound", "speaker")) or any(
        token in issue for token in ("오디오", "소리", "스피커")
    ):
        return {
            "supported": True,
            "templateId": "audio_output_check",
            "message": (
                f"{prefix}에서 소리 출력 상태 확인이 필요합니다.\n\n"
                "먼저 스피커 전원과 오디오 케이블을 분리했다가 다시 단단히 연결해 주세요.\n"
                "케이블이 빠져 있거나 입력 소스가 맞지 않으면 소리가 나오지 않을 수 있습니다."
            ),
        }
    return {
        "supported": False,
        "templateId": "unsupported_issue",
        "message": "",
    }


def _send_device_health_alert_sms(
    payload: dict[str, Any],
    *,
    logger: logging.Logger,
) -> dict[str, Any]:
    """설정된 provider를 한 번만 호출하고 불명 응답은 재시도하지 않는다."""

    provider = str(company_settings.DEVICE_HEALTH_MONITOR_SMS_PROVIDER or "").strip().lower()
    if provider == "solapi":
        return _send_device_health_alert_solapi_sms(payload, logger=logger)
    if provider in {"webhook", "http"}:
        url = str(company_settings.DEVICE_HEALTH_MONITOR_SMS_WEBHOOK_URL or "").strip()
        if not url:
            return {"status": "not_configured", "ok": False}
        try:
            timeout_sec = max(
                1,
                int(
                    company_settings.DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_TIMEOUT_SEC
                ),
            )
        except (TypeError, ValueError):
            return {
                "status": "error",
                "ok": False,
                "provider": "webhook",
                "smsDeliveryStatus": _SMS_DELIVERY_REQUEST_FAILED,
            }
        try:
            _mark_company_api_mutation_attempted()
            response = requests.post(
                url,
                json=payload,
                timeout=timeout_sec,
            )
        except Exception as exc:
            logger.warning(
                "장비 이상 알림 SMS webhook 결과 불명 error_type=%s",
                type(exc).__name__,
            )
            return {
                "status": "error",
                "ok": False,
                "provider": "webhook",
                "smsDeliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
            }
        if 200 <= int(response.status_code) < 300:
            return {
                "status": "sent",
                "ok": True,
                "provider": "webhook",
                "statusCode": int(response.status_code),
                "smsDeliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
            }
        return {
            "status": "error",
            "ok": False,
            "provider": "webhook",
            "statusCode": int(response.status_code),
            "smsDeliveryStatus": (
                _SMS_DELIVERY_REQUEST_FAILED
                if 400 <= int(response.status_code) < 500
                and int(response.status_code) != 408
                else _SMS_DELIVERY_CONFIRM_REQUIRED
            ),
        }
    return {"status": "not_configured", "ok": False}


def _send_device_health_alert_solapi_sms(
    payload: dict[str, Any],
    *,
    logger: logging.Logger,
) -> dict[str, Any]:
    if not (
        company_settings.SOLAPI_API_KEY
        and company_settings.SOLAPI_API_SECRET
        and company_settings.SOLAPI_FROM_NUMBER
    ):
        return {"status": "not_configured", "ok": False}
    sms = payload.get("sms") if isinstance(payload.get("sms"), dict) else {}
    to_number = _normalize_phone_number(sms.get("to"))
    from_number = _normalize_phone_number(company_settings.SOLAPI_FROM_NUMBER)
    message = str(sms.get("message") or "").strip()
    if not to_number or not from_number or not message:
        return {"status": "invalid_sms_payload", "ok": False}
    request_payload = {
        "messages": [
            {
                "to": to_number,
                "from": from_number,
                "text": message,
                "type": _sms_type(message),
                "country": "82",
            }
        ],
        "showMessageList": True,
    }
    try:
        request_url = (
            f"{company_settings.SOLAPI_BASE_URL.rstrip('/')}"
            "/messages/v4/send-many/detail"
        )
        request_headers = {
            "Authorization": _build_solapi_authorization_header(),
            "Content-Type": "application/json",
        }
        timeout_sec = max(
            1,
            int(
                company_settings.DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_TIMEOUT_SEC
            ),
        )
    except Exception as exc:
        logger.warning(
            "장비 이상 알림 Solapi 요청 구성 실패 error_type=%s",
            type(exc).__name__,
        )
        return {
            "status": "error",
            "ok": False,
            "provider": "solapi",
            "smsDeliveryStatus": _SMS_DELIVERY_REQUEST_FAILED,
        }
    try:
        _mark_company_api_mutation_attempted()
        response = requests.post(
            request_url,
            json=request_payload,
            headers=request_headers,
            timeout=timeout_sec,
        )
    except Exception as exc:
        logger.warning(
            "장비 이상 알림 Solapi 결과 불명 error_type=%s",
            type(exc).__name__,
        )
        return {
            "status": "error",
            "ok": False,
            "provider": "solapi",
            "smsDeliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
        }
    try:
        response_payload = response.json()
    except ValueError:
        response_payload = {}
    if not 200 <= int(response.status_code) < 300:
        return {
            "status": "error",
            "ok": False,
            "provider": "solapi",
            "statusCode": int(response.status_code),
            "smsDeliveryStatus": (
                _SMS_DELIVERY_REQUEST_FAILED
                if 400 <= int(response.status_code) < 500
                and int(response.status_code) != 408
                else _SMS_DELIVERY_CONFIRM_REQUIRED
            ),
        }
    group = response_payload.get("groupInfo") if isinstance(response_payload, dict) else {}
    messages = response_payload.get("messageList") if isinstance(response_payload, dict) else []
    failed = response_payload.get("failedMessageList") if isinstance(response_payload, dict) else []
    group = group if isinstance(group, dict) else {}
    messages = messages if isinstance(messages, list) else []
    failed = failed if isinstance(failed, list) else []
    first = messages[0] if messages and isinstance(messages[0], dict) else {}
    first_failed = failed[0] if failed and isinstance(failed[0], dict) else {}
    group_id = str(group.get("groupId") or response_payload.get("groupId") or "")
    message_id = str(first.get("messageId") or first_failed.get("messageId") or "")
    provider_status = str(first.get("statusCode") or first_failed.get("statusCode") or "")
    if failed or (provider_status and provider_status not in {"2000", "3000", "4000"}):
        return {
            "status": "error",
            "ok": False,
            "provider": "solapi",
            "groupId": group_id,
            "messageId": message_id,
            "providerStatusCode": provider_status,
            "smsDeliveryStatus": _SMS_DELIVERY_FAILED,
        }
    delivery_status = (
        _SMS_DELIVERY_DELIVERED
        if provider_status == "4000"
        else (
            _SMS_DELIVERY_ACCEPTED
            if group_id
            else _SMS_DELIVERY_CONFIRM_REQUIRED
        )
    )
    return {
        "status": "sent",
        "ok": True,
        "provider": "solapi",
        "groupId": group_id,
        "messageId": message_id,
        "providerStatusCode": provider_status,
        "smsDeliveryStatus": delivery_status,
    }


def _claim_device_health_alert_voice_guide(
    claim_key: str,
    now: datetime,
) -> dict[str, Any]:
    cooldown_seconds = max(
        0,
        int(company_settings.DEVICE_HEALTH_MONITOR_VOICE_GUIDE_COOLDOWN_SEC),
    )
    with _DEVICE_HEALTH_ALERT_VOICE_CLAIMS_LOCK:
        previous = _DEVICE_HEALTH_ALERT_VOICE_CLAIMS.get(claim_key)
        if previous is not None:
            elapsed = max(0.0, (now - previous).total_seconds())
            if elapsed < cooldown_seconds:
                return {
                    "claimed": False,
                    "remainingSeconds": max(1, int(cooldown_seconds - elapsed)),
                }
        _DEVICE_HEALTH_ALERT_VOICE_CLAIMS[claim_key] = now
    return {"claimed": True, "remainingSeconds": 0}


def _normalize_phone_number(value: Any) -> str:
    text = str(value or "").strip()
    if text.startswith("+82"):
        return "0" + re.sub(r"\D", "", text[3:])
    if text.startswith("82"):
        return "0" + re.sub(r"\D", "", text[2:])
    return re.sub(r"\D", "", text)


def _is_mobile_phone_number(value: Any) -> bool:
    normalized = _normalize_phone_number(value)
    return bool(_DEVICE_HEALTH_ALERT_PHONE_PATTERN.fullmatch(normalized))


def _sms_type(message: str) -> str:
    return "SMS" if len(message.encode("euc-kr", errors="replace")) <= 90 else "LMS"


def _parse_version(value: Any) -> tuple[int, int, int] | None:
    matched = re.fullmatch(r"v?(\d+)\.(\d+)\.(\d+)", str(value or "").strip())
    if matched is None:
        return None
    return tuple(int(matched.group(index)) for index in range(1, 4))  # type: ignore[return-value]


def _is_voice_guide_supported(target: DeviceHealthAlertActionTarget) -> bool:
    if target.alert_category in _DEVICE_HEALTH_ALERT_VOICE_CATEGORIES:
        return True
    return (
        target.alert_category == "mixed"
        and "캡처보드" in target.problem_components
    )


def _format_target(target: DeviceHealthAlertActionTarget) -> str:
    return " / ".join(
        (target.hospital_name, target.room_name, target.device_name)
    )


def _ack_target(target: DeviceHealthAlertActionTarget) -> dict[str, Any]:
    return {
        "hospitalSeq": target.hospital_seq,
        "hospitalName": target.hospital_name,
        "hospital": target.hospital_label,
        "room": target.room_name,
        "device": target.device_name,
        "issue": target.issue,
    }


def _ack_operation_result(
    target: DeviceHealthAlertActionTarget,
    acknowledgement: DeviceHealthAlertAcknowledgement,
) -> dict[str, Any]:
    """Slack UI가 최초 담당자·시간만 표시하도록 고정 receipt를 만든다."""

    return {
        "kind": "device_health_alert_ack",
        "created": acknowledgement.created,
        "actorUserId": acknowledgement.actor_user_id,
        "acknowledgedAt": acknowledgement.acknowledged_at.isoformat(),
        "target": {
            "hospital": target.hospital_name,
            "room": target.room_name,
            "device": target.device_name,
            "components": list(target.problem_components),
            "issue": target.issue,
        },
    }


def _build_sms_operation_result(
    action: DeviceHealthAlertAction,
    send_result: Mapping[str, Any],
    *,
    accepted_at: str,
) -> dict[str, Any] | None:
    """delivery poll에 필요한 non-PII receipt만 transport metadata로 남긴다."""

    group_id = str(send_result.get("groupId") or "").strip()
    if not group_id:
        return None
    return {
        "kind": "sms_delivery",
        "provider": str(send_result.get("provider") or "").strip(),
        "deliveryStatus": str(
            send_result.get("smsDeliveryStatus") or ""
        ).strip(),
        "groupId": group_id,
        "messageId": str(send_result.get("messageId") or "").strip(),
        "acceptedAt": accepted_at,
        # 전화번호와 문자 본문은 receipt에 넣지 않는다. 기존 outbox가 쓰는
        # 장애 식별값만 Slack/API 어느 쪽에서도 별도 렌더링하지 않고 전달한다.
        "target": {
            "hospital": action.target.hospital_name,
            "room": action.target.room_name,
            "device": action.target.device_name,
            "components": list(action.target.problem_components),
            "issue": action.target.issue,
        },
    }


def _result(
    *,
    route: str,
    outcome: str,
    body: str,
    fallback_reason: str | None = None,
    operation_result: Mapping[str, Any] | None = None,
    delivery_scope: str = "conversation",
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route=route,
        outcome=outcome,  # type: ignore[arg-type]
        messages=(
            AssistantMessage(
                body=body,
                delivery_scope=delivery_scope,  # type: ignore[arg-type]
            ),
        ),
        fallback_reason=fallback_reason,
        operation_result=operation_result,
    )


__all__ = [
    "DEVICE_HEALTH_ALERT_MARK_DONE_ACTION",
    "DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE",
    "DEVICE_HEALTH_ALERT_SMS_ACTION",
    "DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE",
    "DEVICE_HEALTH_ALERT_SMS_ROUTE",
    "DEVICE_HEALTH_ALERT_UI_RECEIPT_ACTION",
    "DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE",
    "DEVICE_HEALTH_ALERT_VOICE_ACTION",
    "DEVICE_HEALTH_ALERT_VOICE_ROUTE",
    "DeviceHealthAlertAction",
    "DeviceHealthAlertActionAssistantRoute",
    "DeviceHealthAlertActionRouteDeps",
    "DeviceHealthAlertActionTarget",
    "DeviceHealthAlertUiReceipt",
    "match_device_health_alert_action_route",
]
