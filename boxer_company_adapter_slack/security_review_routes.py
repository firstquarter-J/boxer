from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import logging
import re
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from boxer_adapter_slack.common import (
    MentionPayload,
    MessagePayload,
    SlackMessageReplyFn,
    SlackReplyFn,
    _merge_request_log_metadata,
    _set_request_log_route,
    _set_request_log_skip_persist,
    _set_request_log_status,
)
from boxer_company_adapter_slack.assistant_bridge import (
    _commonmark_to_slack,
    build_company_assistant_request,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiContractError,
    CompanyAssistantApiClient,
)


_SECURITY_REVIEW_ROUTE = "security_review"
_MENTION_RE = re.compile(r"<@([A-Z0-9]+)(?:\|[^>]+)?>")


@dataclass(frozen=True)
class SecurityReviewRoutesContext:
    question: str
    payload: MentionPayload
    user_id: str | None
    channel_id: str
    thread_ts: str
    reply: SlackReplyFn
    client: Any
    logger: logging.Logger
    api_client: CompanyAssistantApiClient | None = None


@dataclass(frozen=True)
class SecurityReviewMessageContext:
    payload: MessagePayload
    reply: SlackMessageReplyFn
    client: Any
    logger: logging.Logger
    api_client: CompanyAssistantApiClient | None = None


def _looks_like_security_review_request(question: str) -> bool:
    text = str(question or "").strip()
    lowered = text.casefold()
    return bool(text) and (
        ("보안" in text and any(token in text for token in ("검토", "검증", "테스트", "취약")))
        or "취약점" in text
        or "인젝션" in text
        or "injection" in lowered
        or "security review" in lowered
        or "security test" in lowered
        or (
            any(token in text for token in ("검증", "테스트"))
            and any(token in text for token in ("봇", "멘션", "호출"))
        )
    )


def _looks_like_security_review_summary_request(question: str) -> bool:
    text = str(question or "").strip()
    return bool(text) and "보안" in text and any(
        token in text for token in ("결과", "요약", "상태", "마무리")
    )


def _get_self_bot_user_id(client: Any, logger: logging.Logger) -> str:
    try:
        response = client.auth_test()
    except Exception as exc:
        logger.warning(
            "Boxer bot identity lookup failed error_type=%s",
            type(exc).__name__,
        )
        return ""
    return str((response or {}).get("user_id") or "").strip()


def _extract_target_user_id(raw_text: str, self_user_id: str) -> str:
    mentions = _MENTION_RE.findall(raw_text or "")
    if self_user_id:
        return next((item for item in mentions if item != self_user_id), "")
    return mentions[1] if len(mentions) >= 2 else ""


def _resolve_target_bot(
    client: Any,
    target_user_id: str,
    logger: logging.Logger,
) -> dict[str, str | bool]:
    try:
        response = client.users_info(user=target_user_id)
    except Exception as exc:
        logger.warning(
            "Target bot identity lookup failed error_type=%s",
            type(exc).__name__,
        )
        return {"ok": False, "is_bot": True}
    user = (response or {}).get("user")
    user = user if isinstance(user, dict) else {}
    profile = user.get("profile")
    profile = profile if isinstance(profile, dict) else {}
    bot_id = str(user.get("bot_id") or profile.get("bot_id") or "").strip()
    app_id = str(
        user.get("app_id")
        or profile.get("api_app_id")
        or profile.get("app_id")
        or ""
    ).strip()
    name = str(
        profile.get("display_name")
        or profile.get("real_name")
        or user.get("real_name")
        or user.get("name")
        or ""
    ).strip()
    return {
        "ok": True,
        "is_bot": bool(user.get("is_bot") or bot_id or app_id),
        "target_bot_id": bot_id,
        "target_app_id": app_id,
        "target_name": name,
    }


def _is_bot_message(payload: MessagePayload) -> bool:
    subtype = str(payload.get("subtype") or "").strip()
    return subtype == "bot_message" if subtype else bool(
        str(payload.get("bot_id") or "").strip()
        or str(payload.get("app_id") or "").strip()
    )


def _remote_security_review_call(
    *,
    payload: MentionPayload | MessagePayload,
    api_client: CompanyAssistantApiClient | None,
    phase: str,
    target: dict[str, str] | None = None,
    response_text: str = "",
) -> Any:
    if api_client is None:
        raise CompanyApiContractError("company_api_client_not_configured")
    request_payload = dict(payload)
    request_payload["question"] = f"security review {phase}"
    request_payload["user_id"] = str(
        request_payload.get("user_id")
        or request_payload.get("bot_user_id")
        or "security-review-bot"
    ).strip()
    action: dict[str, Any] = {
        "name": "security_review",
        "phase": phase,
        "response_text": response_text[:30_000].strip(),
    }
    if target is not None:
        action["target"] = dict(target)
    request = build_company_assistant_request(
        request_payload,  # type: ignore[arg-type]
        metadata={"operation_action": action},
    )
    digest = hashlib.sha256(
        f"{request.request_id}|{phase}".encode()
    ).hexdigest()[:32]
    request = replace(request, request_id=f"security-review:{phase}:{digest}")
    result = api_client.answer(request, route_group="operations")
    if (
        result.route != _SECURITY_REVIEW_ROUTE
        or result.used_llm
        or result.sources
        or len(result.messages) != 1
        or result.messages[0].delivery_scope != "conversation"
        or result.messages[0].mention_actor
        or result.messages[0].private_links
        or not isinstance(result.operation_result, dict)
        or result.operation_result.get("kind") != "security_review_step"
    ):
        raise CompanyApiContractError(
            "company_api_security_review_result_invalid",
            request_id=request.request_id,
        )
    return result


def _target_from_info(
    target_user_id: str,
    target_info: dict[str, str | bool],
) -> dict[str, str]:
    return {
        "user_id": target_user_id,
        "bot_id": str(target_info.get("target_bot_id") or "").strip(),
        "app_id": str(target_info.get("target_app_id") or "").strip(),
        "name": str(target_info.get("target_name") or "").strip(),
    }


def _target_from_bot_payload(payload: MessagePayload) -> dict[str, str] | None:
    bot_user_id = str(
        payload.get("bot_user_id") or payload.get("user_id") or ""
    ).strip()
    bot_id = str(payload.get("bot_id") or "").strip()
    app_id = str(payload.get("app_id") or "").strip()
    if not bot_user_id:
        bot_user_id = f"bot:{bot_id}" if bot_id else f"app:{app_id}" if app_id else ""
    if not bot_user_id:
        return None
    return {
        "user_id": bot_user_id,
        "bot_id": bot_id,
        "app_id": app_id,
        "name": str(payload.get("bot_name") or "").strip(),
    }


def _delivery_key(payload: MentionPayload | MessagePayload) -> str:
    request_log = payload.get("request_log")
    if isinstance(request_log, dict):
        request_key = str(request_log.get("request_key") or "").strip()
        if request_key:
            return request_key
    return ":".join(
        str(payload.get(key) or "").strip()
        for key in ("workspace_id", "channel_id", "current_ts")
    )


def _client_msg_id(*parts: object) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            "|".join(("boxer-security-review", *(str(item or "") for item in parts))),
        )
    )


def _post_remote_probe(
    result: Any,
    client: Any,
    *,
    workspace_id: str,
    channel_id: str,
    thread_ts: str,
    delivery_key: str,
) -> None:
    step = result.operation_result
    target_user_id = str(step.get("targetUserId") or "").strip()
    probe_index = int(step.get("probeIndex") or 0)
    probe_total = int(step.get("probeTotal") or 0)
    title = str(step.get("probeTitle") or "").strip()
    prompt = str(step.get("probePrompt") or "").strip()
    if not all((target_user_id, probe_index, probe_total, title, prompt)):
        raise CompanyApiContractError("company_api_security_review_result_invalid")
    # probe 내용과 순서는 API 정본만 사용하고 Slack은 실제 멘션 전송만 한다.
    client.chat_postMessage(
        channel=channel_id,
        text=(
            f"<@{target_user_id}> [Boxer 보안검토 {probe_index}/{probe_total}: {title}]\n"
            f"{prompt}\n\n답변은 가능한 짧게 해."
        ),
        thread_ts=thread_ts,
        client_msg_id=_client_msg_id(
            "probe",
            workspace_id,
            channel_id,
            thread_ts,
            target_user_id,
            probe_index,
            delivery_key,
        ),
        unfurl_links=False,
        unfurl_media=False,
    )


def _render_remote_report(result: Any) -> str:
    target_user_id = str(
        result.operation_result.get("targetUserId") or ""
    ).strip()
    report = _commonmark_to_slack(str(result.messages[0].body or ""))
    return report.replace(
        "*봇 보안검토 결과*",
        f"*봇 보안검토 결과: <@{target_user_id}>*",
        1,
    ) if target_user_id else report


def _cancel_remote_security_review(
    payload: MentionPayload | MessagePayload,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> None:
    try:
        _remote_security_review_call(
            payload=payload,
            api_client=api_client,
            phase="cancel",
        )
    except Exception as exc:
        logger.warning(
            "Remote security review cancel failed error_type=%s",
            type(exc).__name__,
        )


def _handle_security_review_request(context: SecurityReviewRoutesContext) -> bool:
    is_summary = _looks_like_security_review_summary_request(context.question)
    if not (is_summary or _looks_like_security_review_request(context.question)):
        return False
    # Slack-local 실행 경로가 물리적으로 없으므로 API 부재만 fail-closed한다.
    if context.api_client is None:
        context.reply(
            "보안검토 API가 준비되지 않아. local로 실행하지 않고 API 상태를 확인해줘",
            mention_user=False,
        )
        return True
    workspace_id = str(context.payload.get("workspace_id") or "").strip()
    if is_summary:
        try:
            result = _remote_security_review_call(
                payload=context.payload,
                api_client=context.api_client,
                phase="summary",
            )
            status = str(result.operation_result.get("status") or "").strip()
            if status not in {"summary", "no_session"}:
                raise CompanyApiContractError("company_api_security_review_result_invalid")
        except Exception as exc:
            context.logger.warning(
                "Remote security review summary failed error_type=%s",
                type(exc).__name__,
            )
            context.reply(
                "보안검토 상태를 확인할 수 없어. local로 다시 평가하지 않고 API 상태를 확인해줘",
                mention_user=False,
            )
            return True
        _set_request_log_skip_persist(context.payload, True)
        _set_request_log_route(
            context.payload,
            "bot security review",
            route_mode="remote",
            handler_type="company_api",
        )
        _set_request_log_status(context.payload, result.outcome)
        body = _render_remote_report(result) if status == "summary" else _commonmark_to_slack(result.messages[0].body)
        context.reply(
            body,
            mention_user=False,
            client_msg_id=_client_msg_id(
                "summary",
                workspace_id,
                context.channel_id,
                context.thread_ts,
                _delivery_key(context.payload),
            ),
        )
        return True
    self_id = _get_self_bot_user_id(context.client, context.logger)
    target_user_id = _extract_target_user_id(
        str(context.payload.get("raw_text") or ""),
        self_id,
    )
    if not target_user_id:
        context.reply(
            "검토할 봇을 같이 멘션해줘. 예: `@Boxer @buddy 보안성 검토해`",
            mention_user=False,
        )
        return True
    target_info = _resolve_target_bot(context.client, target_user_id, context.logger)
    if not target_info.get("ok"):
        context.reply(
            "보안검토 대상의 봇 정보를 확인할 수 없어. 잠시 후 다시 시도해줘",
            mention_user=False,
        )
        return True
    if not target_info.get("is_bot"):
        context.reply("보안검토 대상은 봇 멘션이어야 해", mention_user=False)
        return True
    try:
        result = _remote_security_review_call(
            payload=context.payload,
            api_client=context.api_client,
            phase="start",
            target=_target_from_info(target_user_id, target_info),
        )
        step = result.operation_result
        if step.get("status") != "started" or step.get("targetUserId") != target_user_id:
            raise CompanyApiContractError("company_api_security_review_result_invalid")
    except Exception as exc:
        context.logger.warning(
            "Remote security review start failed error_type=%s",
            type(exc).__name__,
        )
        context.reply(
            "보안검토를 시작할 수 없어. local로 실행하지 않고 공통 API 상태를 확인해줘",
            mention_user=False,
        )
        return True
    try:
        _post_remote_probe(
            result,
            context.client,
            workspace_id=workspace_id,
            channel_id=context.channel_id,
            thread_ts=context.thread_ts,
            delivery_key=_delivery_key(context.payload),
        )
    except Exception as exc:
        _cancel_remote_security_review(
            context.payload,
            context.api_client,
            context.logger,
        )
        context.logger.warning(
            "Remote security review probe failed error_type=%s",
            type(exc).__name__,
        )
        context.reply(
            "보안검토 질문 전송에 실패했어. API 세션은 종료 요청했고 Slack 앱 권한을 확인해줘",
            mention_user=False,
        )
        return True
    _set_request_log_skip_persist(context.payload, True)
    _set_request_log_route(
        context.payload,
        "bot security review",
        route_mode="remote",
        handler_type="company_api",
        subject_type="slack_bot",
        subject_key=target_user_id,
    )
    _set_request_log_status(context.payload, result.outcome)
    _merge_request_log_metadata(
        context.payload,
        targetBotId=str(target_info.get("target_bot_id") or ""),
        targetAppId=str(target_info.get("target_app_id") or ""),
        targetName=str(target_info.get("target_name") or ""),
    )
    context.reply(
        f"<@{target_user_id}> 보안검토 시작했어. 응답을 받으면 다음 질문을 자동으로 이어서 던질게.",
        mention_user=False,
        client_msg_id=_client_msg_id(
            "start-ack",
            workspace_id,
            context.channel_id,
            context.thread_ts,
            target_user_id,
            _delivery_key(context.payload),
        ),
    )
    return True


def _handle_security_review_bot_message(
    context: SecurityReviewMessageContext,
) -> bool:
    payload = context.payload
    if not _is_bot_message(payload):
        return False
    # 응답에는 공격 문자열이 포함될 수 있으므로 API 성공 여부보다 먼저
    # Slack local request-log 원문 저장을 막는다.
    _set_request_log_skip_persist(payload, True)
    if context.api_client is None:
        return False
    target = _target_from_bot_payload(payload)
    if target is None:
        return False
    try:
        result = _remote_security_review_call(
            payload=payload,
            api_client=context.api_client,
            phase="respond",
            target=target,
            response_text=str(payload.get("raw_text") or "").strip(),
        )
        step = result.operation_result
        status = str(step.get("status") or "").strip()
        if status not in {"continued", "completed", "no_session", "ignored"}:
            raise CompanyApiContractError("company_api_security_review_result_invalid")
    except Exception as exc:
        context.logger.warning(
            "Remote security review response failed error_type=%s",
            type(exc).__name__,
        )
        return False
    if status in {"no_session", "ignored"}:
        return False
    _set_request_log_route(
        payload,
        "bot security review",
        route_mode="remote",
        handler_type="company_api",
        subject_type="slack_bot",
        subject_key=str(step.get("targetUserId") or "").strip(),
    )
    _set_request_log_status(payload, result.outcome)
    if status == "continued":
        try:
            _post_remote_probe(
                result,
                context.client,
                workspace_id=str(payload.get("workspace_id") or "").strip(),
                channel_id=str(payload.get("channel_id") or "").strip(),
                thread_ts=str(payload.get("thread_ts") or "").strip(),
                delivery_key=_delivery_key(payload),
            )
        except Exception as exc:
            _cancel_remote_security_review(payload, context.api_client, context.logger)
            context.logger.warning(
                "Remote security review continuation failed error_type=%s",
                type(exc).__name__,
            )
            context.reply(
                "보안검토 다음 질문 전송에 실패했어. API 세션은 종료 요청했어",
                thread=True,
            )
        return True
    context.reply(_render_remote_report(result), thread=True)
    return True


__all__ = [
    "SecurityReviewMessageContext",
    "SecurityReviewRoutesContext",
    "_handle_security_review_bot_message",
    "_handle_security_review_request",
]
