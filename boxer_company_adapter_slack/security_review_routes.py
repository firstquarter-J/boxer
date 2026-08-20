import hashlib
import logging
import re
import time
from dataclasses import dataclass, field, replace
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
from boxer_company.assistant.security_review_route import (
    SECURITY_REVIEW_PROBES,
    SECURITY_REVIEW_ROUTE,
    SecurityReviewProbe,
    SecurityReviewResponse,
    assess_security_review_response,
    build_security_review_report,
)
from boxer_company_adapter_slack.assistant_bridge import (
    _commonmark_to_slack,
    build_company_assistant_request,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiContractError,
    CompanyAssistantApiClient,
)


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
    operations_remote: bool = False


@dataclass(frozen=True)
class SecurityReviewMessageContext:
    payload: MessagePayload
    reply: SlackMessageReplyFn
    client: Any
    logger: logging.Logger
    api_client: CompanyAssistantApiClient | None = None
    operations_remote: bool = False


@dataclass
class SecurityReviewSession:
    workspace_id: str
    channel_id: str
    thread_ts: str
    requested_by: str | None
    target_user_id: str
    target_bot_id: str = ""
    target_app_id: str = ""
    target_name: str = ""
    current_probe_index: int = 0
    responses: list[SecurityReviewResponse] = field(default_factory=list)
    started_at_epoch: float = field(default_factory=time.time)


_MENTION_RE = re.compile(r"<@([A-Z0-9]+)(?:\|[^>]+)?>")
_SECURITY_REVIEW_SESSION_TTL_SEC = 60 * 30
_SECURITY_REVIEW_SESSIONS: dict[tuple[str, str, str], SecurityReviewSession] = {}
# local rollback도 공통 API와 같은 probe 정본을 import해 drift를 막는다.
_SECURITY_REVIEW_PROBES = SECURITY_REVIEW_PROBES


def _session_key(workspace_id: str, channel_id: str, thread_ts: str) -> tuple[str, str, str]:
    return (workspace_id or "", channel_id or "", thread_ts or "")


def _cleanup_stale_security_review_sessions(now_epoch: float | None = None) -> None:
    now = time.time() if now_epoch is None else now_epoch
    stale_keys = [
        key
        for key, session in _SECURITY_REVIEW_SESSIONS.items()
        if now - session.started_at_epoch > _SECURITY_REVIEW_SESSION_TTL_SEC
    ]
    for key in stale_keys:
        _SECURITY_REVIEW_SESSIONS.pop(key, None)


def _looks_like_security_review_request(question: str) -> bool:
    text = (question or "").strip()
    if not text:
        return False
    lowered = text.lower()
    if "보안" in text and any(token in text for token in ("검토", "검증", "테스트", "취약")):
        return True
    if "취약점" in text or "인젝션" in text:
        return True
    if "injection" in lowered or "security review" in lowered or "security test" in lowered:
        return True
    return (
        any(token in text for token in ("검증", "테스트"))
        and any(token in text for token in ("봇", "멘션", "호출"))
    )


def _looks_like_security_review_summary_request(question: str) -> bool:
    text = (question or "").strip()
    if not text:
        return False
    return "보안" in text and any(token in text for token in ("결과", "요약", "상태", "마무리"))


def _get_self_bot_user_id(client: Any, logger: logging.Logger) -> str:
    try:
        response = client.auth_test()
    except Exception:
        logger.warning("Failed to resolve Boxer bot user id", exc_info=True)
        return ""
    return str((response or {}).get("user_id") or "").strip()


def _extract_target_user_id(raw_text: str, self_user_id: str) -> str:
    mentions = _MENTION_RE.findall(raw_text or "")
    if not mentions:
        return ""
    if self_user_id:
        for mention in mentions:
            if mention != self_user_id:
                return mention
        return ""
    # app_mention 이벤트의 첫 멘션은 보통 Boxer 자신이라, self 조회 실패 시 두 번째 멘션을 대상으로 본다.
    if len(mentions) >= 2:
        return mentions[1]
    return ""


def _resolve_target_bot(client: Any, target_user_id: str, logger: logging.Logger) -> dict[str, str | bool]:
    try:
        response = client.users_info(user=target_user_id)
    except Exception:
        logger.warning("Failed to resolve target bot info user=%s", target_user_id, exc_info=True)
        return {
            "ok": False,
            "is_bot": True,
            "target_bot_id": "",
            "target_app_id": "",
            "target_name": "",
        }

    user = (response or {}).get("user")
    user_dict = user if isinstance(user, dict) else {}
    profile = user_dict.get("profile")
    profile_dict = profile if isinstance(profile, dict) else {}
    target_bot_id = str(user_dict.get("bot_id") or profile_dict.get("bot_id") or "").strip()
    target_app_id = str(
        user_dict.get("app_id")
        or profile_dict.get("api_app_id")
        or profile_dict.get("app_id")
        or ""
    ).strip()
    target_name = str(
        profile_dict.get("display_name")
        or profile_dict.get("real_name")
        or user_dict.get("real_name")
        or user_dict.get("name")
        or ""
    ).strip()
    is_bot = bool(user_dict.get("is_bot") or target_bot_id or target_app_id)
    return {
        "ok": True,
        "is_bot": is_bot,
        "target_bot_id": target_bot_id,
        "target_app_id": target_app_id,
        "target_name": target_name,
    }


def _format_probe_message(session: SecurityReviewSession, probe: SecurityReviewProbe) -> str:
    total = len(_SECURITY_REVIEW_PROBES)
    index = session.current_probe_index + 1
    return (
        f"<@{session.target_user_id}> [Boxer 보안검토 {index}/{total}: {probe.title}]\n"
        f"{probe.prompt}\n\n"
        "답변은 가능한 짧게 해."
    )


def _post_current_probe(
    session: SecurityReviewSession,
    client: Any,
    logger: logging.Logger,
) -> None:
    probe = _SECURITY_REVIEW_PROBES[session.current_probe_index]
    # 대상 봇을 깨우는 실제 Slack 액션은 여기 한 곳으로 모아 루프 범위를 통제한다.
    client.chat_postMessage(
        channel=session.channel_id,
        text=_format_probe_message(session, probe),
        thread_ts=session.thread_ts,
        unfurl_links=False,
        unfurl_media=False,
    )
    logger.info(
        "Posted security review probe index=%s target=%s thread_ts=%s",
        session.current_probe_index,
        session.target_user_id,
        session.thread_ts,
    )


def _is_bot_message(payload: MessagePayload) -> bool:
    subtype = str(payload.get("subtype") or "").strip()
    if subtype:
        return subtype == "bot_message"
    # 최신 Slack 앱 메시지는 subtype 없이 bot_id/app_id만 전달될 수 있다.
    return bool(
        str(payload.get("bot_id") or "").strip()
        or str(payload.get("app_id") or "").strip()
    )


def _message_matches_target(payload: MessagePayload, session: SecurityReviewSession) -> bool:
    if not _is_bot_message(payload):
        return False
    bot_user_id = str(payload.get("bot_user_id") or payload.get("user_id") or "").strip()
    bot_id = str(payload.get("bot_id") or "").strip()
    app_id = str(payload.get("app_id") or "").strip()
    bot_name = str(payload.get("bot_name") or "").strip().lower()
    target_name = (session.target_name or "").strip().lower()

    if bot_user_id and bot_user_id == session.target_user_id:
        return True
    if session.target_bot_id and bot_id == session.target_bot_id:
        return True
    if session.target_app_id and app_id == session.target_app_id:
        return True
    return bool(target_name and bot_name and bot_name == target_name)


def _assess_response(response: SecurityReviewResponse) -> tuple[str, str]:
    # local rollback도 API와 같은 channel-neutral 판정 함수를 사용한다.
    return assess_security_review_response(response)


def _build_security_review_report(session: SecurityReviewSession) -> str:
    report = _commonmark_to_slack(build_security_review_report(session))
    return report.replace(
        "*봇 보안검토 결과*",
        f"*봇 보안검토 결과: <@{session.target_user_id}>*",
        1,
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
    actor_id = str(
        request_payload.get("user_id")
        or request_payload.get("bot_user_id")
        or "security-review-bot"
    ).strip()
    request_payload["user_id"] = actor_id
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
    # 같은 Slack event에서 probe 전송 실패 후 cancel을 보내도 request guard가
    # 충돌하지 않도록 event+phase를 고정 길이 correlation ID로 만든다.
    digest = hashlib.sha256(
        f"{request.request_id}|{phase}".encode("utf-8")
    ).hexdigest()[:32]
    request = replace(
        request,
        request_id=f"security-review:{phase}:{digest}",
    )
    result = api_client.answer(request, route_group="operations")
    if (
        result.route != SECURITY_REVIEW_ROUTE
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
        bot_user_id = (
            f"bot:{bot_id}"
            if bot_id
            else f"app:{app_id}"
            if app_id
            else ""
        )
    if not bot_user_id:
        return None
    return {
        "user_id": bot_user_id,
        "bot_id": bot_id,
        "app_id": app_id,
        "name": str(payload.get("bot_name") or "").strip(),
    }


def _security_review_delivery_key(
    payload: MentionPayload | MessagePayload,
) -> str:
    request_log = payload.get("request_log")
    request_key = (
        str(request_log.get("request_key") or "").strip()
        if isinstance(request_log, dict)
        else ""
    )
    if request_key:
        return request_key
    return ":".join(
        str(payload.get(key) or "").strip()
        for key in (
            "workspace_id",
            "channel_id",
            "current_ts",
        )
    )


def _security_review_client_msg_id(*parts: object) -> str:
    return str(
        uuid5(
            NAMESPACE_URL,
            "|".join(
                ("boxer-security-review",)
                + tuple(str(part or "") for part in parts)
            ),
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
    probe_title = str(step.get("probeTitle") or "").strip()
    probe_prompt = str(step.get("probePrompt") or "").strip()
    if not all(
        (
            target_user_id,
            probe_index,
            probe_total,
            probe_title,
            probe_prompt,
        )
    ):
        raise CompanyApiContractError(
            "company_api_security_review_result_invalid"
        )
    # Slack mention/header는 transport가 만들고 probe 정본은 API 결과만 사용한다.
    client.chat_postMessage(
        channel=channel_id,
        text=(
            f"<@{target_user_id}> [Boxer 보안검토 "
            f"{probe_index}/{probe_total}: {probe_title}]\n"
            f"{probe_prompt}\n\n답변은 가능한 짧게 해."
        ),
        thread_ts=thread_ts,
        # API replay 뒤 adapter가 다시 전송해도 Slack이 같은 probe를
        # 중복 생성하지 않도록 transport identity를 결정적으로 고정한다.
        client_msg_id=_security_review_client_msg_id(
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
    step = result.operation_result
    target_user_id = str(step.get("targetUserId") or "").strip()
    report = _commonmark_to_slack(str(result.messages[0].body or ""))
    if target_user_id:
        report = report.replace(
            "*봇 보안검토 결과*",
            f"*봇 보안검토 결과: <@{target_user_id}>*",
            1,
        )
    return report


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
        # Slack 전송 실패 뒤 domain session 정리 실패는 raw 응답 없이 타입만 남긴다.
        logger.warning(
            "Failed to cancel remote security review error_type=%s",
            type(exc).__name__,
        )


def _handle_security_review_request(context: SecurityReviewRoutesContext) -> bool:
    question = context.question
    if not (_looks_like_security_review_request(question) or _looks_like_security_review_summary_request(question)):
        return False

    key = _session_key(
        str(context.payload.get("workspace_id") or "").strip(),
        context.channel_id,
        context.thread_ts,
    )

    if _looks_like_security_review_summary_request(question):
        if context.operations_remote:
            try:
                result = _remote_security_review_call(
                    payload=context.payload,
                    api_client=context.api_client,
                    phase="summary",
                )
                status = str(
                    result.operation_result.get("status") or ""
                ).strip()
                if status not in {"summary", "no_session"}:
                    raise CompanyApiContractError(
                        "company_api_security_review_result_invalid"
                    )
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
            body = (
                _render_remote_report(result)
                if status == "summary"
                else _commonmark_to_slack(result.messages[0].body)
            )
            context.reply(
                body,
                mention_user=False,
                client_msg_id=_security_review_client_msg_id(
                    "summary",
                    key[0],
                    key[1],
                    key[2],
                    _security_review_delivery_key(context.payload),
                ),
            )
            return True

        _cleanup_stale_security_review_sessions()
        session = _SECURITY_REVIEW_SESSIONS.pop(key, None)
        if session is None:
            context.reply("진행 중인 봇 보안검토 세션이 없어", mention_user=False)
            return True
        context.reply(_build_security_review_report(session), mention_user=False)
        return True

    self_user_id = _get_self_bot_user_id(context.client, context.logger)
    target_user_id = _extract_target_user_id(str(context.payload.get("raw_text") or ""), self_user_id)
    if not target_user_id:
        context.reply(
            "검토할 봇을 같이 멘션해줘. 예: `@Boxer @buddy 보안성 검토해`",
            mention_user=False,
        )
        return True

    target_info = _resolve_target_bot(context.client, target_user_id, context.logger)
    if not target_info.get("ok"):
        # bot identity를 Slack에서 확정하지 못하면 API 세션이나 probe
        # 발송을 시작하지 않고 local/remote 모두 fail-closed한다.
        context.reply(
            "보안검토 대상의 봇 정보를 확인할 수 없어. 잠시 후 다시 시도해줘",
            mention_user=False,
        )
        return True
    if not target_info.get("is_bot"):
        context.reply("보안검토 대상은 봇 멘션이어야 해", mention_user=False)
        return True

    if context.operations_remote:
        try:
            result = _remote_security_review_call(
                payload=context.payload,
                api_client=context.api_client,
                phase="start",
                target=_target_from_info(target_user_id, target_info),
            )
            step = result.operation_result
            if (
                step.get("status") != "started"
                or step.get("targetUserId") != target_user_id
            ):
                raise CompanyApiContractError(
                    "company_api_security_review_result_invalid"
                )
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
                workspace_id=str(
                    context.payload.get("workspace_id") or ""
                ).strip(),
                channel_id=context.channel_id,
                thread_ts=context.thread_ts,
                delivery_key=_security_review_delivery_key(
                    context.payload
                ),
            )
        except Exception:
            _cancel_remote_security_review(
                context.payload,
                context.api_client,
                context.logger,
            )
            context.logger.warning(
                "Failed to post remote security review probe target=%s",
                target_user_id,
                exc_info=True,
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
            client_msg_id=_security_review_client_msg_id(
                "start-ack",
                key[0],
                key[1],
                key[2],
                target_user_id,
                _security_review_delivery_key(context.payload),
            ),
        )
        return True

    _cleanup_stale_security_review_sessions()
    session = SecurityReviewSession(
        workspace_id=key[0],
        channel_id=context.channel_id,
        thread_ts=context.thread_ts,
        requested_by=context.user_id,
        target_user_id=target_user_id,
        target_bot_id=str(target_info.get("target_bot_id") or ""),
        target_app_id=str(target_info.get("target_app_id") or ""),
        target_name=str(target_info.get("target_name") or ""),
    )
    _SECURITY_REVIEW_SESSIONS[key] = session
    _set_request_log_route(
        context.payload,
        "bot security review",
        route_mode="start",
        handler_type="router",
        subject_type="slack_bot",
        subject_key=target_user_id,
    )
    _merge_request_log_metadata(
        context.payload,
        targetBotId=session.target_bot_id,
        targetAppId=session.target_app_id,
        targetName=session.target_name,
    )

    try:
        _post_current_probe(session, context.client, context.logger)
    except Exception:
        _SECURITY_REVIEW_SESSIONS.pop(key, None)
        context.logger.exception("Failed to start bot security review target=%s", target_user_id)
        context.reply("보안검토 질문 전송에 실패했어. Slack 앱 권한과 대상 봇 멘션 가능 여부를 확인해줘")
        return True

    context.reply(
        f"<@{target_user_id}> 보안검토 시작했어. 응답을 받으면 다음 질문을 자동으로 이어서 던질게.",
        mention_user=False,
    )
    return True


def _handle_security_review_bot_message(context: SecurityReviewMessageContext) -> bool:
    payload = context.payload
    if not _is_bot_message(payload):
        return False

    if context.operations_remote:
        observed_target = _target_from_bot_payload(payload)
        if observed_target is None:
            return False
        # 보안 probe 응답에는 실제 secret처럼 보이는 문자열이 포함될 수 있다.
        # API 장애로 세션 여부를 못 알아도 Slack 로컬 원문 저장은 먼저 막는다.
        _set_request_log_skip_persist(payload, True)
        try:
            result = _remote_security_review_call(
                payload=payload,
                api_client=context.api_client,
                phase="respond",
                target=observed_target,
                response_text=str(payload.get("raw_text") or "").strip(),
            )
            step = result.operation_result
            status = str(step.get("status") or "").strip()
            if status not in {
                "continued",
                "completed",
                "no_session",
                "ignored",
            }:
                raise CompanyApiContractError(
                    "company_api_security_review_result_invalid"
                )
        except Exception as exc:
            # 모든 bot event가 이 gateway를 지나므로 세션 여부를 모르는
            # API 장애 때 채널에 오류를 뿌리지 않고 local 평가도 하지 않는다.
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
                    workspace_id=str(
                        payload.get("workspace_id") or ""
                    ).strip(),
                    channel_id=str(payload.get("channel_id") or "").strip(),
                    thread_ts=str(payload.get("thread_ts") or "").strip(),
                    delivery_key=_security_review_delivery_key(payload),
                )
            except Exception:
                _cancel_remote_security_review(
                    payload,
                    context.api_client,
                    context.logger,
                )
                context.logger.warning(
                    "Failed to continue remote bot security review target=%s",
                    str(step.get("targetUserId") or "").strip(),
                    exc_info=True,
                )
                context.reply(
                    "보안검토 다음 질문 전송에 실패했어. API 세션은 종료 요청했어",
                    thread=True,
                )
            return True

        context.reply(_render_remote_report(result), thread=True)
        context.logger.info(
            "Completed remote bot security review target=%s thread_ts=%s responses=%s",
            str(step.get("targetUserId") or "").strip(),
            str(payload.get("thread_ts") or "").strip(),
            int(step.get("probeIndex") or 0),
        )
        return True

    _cleanup_stale_security_review_sessions()
    key = _session_key(
        str(payload.get("workspace_id") or "").strip(),
        str(payload.get("channel_id") or "").strip(),
        str(payload.get("thread_ts") or "").strip(),
    )
    session = _SECURITY_REVIEW_SESSIONS.get(key)
    if session is None:
        return False
    if not _message_matches_target(payload, session):
        return False

    if session.current_probe_index >= len(_SECURITY_REVIEW_PROBES):
        return True

    probe = _SECURITY_REVIEW_PROBES[session.current_probe_index]
    session.responses.append(
        SecurityReviewResponse(
            probe=probe,
            text=str(payload.get("raw_text") or "").strip(),
        )
    )
    session.current_probe_index += 1

    if session.current_probe_index < len(_SECURITY_REVIEW_PROBES):
        try:
            _post_current_probe(session, context.client, context.logger)
        except Exception:
            _SECURITY_REVIEW_SESSIONS.pop(key, None)
            context.logger.exception(
                "Failed to continue bot security review target=%s",
                session.target_user_id,
            )
            context.reply("보안검토 다음 질문 전송에 실패했어. 여기까지 수집한 응답만 확인해줘", thread=True)
        return True

    _SECURITY_REVIEW_SESSIONS.pop(key, None)
    context.reply(_build_security_review_report(session), thread=True)
    context.logger.info(
        "Completed bot security review target=%s thread_ts=%s responses=%s",
        session.target_user_id,
        session.thread_ts,
        len(session.responses),
    )
    return True
