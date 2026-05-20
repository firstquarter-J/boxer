import logging
from dataclasses import dataclass
from typing import Any, Callable

import anthropic

from boxer_adapter_slack.common import MentionPayload, SlackReplyFn, _set_request_log_route
from boxer_adapter_slack.context import _load_slack_thread_context
from boxer.context.builder import _build_model_input
from boxer.core import settings as s
from boxer.core.llm import _ask_claude, _ask_ollama_chat, _check_ollama_health
from boxer.retrieval.connectors.notion import _is_notion_configured
from boxer.retrieval.synthesis import _synthesize_retrieval_answer
from boxer_company.prompt_security import (
    build_prompt_security_refusal,
    is_prompt_exfiltration_attempt,
)
from boxer_company.notion_playbooks import _select_notion_references
from boxer_company.retrieval_rules import (
    _build_company_retrieval_rules,
    _transform_company_retrieval_payload,
)
from boxer_company_adapter_slack.notion_freeform import (
    _build_freeform_chat_system_prompt,
    _build_notion_doc_fallback,
    _build_notion_doc_query_text,
    _build_notion_doc_security_refusal,
    _get_freeform_system_prompt,
    _is_notion_doc_exfiltration_attempt,
    _looks_like_notion_doc_followup,
    _looks_like_notion_doc_question,
    _sanitize_freeform_reply,
    _sanitize_notion_references_for_llm,
)


@dataclass(frozen=True)
class KnowledgeRoutesContext:
    question: str
    barcode: str | None
    user_id: str | None
    payload: MentionPayload
    thread_ts: str
    channel_id: str
    current_ts: str
    reply: SlackReplyFn
    logger: logging.Logger
    client: Any
    claude_client: Any


@dataclass(frozen=True)
class KnowledgeRoutesDeps:
    reply_with_retrieval_synthesis: Callable[..., None]
    timeout_reply_text: Callable[[], str]
    llm_unavailable_reply_text: Callable[[str | None], str]
    is_timeout_error: Callable[[Exception], bool]
    is_claude_allowed_user: Callable[[str | None], bool]
    build_barcode_fallback_evidence: Callable[[], dict[str, Any] | None]


def _build_claude_api_key_missing_reply() -> str:
    # 키 설정 문제는 일반 장애 문구로 숨기지 않고 운영자가 바로 조치할 수 있게 노출한다.
    return "API 키가 설정되지 않아 지금은 AI 답변을 생성할 수 없어. 서버의 `ANTHROPIC_API_KEY`를 확인해줘"


def _build_claude_api_key_invalid_reply() -> str:
    return "API 키가 유효하지 않아 지금은 AI 답변을 생성할 수 없어. 서버의 `ANTHROPIC_API_KEY`를 확인해줘"


def _build_claude_permission_denied_reply() -> str:
    return (
        "API 키 권한이 없어 지금은 AI 답변을 생성할 수 없어. "
        "서버의 `ANTHROPIC_API_KEY`를 확인해줘"
    )


def _build_claude_credit_unavailable_reply() -> str:
    return "토큰이 충전되지 않아 답변할 수 없어. 추가 결제가 필요해."


def _flatten_error_text(value: Any) -> str:
    if isinstance(value, dict):
        parts: list[str] = []
        for key, item in value.items():
            parts.append(str(key))
            parts.append(_flatten_error_text(item))
        return " ".join(part for part in parts if part)
    if isinstance(value, (list, tuple, set)):
        return " ".join(_flatten_error_text(item) for item in value)
    return str(value or "")


def _is_claude_credit_unavailable_error(exc: Exception) -> bool:
    # Anthropic의 잔액/결제 오류는 SDK 버전에 따라 RateLimitError 본문 문구로만 구분될 수 있다.
    body_text = _flatten_error_text(getattr(exc, "body", None))
    combined = f"{body_text} {getattr(exc, 'message', '')} {str(exc)}".lower()
    return any(
        token in combined
        for token in (
            "credit balance",
            "credits",
            "insufficient_quota",
            "insufficient quota",
            "quota_exceeded",
            "billing",
            "payment",
            "prepaid",
        )
    )


def _handle_knowledge_routes(
    context: KnowledgeRoutesContext,
    deps: KnowledgeRoutesDeps,
) -> bool:
    question = context.question
    provider = (s.LLM_PROVIDER or "").lower().strip()

    notion_thread_context = ""
    is_notion_doc_question = _looks_like_notion_doc_question(question)
    if not is_notion_doc_question and context.thread_ts:
        notion_thread_context = _load_slack_thread_context(
            context.client,
            context.logger,
            context.channel_id,
            context.thread_ts,
            context.current_ts,
        )
        is_notion_doc_question = _looks_like_notion_doc_followup(question, notion_thread_context)

    if is_notion_doc_question:
        _set_request_log_route(context.payload, "notion playbook qa", handler_type="router")
        try:
            if _is_notion_doc_exfiltration_attempt(question, notion_thread_context):
                context.logger.warning(
                    "Blocked notion doc exfiltration attempt in thread_ts=%s question=%s",
                    context.thread_ts,
                    question,
                )
                context.reply(_build_notion_doc_security_refusal())
                return True
            evidence_payload = {
                "route": "notion_playbook_qa",
                "source": "notion",
                "request": {
                    "question": question,
                },
            }
            if not notion_thread_context and context.thread_ts:
                notion_thread_context = _load_slack_thread_context(
                    context.client,
                    context.logger,
                    context.channel_id,
                    context.thread_ts,
                    context.current_ts,
                )
            notion_query_text = _build_notion_doc_query_text(question, notion_thread_context)
            if notion_query_text and notion_query_text != question:
                evidence_payload["request"]["contextualQuestion"] = notion_query_text
            notion_references = _select_notion_references(
                notion_query_text or question,
                evidence_payload=evidence_payload,
                max_results=3,
            )
            if notion_references:
                sanitized_references = _sanitize_notion_references_for_llm(notion_references)
                evidence_payload["notionPlaybooks"] = sanitized_references
                evidence_payload["notionReferences"] = sanitized_references
                fallback_text = _build_notion_doc_fallback(question, sanitized_references)
                deps.reply_with_retrieval_synthesis(
                    fallback_text,
                    evidence_payload,
                    route_name="notion playbook qa",
                )
                context.logger.info(
                    "Responded with notion doc answer in thread_ts=%s refs=%s",
                    context.thread_ts,
                    len(notion_references),
                )
                return True
            if not _is_notion_configured():
                context.logger.warning("Notion doc query had no local match and notion is not configured in runtime")
            context.reply("관련 운영 문서를 찾지 못했어. 증상이나 키워드를 조금 더 구체적으로 말해줘")
            context.logger.info("No notion references matched in thread_ts=%s question=%s", context.thread_ts, question)
            return True
        except TimeoutError:
            context.logger.warning("Notion doc answer timeout")
            context.reply(deps.timeout_reply_text())
            return True
        except Exception:
            context.logger.exception("Notion doc answer failed")
            context.reply("문서 기반 답변 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return True

    if provider == "claude" and not context.claude_client:
        _set_request_log_route(
            context.payload,
            "llm_freeform",
            route_mode="claude",
            handler_type="llm_freeform",
        )
        context.logger.warning("Claude client unavailable before answer generation")
        context.reply(_build_claude_api_key_missing_reply())
        return True

    if provider == "claude" and context.claude_client:
        _set_request_log_route(
            context.payload,
            "llm_freeform",
            route_mode="claude",
            handler_type="llm_freeform",
        )
        if not question:
            context.reply("질문 내용을 같이 보내줘. 지원 기능이 궁금하면 `사용법`이라고 보내줘")
            return True
        if not deps.is_claude_allowed_user(context.user_id):
            context.reply("AI 질문은 현재 지정된 사용자만 사용할 수 있어")
            context.logger.info("Rejected claude call for user=%s", context.user_id)
            return True
        try:
            thread_context = _load_slack_thread_context(
                context.client,
                context.logger,
                context.channel_id,
                context.thread_ts,
                context.current_ts,
            )
            if is_prompt_exfiltration_attempt(question, thread_context):
                context.logger.warning(
                    "Blocked freeform prompt exfiltration attempt in thread_ts=%s question=%s",
                    context.thread_ts,
                    question,
                )
                context.reply(build_prompt_security_refusal())
                return True
            fallback_evidence = deps.build_barcode_fallback_evidence()
            if fallback_evidence is not None:
                synthesis_thread_context = ""
                if s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT:
                    synthesis_thread_context = _load_slack_thread_context(
                        context.client,
                        context.logger,
                        context.channel_id,
                        context.thread_ts,
                        context.current_ts,
                    )
                answer = _synthesize_retrieval_answer(
                    question=question,
                    thread_context=synthesis_thread_context,
                    evidence_payload=fallback_evidence,
                    provider="claude",
                    claude_client=context.claude_client,
                    system_prompt=_get_freeform_system_prompt(question, synthesis_thread_context),
                    extra_rules=_build_company_retrieval_rules(fallback_evidence),
                    evidence_transform=_transform_company_retrieval_payload,
                )
                if answer:
                    context.reply(answer)
                    context.logger.info(
                        "Responded with claude answer using barcode evidence in thread_ts=%s barcode=%s",
                        context.thread_ts,
                        context.barcode,
                    )
                    return True
                context.logger.warning(
                    "Claude barcode evidence synthesis returned empty in thread_ts=%s barcode=%s",
                    context.thread_ts,
                    context.barcode,
                )
            model_input = _build_model_input(question, thread_context)
            answer = _ask_claude(
                context.claude_client,
                model_input,
                system_prompt=_build_freeform_chat_system_prompt(
                    question,
                    thread_context,
                    speaker_user_id=context.user_id,
                ),
            )
            answer = _sanitize_freeform_reply(answer)
            if not answer:
                answer = "답변을 생성하지 못했어. 다시 질문해줘"
            context.reply(answer)
            context.logger.info("Responded with claude answer in thread_ts=%s", context.thread_ts)
        except TimeoutError:
            context.logger.warning("Claude API timeout")
            context.reply(deps.timeout_reply_text())
        except anthropic.AuthenticationError:
            context.logger.exception("Claude API authentication failed")
            context.reply(_build_claude_api_key_invalid_reply())
        except anthropic.RateLimitError as exc:
            context.logger.exception("Claude API rate limit failed")
            if _is_claude_credit_unavailable_error(exc):
                context.reply(_build_claude_credit_unavailable_reply())
            else:
                context.reply("API 호출 제한으로 지금은 AI 답변을 생성할 수 없어. 잠시 후 다시 시도해줘")
        except anthropic.PermissionDeniedError as exc:
            context.logger.exception("Claude API permission denied")
            if _is_claude_credit_unavailable_error(exc):
                context.reply(_build_claude_credit_unavailable_reply())
            else:
                context.reply(_build_claude_permission_denied_reply())
        except anthropic.APIStatusError as exc:
            context.logger.exception("Claude API status failed")
            # Anthropic 잔액 부족은 운영에서 400 BadRequest로도 내려와서 status 계열에서 한 번 더 분류한다.
            if _is_claude_credit_unavailable_error(exc):
                context.reply(_build_claude_credit_unavailable_reply())
            else:
                context.reply("AI 응답 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        except Exception:
            context.logger.exception("Claude API call failed")
            context.reply("AI 응답 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if provider == "ollama":
        _set_request_log_route(
            context.payload,
            "llm_freeform",
            route_mode="ollama",
            handler_type="llm_freeform",
        )
        if not question:
            context.reply("질문 내용을 같이 보내줘. 지원 기능이 궁금하면 `사용법`이라고 보내줘")
            return True
        try:
            thread_context = _load_slack_thread_context(
                context.client,
                context.logger,
                context.channel_id,
                context.thread_ts,
                context.current_ts,
            )
            if is_prompt_exfiltration_attempt(question, thread_context):
                context.logger.warning(
                    "Blocked freeform prompt exfiltration attempt in thread_ts=%s question=%s",
                    context.thread_ts,
                    question,
                )
                context.reply(build_prompt_security_refusal())
                return True
            health = _check_ollama_health()
            if not health["ok"]:
                context.logger.warning("Ollama unavailable before answer generation: %s", health["summary"])
                context.reply(deps.llm_unavailable_reply_text(str(health["summary"])))
                return True
            fallback_evidence = deps.build_barcode_fallback_evidence()
            if fallback_evidence is not None:
                synthesis_thread_context = ""
                if s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT:
                    synthesis_thread_context = _load_slack_thread_context(
                        context.client,
                        context.logger,
                        context.channel_id,
                        context.thread_ts,
                        context.current_ts,
                    )
                answer = _synthesize_retrieval_answer(
                    question=question,
                    thread_context=synthesis_thread_context,
                    evidence_payload=fallback_evidence,
                    provider="ollama",
                    claude_client=None,
                    system_prompt=_get_freeform_system_prompt(question, synthesis_thread_context),
                    extra_rules=_build_company_retrieval_rules(fallback_evidence),
                    evidence_transform=_transform_company_retrieval_payload,
                )
                if answer:
                    context.reply(answer)
                    context.logger.info(
                        "Responded with ollama answer using barcode evidence in thread_ts=%s barcode=%s",
                        context.thread_ts,
                        context.barcode,
                    )
                    return True
                context.logger.warning(
                    "Ollama barcode evidence synthesis returned empty in thread_ts=%s barcode=%s",
                    context.thread_ts,
                    context.barcode,
                )
            model_input = _build_model_input(question, thread_context)
            answer = _ask_ollama_chat(
                model_input,
                system_prompt=_build_freeform_chat_system_prompt(
                    question,
                    thread_context,
                    speaker_user_id=context.user_id,
                ),
                think=False,
            )
            answer = _sanitize_freeform_reply(answer)
            if not answer:
                answer = "답변을 생성하지 못했어. 다시 질문해줘"
            context.reply(answer)
            context.logger.info("Responded with ollama answer in thread_ts=%s", context.thread_ts)
        except TimeoutError:
            context.logger.warning("Ollama API timeout")
            context.reply(deps.timeout_reply_text())
        except RuntimeError as exc:
            if deps.is_timeout_error(exc):
                context.logger.warning("Ollama API timeout")
                context.reply(deps.timeout_reply_text())
                return True
            context.logger.exception("Ollama API call failed")
            context.reply("Ollama 응답 중 오류가 발생했어. 서버 연결 상태를 확인해줘")
        except Exception:
            context.logger.exception("Ollama API call failed")
            context.reply("Ollama 응답 중 오류가 발생했어. 서버 연결 상태를 확인해줘")
        return True

    return False
