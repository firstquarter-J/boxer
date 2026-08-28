from __future__ import annotations

from collections.abc import Mapping, Sequence
import re
from typing import Any

from boxer_company.transport_contracts import (
    HpaChangePollResult,
    HpaChangePollState,
)


_SAFE_PR_URL_RE = re.compile(
    r"^https://github\.com/mmtalk-app/"
    r"(?:mmb-hospital-admin-server|mmb-hospital-admin-client)/pull/[0-9]+/?$"
)
_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)
_MENTION_RE = re.compile(r"<@[A-Z0-9]+(?:\|[^>]+)?>")
_SECRET_RE = re.compile(
    r"(?i)(?:github_pat_|ghp_|xox[baprs]-|bearer\s+|"
    r"(?:api[_-]?key|password|secret|token)\s*[:=])\S+"
)
_INTERNAL_TOKEN_RE = re.compile(
    r"(?i)(?:"
    r"\.?[A-Za-z0-9_./-]+\.(?:ts|tsx|js|jsx|py|json|ya?ml|toml|sql|sh|md)"
    r"|[A-Z][A-Z0-9]+(?:_[A-Z0-9]+)+"
    r"|[a-z][a-z0-9]*(?:_[a-z0-9]+)+"
    r"|(?:Redis|MySQL|AWS|ECS|PM2|Vercel|NestJS|TypeORM|GraphQL|S3|RDS)"
    r")"
)
_SUMMARY_TEXTS = {
    "adaptation_available": "요청 목적은 유효하고 HPA 제품 방식으로 바꿔 반영할 수 있어.",
    "already_supported": "요청 목적은 HPA의 기존 기능으로 이미 충족되는지 확인했어.",
    "product_decision_required": "구현 전에 최종 제품 동작을 먼저 결정해야 해.",
    "mixed": "항목마다 그대로 반영할 부분과 HPA 방식으로 바꿀 부분을 나눠 확인했어.",
}
_ASSUMPTION_EXPLANATIONS = {
    "web_only_term": "CR Web 명칭과 HPA 기능은 1:1로 대응하지 않아 기능 목적을 기준으로 봐야 해.",
    "copy_not_portable": "같은 목적은 유지하되 HPA 제품 방식으로 다시 구성해야 해.",
    "referenced_call_unavailable": "CR Web 호출 방식을 복사하지 않고 HPA 기존 흐름에 연결해야 해.",
    "configuration_not_shared": "제품별 설정 적용 방식이 달라 같은 값을 복사해도 동작을 보장할 수 없어.",
    "timeout_baseline_differs": "HPA의 실제 처리 시간을 기준으로 안전한 여유를 정해야 해.",
    "already_satisfied": "중복 변경보다 HPA의 기존 기능을 유지하는 편이 안전해.",
    "product_decision_needed": "코드 이식보다 고객에게 제공할 동작을 먼저 결정해야 해.",
}
_HANDLING_LABELS = {
    "direct": "그대로 반영",
    "adapted": "HPA 방식으로 변환 반영",
    "not_needed": "추가 반영 불필요",
    "blocked": "제품 결정 후 반영",
}
_RESULT_LABELS = {
    "applied": "반영 완료",
    "already_satisfied": "기존 기능으로 충족",
    "not_applicable": "이번 HPA 변경 대상 아님",
    "deferred": "추가 결정 후 반영",
}


def _safe_line(value: Any, *, fallback: str = "확인 필요", limit: int = 240) -> str:
    text = " ".join(str(value or "").split())
    text = _MENTION_RE.sub("", text)
    text = _URL_RE.sub("", text)
    text = _SECRET_RE.sub("[보호됨]", text)
    text = _INTERNAL_TOKEN_RE.sub("내부 구현", text)
    text = " ".join(text.split()).strip(" -:;,.")
    return (text or fallback)[:limit]


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _requester_view(result: Mapping[str, Any]) -> Mapping[str, Any]:
    review = _mapping(result.get("review"))
    return _mapping(review.get("requesterView"))


def _request_items(result: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw = _requester_view(result).get("requestItems")
    if not isinstance(raw, list):
        return []
    return [item for item in raw[:20] if isinstance(item, Mapping)]


def _applied_results(result: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw = _mapping(result.get("implementation")).get("appliedResults")
    if not isinstance(raw, list):
        return []
    return [item for item in raw[:20] if isinstance(item, Mapping)]


def _review_message(result: Mapping[str, Any]) -> str:
    view = _requester_view(result)
    summary = _SUMMARY_TEXTS.get(
        str(view.get("summaryCode") or ""),
        "요청 담당자의 결정이 필요한 항목이 있어.",
    )
    lines = ["*HPA 변경 요청 검토*", f"• 요약: {summary}"]
    assumptions = view.get("wrongAssumptions")
    if isinstance(assumptions, list):
        safe_assumptions = [item for item in assumptions[:5] if isinstance(item, Mapping)]
        if safe_assumptions:
            lines.append("\n*CR Web 코드를 그대로 못 쓰는 이유*")
            for item in safe_assumptions:
                lines.append(f"• 전제: {_safe_line(item.get('assumption'))}")
                explanation = _ASSUMPTION_EXPLANATIONS.get(
                    str(item.get("explanationCode") or ""),
                    "HPA의 실제 제품 경계에 맞춰 다시 확인해야 해.",
                )
                lines.append(f"  ↳ {explanation}")
    items = _request_items(result)
    if items:
        lines.append("\n*HPA 구현 방식*")
        for index, item in enumerate(items, 1):
            label = _HANDLING_LABELS.get(
                str(item.get("handling") or ""),
                "확인 필요",
            )
            lines.append(f"*{index}. {_safe_line(item.get('request'), fallback='요청 항목')}*")
            lines.append(f"• 처리: {label}")
    return "\n".join(lines)


def _question_message(result: Mapping[str, Any]) -> str | None:
    raw = _mapping(result.get("review")).get("blocking_questions")
    if not isinstance(raw, list) or not raw:
        return None
    lines = ["*추가 확인이 필요해*"]
    for index, item in enumerate(raw[:5], 1):
        data = item if isinstance(item, Mapping) else {"question": item}
        subject = _safe_line(
            data.get("subject") or data.get("question"),
            fallback="제품 동작 결정",
        )
        lines.extend((f"\n*질문 {index}*", f"• 결정 대상: {subject}"))
    return "\n".join(lines)


def _implementation_message(
    result: Mapping[str, Any],
    *,
    no_change: bool,
    pr_urls: Sequence[str],
) -> str:
    title = "*최종 확인 결과*" if no_change else "*HPA 변경 작업 완료*"
    lines = [title]
    lines.append(
        "• 상태: 코드 변경 불필요 · PR 없음"
        if no_change
        else "• 상태: 구현·검증 완료 · PR 준비"
    )
    applied = _applied_results(result)
    if applied:
        lines.append("\n*최종 구현 결과*")
        for index, item in enumerate(applied, 1):
            lines.append(
                f"*{index}. {_safe_line(item.get('request'), fallback='요청 항목')}*"
            )
            lines.append(
                "• 구현 상태: "
                + _RESULT_LABELS.get(
                    str(item.get("status") or ""),
                    "확인 필요",
                )
            )
    elif not no_change:
        lines.append("• 공개 가능한 항목별 요약이 없어 PR에서 확인이 필요해.")
    safe_urls = [url for url in pr_urls if _SAFE_PR_URL_RE.fullmatch(str(url or ""))]
    if safe_urls:
        lines.append("\n*PR*")
        lines.extend(f"• {url}" for url in safe_urls[:4])
        lines.append("• 운영 반영: 미머지 · 미배포")
    elif no_change:
        lines.append("• 코드 변경과 PR을 만들지 않았어.")
    return "\n".join(lines)


def _format_hpa_change_poll_messages(poll: HpaChangePollResult) -> list[str]:
    """API-owned HPA snapshot을 공개 가능한 Slack 문구로만 렌더링한다."""

    result = dict(poll.result) if isinstance(poll.result, Mapping) else {}
    if poll.state is HpaChangePollState.NEEDS_CLARIFICATION:
        messages = [_review_message(result)]
        question = _question_message(result)
        if question:
            messages.append(question)
        return messages
    if poll.state is HpaChangePollState.REVIEW_READY:
        return [_review_message(result)]
    if poll.state is HpaChangePollState.PR_OPENED:
        return [
            _implementation_message(
                result,
                no_change=False,
                pr_urls=poll.pr_urls,
            )
        ]
    if poll.state is HpaChangePollState.NO_CHANGE_NEEDED:
        return [
            _implementation_message(
                result,
                no_change=True,
                pr_urls=(),
            )
        ]
    if poll.state is HpaChangePollState.FAILED:
        return ["*HPA 변경 작업 실패*\n• 내부 작업 상태를 확인해줘. 자동 재실행하지 않았어."]
    if poll.state is HpaChangePollState.RUNNING:
        return ["*HPA 변경 작업 진행 중*\n• 격리 worker에서 검토·구현 중이야."]
    return ["*HPA 변경 요청 접수*\n• 작업 큐에서 순서를 기다리고 있어."]


__all__ = [
    "_format_hpa_change_poll_messages",
]
