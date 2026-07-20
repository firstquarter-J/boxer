from __future__ import annotations

import logging
import re
import threading
import uuid
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from boxer_company.hpa_change_workflow import (
    HpaChangeJob,
    HpaChangePollResult,
    HpaChangePollState,
    HpaChangeStatus,
    redact_sensitive_text,
)
from boxer_company_adapter_slack.hpa_change_runtime import HpaChangeRuntime


_REPORTABLE_STATUSES = (
    HpaChangeStatus.RECEIVED,
    HpaChangeStatus.DISPATCHING,
    HpaChangeStatus.DISPATCHED,
    HpaChangeStatus.RUNNING,
    HpaChangeStatus.WORKFLOW_SUCCEEDED,
    HpaChangeStatus.RESULT_READY,
    HpaChangeStatus.REVIEW_READY,
    HpaChangeStatus.REVIEW_POSTED,
    HpaChangeStatus.NEEDS_CLARIFICATION,
    HpaChangeStatus.PR_CREATED,
    HpaChangeStatus.NO_CHANGE_NEEDED,
    HpaChangeStatus.FAILED,
    HpaChangeStatus.CANCELED,
)
_ACTIVE_STATUSES = frozenset(
    {
        HpaChangeStatus.RECEIVED,
        HpaChangeStatus.DISPATCHING,
        HpaChangeStatus.DISPATCHED,
        HpaChangeStatus.RUNNING,
        HpaChangeStatus.WORKFLOW_SUCCEEDED,
        HpaChangeStatus.RESULT_READY,
    }
)
_RECOVERABLE_RESULT_STATUSES = frozenset(
    {
        "needs_clarification",
        "clarification_required",
        "blocked",
        "pr_opened",
        "pr_created",
        "failed",
        "error",
        "canceled",
        "cancelled",
        "no_change_needed",
    }
)
_SAFE_PR_URL_RE = re.compile(
    r"^https://github\.com/mmtalk-app/"
    r"(?:mmb-hospital-admin-server|mmb-hospital-admin-client)/pull/[0-9]+/?$"
)
_PUBLIC_URL_RE = re.compile(r"https?://|files\.slack\.com", re.IGNORECASE)
_REQUEST_ITEM_ID_RE = re.compile(r"^REQ-[0-9]{2}$")
_INTERNAL_IDENTIFIER_TOKEN_RE = re.compile(
    r"(?:"
    r"[A-Za-z0-9_.-]+\.(?:ts|tsx|js|jsx|mjs|cjs|py|json|ya?ml|lock|toml|sql|sh|txt|md)"
    r"|\.env(?:\.[A-Za-z0-9_-]+)?"
    r"|(?:package\.json|pnpm-lock\.yaml|Dockerfile|NestJS|TypeORM|GraphQL|ECS|"
    r"ExternalConfigService|sharp|pnpm|Vercel|Redis|MySQL|AWS|Lambda)"
    r"|(?:npm|Node\.js|Next\.js|React|Docker|Kubernetes|PM2|FastAPI|Express|"
    r"ffmpeg(?:-static)?|libvips|axios|webpack|TypeScript|JavaScript|Python|Jest|"
    r"RDS|S3|SQS|EC2|CloudFront|GitHub Actions)"
    r"|(?:process\.env|this\.)"
    r"|[A-Z][A-Z0-9]+(?:_[A-Z0-9]+)+"
    r"|[a-z][a-z0-9]*(?:_[a-z0-9]+)+"
    r"|[A-Z][A-Za-z0-9]*(?:Service|Controller|Resolver|Entity|Config|Prompt|Type)"
    r"|[A-Za-z_$][A-Za-z0-9_$]*\s*\([^)]*\)"
    r"|[a-z]+(?:[A-Z][A-Za-z0-9]*)+"
    r")"
)
_PUBLIC_HANDLING_LABELS = {
    "direct": "그대로 반영",
    "adapted": "HPA 방식으로 변환 반영",
    "not_needed": "추가 반영 불필요",
    "blocked": "제품 결정 후 반영",
}
_PUBLIC_APPLIED_STATUS_LABELS = {
    "applied": "반영 완료",
    "already_satisfied": "기존 기능으로 충족",
    "not_applicable": "이번 HPA 변경 대상 아님",
    "deferred": "추가 결정 후 반영",
}
_PUBLIC_SUMMARY_TEXTS = {
    "adaptation_available": "요청 목적은 유효하고, CR Web 원안은 HPA 제품 방식으로 바꿔 반영할 수 있어.",
    "already_supported": "요청 목적은 HPA의 기존 기능으로 이미 충족돼 추가 변경이 필요한지 확인했어.",
    "product_decision_required": "요청을 구현하기 전에 고객에게 제공할 최종 동작을 먼저 결정해야 해.",
    "mixed": "요청 항목마다 그대로 반영할 부분과 HPA 방식으로 바꿀 부분을 나눠 확인했어.",
}
_WRONG_ASSUMPTION_EXPLANATIONS = {
    "web_only_term": (
        "요청에서 사용한 명칭은 CR Web 기준이라 HPA 기능과 1:1로 대응한다고 볼 수 없어. "
        "명칭이 아니라 기능 목적을 기준으로 대응 항목을 찾아야 해."
    ),
    "copy_not_portable": (
        "첨부 코드는 CR Web 환경에 맞춰 작성된 구현 예시야. "
        "같은 품질 개선 목적은 유지하되 정식 HPA 제품 방식으로 다시 구성해야 해."
    ),
    "referenced_call_unavailable": (
        "요청에서 재사용하라고 한 호출 방식은 CR Web의 생성 흐름을 전제로 해. "
        "HPA에서는 같은 동작을 기존 제품 흐름 안에 연결해야 해."
    ),
    "configuration_not_shared": (
        "두 제품은 설정을 관리하고 적용하는 방식이 같지 않아. "
        "같은 값을 복사하는 것만으로는 동일한 동작을 보장할 수 없어."
    ),
    "timeout_baseline_differs": (
        "늘려야 하는 시간은 참고할 수 있지만 현재 시작 기준은 제품마다 다를 수 있어. "
        "HPA의 실제 처리 시간을 기준으로 안전한 여유를 적용해야 해."
    ),
    "already_satisfied": (
        "HPA는 이미 같은 목적을 다른 방식으로 충족하고 있어. "
        "중복 변경보다 기존 동작을 유지하는 편이 안전해."
    ),
    "product_decision_needed": (
        "이 항목은 코드 이식 문제가 아니라 고객에게 어떤 동작을 제공할지 정하는 제품 결정이야. "
        "결정 후 그 범위에 맞춰 구현해야 해."
    ),
}
_INCOMPATIBILITY_REASON_TEXTS = {
    "different_product_structure": (
        "CR Web과 HPA는 같은 기능을 서로 다른 제품 흐름에 연결하므로, "
        "한쪽의 코드를 그대로 복사하면 기존 기능과 자연스럽게 이어진다고 보장할 수 없어."
    ),
    "different_operating_environment": (
        "CR Web에서 검증된 실행 방식과 정식 HPA의 운영 환경은 동일하지 않아. "
        "HPA 배포와 운영 조건에 맞춘 형태로 바꿔야 해."
    ),
    "different_state_and_data_flow": (
        "입력과 생성 결과를 저장하고 다음 단계로 넘기는 방식이 달라. "
        "일부 코드만 옮기면 기존 처리 흐름을 깨뜨릴 수 있어."
    ),
    "different_release_validation": (
        "정식 제품 변경은 요청 기능뿐 아니라 기존 기능과 함께 배포·회귀 검증해야 해. "
        "CR Web에서의 단독 검증만으로는 완료 기준을 충족하지 못해."
    ),
    "web_specific_sample": (
        "첨부 코드는 구현 의도를 설명하는 CR Web 예시로 사용해. "
        "HPA에는 같은 사용자 효과를 내는 제품용 구현을 적용해야 해."
    ),
}
_REQUEST_REASON_TEXTS = {
    "directly_compatible": "요청한 제품 동작이 HPA에도 그대로 유효해 직접 반영할 수 있어.",
    "web_specific_code": "CR Web 전용 구현을 정식 HPA 제품에 그대로 넣을 수 없어 변환이 필요해.",
    "existing_hpa_capability": "HPA가 이미 같은 목적을 충족하고 있어 중복 구현이 필요하지 않아.",
    "cross_product_difference": "두 제품의 연결·운영 방식이 달라 동일 목적의 HPA 구현이 필요해.",
    "product_decision_required": "기술 구현보다 고객에게 제공할 최종 동작을 먼저 정해야 해.",
    "not_applicable": "요청 목적과 현재 HPA 제품 범위가 직접 연결되지 않아 이번 변경 대상이 아니야.",
}
_REQUEST_APPLICATION_TEXTS = {
    "implement_hpa_equivalent": (
        "요청 동작을 HPA 처리 흐름에 맞춰 다시 구성하고, 필요한 화면·서버·상태 저장 범위를 "
        "함께 연결해 구현해."
    ),
    "update_existing_behavior": (
        "기존 HPA 흐름은 유지하고 요청 동작에 필요한 화면·서버·상태 저장 범위만 조정해."
    ),
    "reuse_existing_capability": (
        "기존 HPA 기능을 재사용하고 같은 처리 흐름을 중복으로 추가하지 않아."
    ),
    "add_end_to_end_capability": (
        "사용자 동작부터 서버 처리와 결과 상태 반영까지 필요한 전체 흐름을 함께 구현해."
    ),
    "no_change_needed": "현재 HPA 흐름이 이미 요청을 충족하므로 변경하지 않아.",
    "await_product_decision": (
        "제품 동작이 결정되기 전에는 변경하지 않고 결정된 범위만 구현해."
    ),
}
_APPLIED_RESULT_TEXTS = {
    "implemented_hpa_equivalent": (
        "요청 동작을 HPA 처리 흐름에 맞춰 다시 구성하고, 필요한 화면·서버·상태 저장 범위를 "
        "함께 연결해 구현했어."
    ),
    "updated_existing_behavior": (
        "기존 HPA 흐름을 유지하면서 요청 동작에 필요한 화면·서버·상태 저장 범위를 구현했어."
    ),
    "existing_capability_reused": (
        "새 중복 구현 없이 기존 HPA 기능으로 요청을 충족하는 걸 확인했어."
    ),
    "no_change_needed": "현재 HPA 흐름이 요청을 이미 충족해 추가 변경하지 않았어.",
    "not_in_scope": "요청 범위를 확인했고 이번 HPA 변경에는 포함하지 않았어.",
    "deferred_for_decision": (
        "제품 동작 결정 전이라 관련 변경을 이번 PR에 포함하지 않았어."
    ),
}
_QUESTION_TEXTS = {
    "failure_behavior": "요청한 기능이 최종 검증을 통과하지 못했을 때 기존 결과를 유지할지, 해당 결과 생성을 실패 처리할지 결정해줘.",
    "delivery_scope": "고객이 어떤 결과를 선택해 발송할 수 있게 할지 결정해줘.",
    "data_migration": "기존 데이터도 새 기준으로 함께 변경할지 결정해줘.",
    "rollout_scope": "새 동작을 전체 고객에게 적용할지, 일부 고객부터 적용할지 결정해줘.",
    "product_priority": "서로 충돌하는 제품 동작 중 어떤 동작을 우선할지 결정해줘.",
    "other_product_decision": "구현 전에 고객에게 제공할 최종 동작을 결정해줘.",
}
_REQUEST_ITEM_COMBINATIONS = {
    "direct": {
        ("directly_compatible", "update_existing_behavior"),
        ("directly_compatible", "add_end_to_end_capability"),
    },
    "adapted": {
        ("web_specific_code", "implement_hpa_equivalent"),
        ("web_specific_code", "update_existing_behavior"),
        ("cross_product_difference", "implement_hpa_equivalent"),
        ("cross_product_difference", "update_existing_behavior"),
        ("cross_product_difference", "add_end_to_end_capability"),
    },
    "not_needed": {
        ("existing_hpa_capability", "reuse_existing_capability"),
        ("existing_hpa_capability", "no_change_needed"),
        ("not_applicable", "no_change_needed"),
    },
    "blocked": {
        ("product_decision_required", "await_product_decision"),
    },
}
_APPLIED_RESULT_COMBINATIONS = {
    "applied": {
        ("directly_compatible", "updated_existing_behavior"),
        ("directly_compatible", "implemented_hpa_equivalent"),
        ("web_specific_code", "implemented_hpa_equivalent"),
        ("web_specific_code", "updated_existing_behavior"),
        ("cross_product_difference", "implemented_hpa_equivalent"),
        ("cross_product_difference", "updated_existing_behavior"),
    },
    "already_satisfied": {
        ("existing_hpa_capability", "existing_capability_reused"),
        ("existing_hpa_capability", "no_change_needed"),
    },
    "not_applicable": {
        ("not_applicable", "not_in_scope"),
        ("not_applicable", "no_change_needed"),
    },
    "deferred": {
        ("product_decision_required", "deferred_for_decision"),
    },
}
_REPORTER_THREAD: threading.Thread | None = None
_REPORTER_THREAD_LOCK = threading.Lock()


def _has_declared_pr_payload(result: Mapping[str, Any]) -> bool:
    for key in ("prs", "pr_urls", "pull_requests"):
        if key not in result:
            continue
        value = result.get(key)
        if not isinstance(value, list) or bool(value):
            return True
    return False


def _safe_review_line(value: Any, *, max_chars: int = 240) -> str:
    text = redact_sensitive_text(str(value or ""))
    text = re.sub(r"[\x00-\x1f\x7f]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    # worker 결과가 Slack mention/link 문법을 만들더라도
    # 알림 부작용이 생기지 않게 한다.
    text = text.replace("<", "‹").replace(">", "›")
    return text[:max_chars]


def _collect_review_entries(
    result: Mapping[str, Any],
    *,
    keys: Sequence[str],
    limit: int = 3,
) -> list[Any]:
    """구조화된 정정 항목의 claim/correction/evidence를 보존해 표시한다."""

    review = result.get("review")
    containers = [result]
    if isinstance(review, Mapping):
        containers.append(review)

    rendered: list[Any] = []
    seen: set[str] = set()
    for container in containers:
        for key in keys:
            raw_items = container.get(key)
            if raw_items is None:
                continue
            candidates = raw_items if isinstance(raw_items, (list, tuple)) else [raw_items]
            for item in candidates:
                fingerprint = repr(item)
                if fingerprint in seen:
                    continue
                seen.add(fingerprint)
                rendered.append(item)
                if len(rendered) >= limit:
                    return rendered
    return rendered


def _correction_claim(item: Any) -> str:
    if isinstance(item, Mapping):
        return _safe_review_line(item.get("claim"), max_chars=240)
    return _safe_review_line(item, max_chars=240)


def _safe_public_label(
    value: Any,
    *,
    fallback: str,
    request_source: str = "",
    max_chars: int = 240,
) -> str:
    """요청자가 쓴 파일·함수 명칭은 허용하되 URL과 내부 경로는 제목에서 제거한다."""

    text = _safe_review_line(value, max_chars=max_chars)
    normalized_source = redact_sensitive_text(str(request_source or ""))
    normalized_source = re.sub(r"[\x00-\x1f\x7f]+", " ", normalized_source)
    normalized_source = re.sub(r"\s+", " ", normalized_source).strip()
    has_url = _PUBLIC_URL_RE.search(text)
    has_internal_path = re.search(
        r"(?:^|[\s(])(?:src|server|client|workspace|apps?|packages?|libs?|config|infra|scripts?|internal|private|common|modules?|utils?)/"
        r"|(?:^|[\s(])(?:[A-Za-z0-9_.-]+/){2,}[A-Za-z0-9_.-]+"
        r"|(?:^|[\s(])[a-z0-9]+(?:[-_][a-z0-9]+)+/[A-Za-z0-9_.-]+"
        r"|(?:^|[\s(])/(?:Users|home|var|tmp)/"
        r"|[A-Za-z]:\\",
        text,
    )
    internal_tokens = [match.group(0) for match in _INTERNAL_IDENTIFIER_TOKEN_RE.finditer(text)]
    has_unquoted_internal_term = any(token not in normalized_source for token in internal_tokens)
    # 공개 제목은 요청 본문 또는 첨부 파일명에서 그대로 가져온 짧은 인용만 허용한다.
    if (
        not text
        or text not in normalized_source
        or has_url
        or has_internal_path
        or has_unquoted_internal_term
    ):
        return fallback
    return text


def _request_source(job: HpaChangeJob) -> str:
    """공개 제목에서 요청자가 실제로 사용한 기술 용어만 허용하기 위한 비교 원문."""

    parts = [job.request_text]
    for attachment in job.attachments:
        # 첨부 코드 내용은 요청자 공개 명칭의 허용 목록으로 사용하지 않는다.
        parts.append(attachment.name)
    return "\n".join(parts)


def _review_mapping(result: Mapping[str, Any]) -> Mapping[str, Any]:
    review = result.get("review")
    return review if isinstance(review, Mapping) else result


def _public_report(result: Mapping[str, Any]) -> Mapping[str, Any]:
    for container in (result, _review_mapping(result)):
        value = (
            container.get("requesterView")
            or container.get("requester_view")
            or container.get("publicReport")
            or container.get("public_report")
        )
        if isinstance(value, Mapping):
            return value
    return {}


def _review_contract_is_valid(result: Mapping[str, Any]) -> bool:
    """구현 시작 전 공개 검토가 새 코드형 계약과 초기 커버리지 검증을 통과했는지 확인한다."""

    quality_gates = result.get("qualityGates") or result.get("quality_gates")
    if (
        not isinstance(quality_gates, Mapping)
        or quality_gates.get("initialRequestCoveragePassed") is not True
    ):
        return False
    report = _public_report(result)
    if str(report.get("summaryCode") or "") not in _PUBLIC_SUMMARY_TEXTS:
        return False
    wrong = report.get("wrongAssumptions") or []
    if not isinstance(wrong, (list, tuple)) or len(wrong) > 5:
        return False
    if any(
        not isinstance(item, Mapping)
        or str(item.get("explanationCode") or "") not in _WRONG_ASSUMPTION_EXPLANATIONS
        for item in wrong
    ):
        return False
    reason_codes = report.get("whyNotDirectCodes") or []
    if (
        not isinstance(reason_codes, (list, tuple))
        or not reason_codes
        or len(reason_codes) > 5
        or any(str(code or "") not in _INCOMPATIBILITY_REASON_TEXTS for code in reason_codes)
    ):
        return False
    request_items = report.get("requestItems") or []
    if not isinstance(request_items, (list, tuple)) or not 1 <= len(request_items) <= 10:
        return False
    for index, item in enumerate(request_items, 1):
        if not isinstance(item, Mapping):
            return False
        if str(item.get("itemId") or "") != f"REQ-{index:02d}":
            return False
        if str(item.get("handling") or "") not in {"direct", "adapted", "not_needed"}:
            return False
        reason_code = str(item.get("reasonCode") or "")
        application_code = str(item.get("applicationCode") or "")
        if reason_code not in _REQUEST_REASON_TEXTS:
            return False
        if application_code not in _REQUEST_APPLICATION_TEXTS:
            return False
        if (reason_code, application_code) not in _REQUEST_ITEM_COMBINATIONS.get(
            str(item.get("handling") or ""),
            set(),
        ):
            return False
    return True


def _public_summary(result: Mapping[str, Any], *, fallback: str) -> str:
    report = _public_report(result)
    summary_code = str(report.get("summaryCode") or report.get("summary_code") or "")
    return _PUBLIC_SUMMARY_TEXTS.get(summary_code, fallback)


def _public_wrong_assumptions(
    result: Mapping[str, Any],
    corrections: Sequence[Any],
    *,
    request_source: str,
) -> list[tuple[str, str]]:
    report = _public_report(result)
    raw_items = report.get("wrongAssumptions") or report.get("wrong_assumptions") or []
    candidates = raw_items if isinstance(raw_items, (list, tuple)) else [raw_items]
    rendered: list[tuple[str, str]] = []
    for item in candidates[:5]:
        if not isinstance(item, Mapping):
            continue
        assumption = _safe_public_label(
            item.get("assumption") or item.get("claim"),
            fallback="CR Web의 방식을 그대로 적용할 수 있다는 전제",
            request_source=request_source,
            max_chars=240,
        )
        if not assumption:
            continue
        explanation_code = str(
            item.get("explanationCode") or item.get("explanation_code") or ""
        )
        explanation = _WRONG_ASSUMPTION_EXPLANATIONS.get(
            explanation_code,
            (
                "이 전제는 CR Web 기준이라 HPA에 그대로 적용할 수 없어. "
                "요청 목적은 유지하면서 HPA 제품 방식으로 바꿔야 해."
            ),
        )
        rendered.append((assumption, explanation))
    if rendered:
        return rendered

    # 이전 worker artifact도 내부 correction/evidence를 공개하지 않고 요청 전제만 사용한다.
    for item in corrections[:5]:
        assumption = _safe_public_label(
            _correction_claim(item),
            fallback="CR Web의 방식을 그대로 적용할 수 있다는 전제",
            request_source=request_source,
        )
        if assumption:
            rendered.append(
                (
                    assumption,
                    "이 전제는 CR Web 기준이라 HPA에 그대로 적용할 수 없어. 요청 의도는 유지하면서 HPA 제품 방식으로 변환해야 해.",
                )
            )
    return rendered


def _public_incompatibility_reasons(result: Mapping[str, Any]) -> list[str]:
    report = _public_report(result)
    raw_items = (
        report.get("whyNotDirectCodes")
        or report.get("why_not_direct_codes")
        or report.get("whyNotDirect")
        or report.get("why_not_direct")
        or []
    )
    candidates = raw_items if isinstance(raw_items, (list, tuple)) else [raw_items]
    rendered: list[str] = []
    fallback = (
        "CR Web과 HPA는 기능을 연결하고 운영하는 방식이 달라서, "
        "코드를 그대로 옮기면 정상 동작과 배포 품질을 보장할 수 없어."
    )
    for item in candidates[:5]:
        code = str(item or "").strip()
        text = _INCOMPATIBILITY_REASON_TEXTS.get(code, "")
        if text and text not in rendered:
            rendered.append(text)
    return rendered or [fallback]


def _public_request_items(
    result: Mapping[str, Any],
    corrections: Sequence[Any],
    *,
    request_source: str,
) -> list[dict[str, str]]:
    report = _public_report(result)
    raw_items = report.get("requestItems") or report.get("request_items") or []
    candidates = raw_items if isinstance(raw_items, (list, tuple)) else [raw_items]
    rendered: list[dict[str, str]] = []
    for item in candidates[:10]:
        if not isinstance(item, Mapping):
            continue
        request = _safe_public_label(
            item.get("request") or item.get("title"),
            fallback="요청한 변경 항목",
            request_source=request_source,
            max_chars=240,
        )
        if not request:
            continue
        raw_handling = str(item.get("handling") or "adapted").strip().lower()
        handling = _PUBLIC_HANDLING_LABELS.get(
            raw_handling,
            _PUBLIC_HANDLING_LABELS["adapted"],
        )
        reason_code = str(item.get("reasonCode") or item.get("reason_code") or "")
        reason = _REQUEST_REASON_TEXTS.get(
            reason_code,
            "CR Web 원안은 HPA에 그대로 적용할 수 없어 제품 방식에 맞춘 판단이 필요해.",
        )
        application_code = str(
            item.get("applicationCode") or item.get("application_code") or ""
        )
        applied_as = _REQUEST_APPLICATION_TEXTS.get(
            application_code,
            "요청한 동작은 유지하면서 HPA 제품에 맞는 방식으로 다시 구성해 적용해.",
        )
        if (reason_code, application_code) not in _REQUEST_ITEM_COMBINATIONS.get(
            raw_handling,
            set(),
        ):
            handling = "제품 기준 재확인 필요"
            reason = "요청 항목의 처리 판단과 이유가 일치하지 않아 다시 확인해야 해."
            applied_as = "일치하는 제품 판단이 확인되기 전에는 구현하지 않아."
        rendered.append(
            {
                "request": request,
                "handling": handling,
                "reason": reason,
                "applied_as": applied_as,
            }
        )
    if rendered:
        return rendered

    for item in corrections[:5]:
        request = _safe_public_label(
            _correction_claim(item),
            fallback="요청한 변경 항목",
            request_source=request_source,
        )
        if not request:
            continue
        rendered.append(
            {
                "request": request,
                "handling": _PUBLIC_HANDLING_LABELS["adapted"],
                "reason": "CR Web 원안은 HPA에 그대로 적용할 수 없어 제품 방식에 맞춘 변환이 필요해.",
                "applied_as": "요청 의도를 유지하면서 HPA 제품에 맞는 방식으로 변환해 적용해.",
            }
        )
    return rendered or [
        {
            "request": "요청한 변경 사항",
            "handling": _PUBLIC_HANDLING_LABELS["adapted"],
            "reason": "CR Web과 HPA의 운영 방식이 달라 원안을 그대로 적용할 수 없어.",
            "applied_as": "요청 목적은 유지하고 HPA 제품에 맞게 변환해 적용해.",
        }
    ]


def _public_applied_results(
    result: Mapping[str, Any],
    *,
    request_source: str,
) -> list[dict[str, str]]:
    quality_gates = result.get("qualityGates") or result.get("quality_gates")
    if (
        not isinstance(quality_gates, Mapping)
        or quality_gates.get("requestCoveragePassed") is not True
        or quality_gates.get("initialRequestCoveragePassed") is not True
    ):
        return []
    implementation = result.get("implementation")
    if not isinstance(implementation, Mapping):
        return []
    raw_items = (
        implementation.get("appliedResults")
        or implementation.get("applied_results")
        or []
    )
    candidates = raw_items if isinstance(raw_items, (list, tuple)) else [raw_items]
    report = _public_report(result)
    expected_items = report.get("requestItems") or report.get("request_items") or []
    if not isinstance(expected_items, (list, tuple)) or not expected_items:
        return []
    if not all(isinstance(item, Mapping) for item in expected_items):
        return []

    expected_ids = [str(item.get("itemId") or item.get("item_id") or "") for item in expected_items]
    actual_ids = [
        str(item.get("itemId") or item.get("item_id") or "")
        for item in candidates
        if isinstance(item, Mapping)
    ]
    # 검토와 독립 리뷰가 같은 요청 목록을 같은 순서로 다뤘을 때만 완료 요약을 신뢰한다.
    if (
        len(candidates) != len(expected_items)
        or any(not _REQUEST_ITEM_ID_RE.fullmatch(item_id) for item_id in expected_ids)
        or len(set(expected_ids)) != len(expected_ids)
        or actual_ids != expected_ids
    ):
        return []

    rendered: list[dict[str, str]] = []
    for item, expected in zip(candidates[:10], expected_items[:10], strict=True):
        if not isinstance(item, Mapping):
            return []
        expected_request = str(expected.get("request") or "").strip()
        if str(item.get("request") or "").strip() != expected_request:
            return []
        request = _safe_public_label(
            expected_request,
            fallback="요청한 변경 항목",
            request_source=request_source,
            max_chars=240,
        )
        if not request:
            continue
        raw_status = str(item.get("status") or "").strip().lower()
        handling = _PUBLIC_APPLIED_STATUS_LABELS.get(raw_status)
        if handling is None:
            return []
        reason_code = str(item.get("reasonCode") or item.get("reason_code") or "")
        expected_handling = str(expected.get("handling") or "")
        expected_reason_code = str(
            expected.get("reasonCode") or expected.get("reason_code") or ""
        )
        expected_statuses = {
            "direct": {"applied"},
            "adapted": {"applied"},
            "not_needed": {
                "already_satisfied"
                if expected_reason_code == "existing_hpa_capability"
                else "not_applicable"
            },
            "blocked": {"deferred"},
        }.get(expected_handling)
        if (
            expected_statuses is None
            or raw_status not in expected_statuses
            or reason_code != expected_reason_code
        ):
            return []
        reason = _REQUEST_REASON_TEXTS.get(
            reason_code,
            "원안 그대로가 아니라 HPA 제품 기준으로 적용 여부와 방식을 판단했어.",
        )
        result_code = str(item.get("resultCode") or item.get("result_code") or "")
        if (reason_code, result_code) not in _APPLIED_RESULT_COMBINATIONS.get(
            raw_status,
            set(),
        ):
            return []
        applied_as = _APPLIED_RESULT_TEXTS.get(
            result_code,
            "구체적인 반영 결과는 PR 검토에서 확인해줘.",
        )
        rendered.append(
            {
                "request": request,
                "handling": handling,
                "reason": reason,
                "applied_as": applied_as,
            }
        )
    return rendered


def _public_questions(
    result: Mapping[str, Any],
    *,
    request_source: str,
) -> list[str]:
    """제품 결정 질문은 제한된 코드와 요청 원문 인용만으로 구성한다."""

    containers = [_review_mapping(result), result]
    rendered: list[str] = []
    seen: set[str] = set()
    for container in containers:
        for key in ("blockingQuestions", "blocking_questions", "questions"):
            raw_items = container.get(key)
            if raw_items is None:
                continue
            candidates = raw_items if isinstance(raw_items, (list, tuple)) else [raw_items]
            for item in candidates:
                if isinstance(item, Mapping):
                    code = str(item.get("questionCode") or item.get("question_code") or "")
                    question = _QUESTION_TEXTS.get(
                        code,
                        _QUESTION_TEXTS["other_product_decision"],
                    )
                    subject = _safe_public_label(
                        item.get("subject"),
                        fallback="요청한 제품 동작",
                        request_source=request_source,
                        max_chars=240,
                    )
                else:
                    question = _QUESTION_TEXTS["other_product_decision"]
                    subject = "요청한 제품 동작"
                value = f"• 결정 대상: {subject}\n• 확인: {question}"
                if value not in seen:
                    seen.add(value)
                    rendered.append(value)
                if len(rendered) >= 10:
                    return rendered
    return rendered


def _safe_pr_urls(values: Sequence[Any]) -> list[str]:
    urls: list[str] = []
    for value in values:
        url = str(value or "").strip()
        if _SAFE_PR_URL_RE.fullmatch(url) and url not in urls:
            urls.append(url)
        if len(urls) >= 4:
            break
    return urls


def _append_message_section(
    lines: list[str],
    title: str,
    content: Sequence[str],
) -> None:
    """Slack 문단 사이에 빈 줄을 넣어 긴 검토 결과를 섹션으로 분리한다."""

    values = [str(item).strip() for item in content if str(item).strip()]
    if not values:
        return
    if lines and lines[-1] != "":
        lines.append("")
    lines.append(f"*{title}*")
    lines.extend(values)


def _append_wrong_assumptions(
    lines: list[str],
    *,
    items: Sequence[tuple[str, str]],
) -> None:
    if lines and lines[-1] != "":
        lines.append("")
    lines.append("*잘못된 전제*")
    if not items:
        lines.append("• 확인된 잘못된 전제 없음")
        return
    for index, (assumption, explanation) in enumerate(items):
        if index:
            lines.append("")
        lines.extend(
            [
                f"• 전제: {assumption}",
                f"  설명: {explanation}",
            ]
        )


def _append_request_items(
    lines: list[str],
    *,
    title: str,
    items: Sequence[Mapping[str, str]],
    completed: bool,
) -> None:
    """요청 항목마다 원안 판단과 HPA 적용 결과를 문단으로 분리한다."""

    if lines and lines[-1] != "":
        lines.append("")
    lines.append(f"*{title}*")
    for index, item in enumerate(items, 1):
        if index > 1:
            lines.append("")
        lines.extend(
            [
                f"*{index}. {item['request']}*",
                (
                    f"• 구현 상태: {item['handling']}"
                    if completed
                    else f"• 처리 방향: {item['handling']}"
                ),
                f"• 판단 이유: {item['reason']}",
                (
                    f"• 최종 구현: {item['applied_as']}"
                    if completed
                    else f"• 구현 방식: {item['applied_as']}"
                ),
            ]
        )


def _append_public_review(
    lines: list[str],
    *,
    result: Mapping[str, Any],
    corrections: Sequence[Any],
    request_source: str,
) -> None:
    """내부 근거와 HPA 구현 식별자를 제외한 요청자용 설명만 표시한다."""

    _append_wrong_assumptions(
        lines,
        items=_public_wrong_assumptions(
            result,
            corrections,
            request_source=request_source,
        ),
    )
    _append_message_section(
        lines,
        "CR Web 코드를 그대로 못 쓰는 이유",
        [f"• {item}" for item in _public_incompatibility_reasons(result)],
    )
    _append_request_items(
        lines,
        title="HPA 구현 방식",
        items=_public_request_items(
            result,
            corrections,
            request_source=request_source,
        ),
        completed=False,
    )


def _format_hpa_change_poll_messages(poll: HpaChangePollResult) -> list[str]:
    task_id = _safe_review_line(poll.task_id, max_chars=80)
    base_lines = [f"• 요청 ID: `{task_id}`"]

    # 요청 원문·첨부·오류 원문은 출력하지 않고
    # worker가 구조화한 review 항목만 짧게 보여준다.
    result = poll.result if isinstance(poll.result, Mapping) else {}
    request_source = _request_source(poll.job)
    corrections = _collect_review_entries(
        result,
        keys=("corrections",),
    )
    questions = _public_questions(result, request_source=request_source)
    summary_fallback = (
        "요청을 HPA 제품 기준으로 검토했고, 구현 전에 요청 담당자의 결정이 필요한 항목이 있어."
        if poll.state is HpaChangePollState.NEEDS_CLARIFICATION
        else "요청한 기능을 HPA 제품 기준으로 검토했고, 필요한 항목은 HPA 방식으로 바꿔 적용할 수 있어."
    )
    summary = _public_summary(result, fallback=summary_fallback)

    if poll.state is HpaChangePollState.RUNNING:
        return ["\n".join(["*HPA 코드 변경 작업 진행 중*", *base_lines])]

    if poll.state is HpaChangePollState.REVIEW_READY:
        lines = [
            "*HPA 코드 변경 검토 결과*",
            "",
            "• 상태: 검토 완료 · 구현 시작 전",
            *base_lines,
        ]
        _append_message_section(lines, "검토 요약", [f"• {summary}"])
        _append_public_review(
            lines,
            result=result,
            corrections=corrections,
            request_source=request_source,
        )
        _append_message_section(
            lines,
            "다음 단계",
            ["• 이 검토 결과가 스레드에 게시된 뒤 HPA 구현을 시작해"],
        )
        return ["\n".join(lines)]

    if poll.state is HpaChangePollState.NEEDS_CLARIFICATION:
        review_lines = [
            "*HPA 코드 변경 검토 결과*",
            "",
            "• 상태: 추가 확인 필요",
            *base_lines,
        ]
        _append_message_section(review_lines, "검토 요약", [f"• {summary}"])
        _append_public_review(
            review_lines,
            result=result,
            corrections=corrections,
            request_source=request_source,
        )

        question_lines = ["*HPA 추가 확인 질문*", "", *base_lines]
        if questions:
            for index, item in enumerate(questions, 1):
                question_lines.extend(["", f"*질문 {index}*", item])
        else:
            question_lines.extend(
                ["", "*질문 1*", "구현 전에 결정할 내용이 있어. 요청 담당자가 결과를 확인해줘"]
            )
        return ["\n".join(review_lines), "\n".join(question_lines)]

    if poll.state is HpaChangePollState.PR_OPENED:
        applied_results = _public_applied_results(
            result,
            request_source=request_source,
        )
        quality_gates = result.get("qualityGates") or result.get("quality_gates")
        verification_complete = (
            isinstance(quality_gates, Mapping)
            and quality_gates.get("verificationPassed") is True
            and quality_gates.get("independentReviewPassed") is True
            and quality_gates.get("requestCoveragePassed") is True
            and quality_gates.get("initialRequestCoveragePassed") is True
            and bool(applied_results)
        )
        lines = [
            "*HPA 코드 변경 PR 준비 완료*",
            "",
            (
                "• 상태: 구현·검증 완료 · PR 준비"
                if verification_complete
                else "• 상태: 구현 완료 · 검증 정보 확인 필요 · PR 준비"
            ),
            "• 운영 반영: 미머지 · 미배포",
            *base_lines,
        ]
        if applied_results:
            _append_request_items(
                lines,
                title="최종 구현 결과",
                items=applied_results,
                completed=True,
            )
        else:
            _append_message_section(
                lines,
                "최종 구현 결과",
                ["• 자동화 결과에 공개 가능한 항목별 요약이 없어 PR에서 확인이 필요해"],
            )
        if verification_complete:
            verification_lines = ["• 자동 빌드·테스트와 독립 리뷰를 통과했어"]
        else:
            verification_lines = ["• 이 결과에는 자동 검증 통과 정보가 없어 PR에서 확인이 필요해"]
        _append_message_section(lines, "검증 결과", verification_lines)
        pr_urls = _safe_pr_urls(poll.pr_urls)
        _append_message_section(
            lines,
            "PR",
            [f"• {url}" for url in pr_urls]
            or ["• 유효한 PR 링크를 확인하지 못했어"],
        )
        _append_message_section(lines, "다음 단계", ["• 현 승인 후 머지·배포"])
        return ["\n".join(lines)]

    if poll.state is HpaChangePollState.NO_CHANGE_NEEDED:
        lines = [
            "*HPA 코드 변경 검토 완료*",
            "",
            "• 상태: 코드 변경 불필요 · PR 없음",
            *base_lines,
        ]
        applied_results = _public_applied_results(
            result,
            request_source=request_source,
        )
        if applied_results:
            _append_request_items(
                lines,
                title="최종 확인 결과",
                items=applied_results,
                completed=True,
            )
        else:
            _append_message_section(
                lines,
                "최종 확인 결과",
                ["• 공개 가능한 항목별 확인 결과가 없어 운영 확인이 필요해"],
            )
        quality_gates = result.get("qualityGates") or result.get("quality_gates")
        if (
            isinstance(quality_gates, Mapping)
            and quality_gates.get("verificationPassed") is True
            and quality_gates.get("independentReviewPassed") is True
            and quality_gates.get("requestCoveragePassed") is True
            and quality_gates.get("initialRequestCoveragePassed") is True
            and bool(applied_results)
        ):
            verification_lines = ["• 자동 빌드·테스트와 독립 리뷰를 통과했어"]
        else:
            verification_lines = ["• 자동 검증 통과 정보를 확인하지 못해 운영 확인이 필요해"]
        _append_message_section(lines, "검증 결과", verification_lines)
        _append_message_section(
            lines,
            "결론",
            [
                "• 기존 기능으로 충족됐거나 이번 HPA 변경 대상이 아닌 항목이라 "
                "코드 변경과 PR을 만들지 않았어"
            ],
        )
        return ["\n".join(lines)]

    if poll.state is HpaChangePollState.FAILED:
        return [
            "\n".join(
                [
                    "*HPA 코드 변경 자동화가 완료되지 못했어*",
                    "",
                    *base_lines,
                    "",
                    (
                        "• 안내: 내부 오류 원문은 노출하지 않았어. "
                        "운영 로그와 worker 상태를 확인해줘"
                    ),
                ]
            )
        ]

    return []


def _format_hpa_change_poll_message(poll: HpaChangePollResult) -> str:
    """호출부 호환을 위해 첫 번째(검토 요약) 댓글만 반환한다."""

    messages = _format_hpa_change_poll_messages(poll)
    return messages[0] if messages else ""


def _is_timed_out(job: HpaChangeJob, runtime: HpaChangeRuntime, now: datetime) -> bool:
    if job.status not in _ACTIVE_STATUSES:
        return False
    if job.status is HpaChangeStatus.RESULT_READY and isinstance(job.result, Mapping):
        raw_status = str(job.result.get("status") or "").strip().lower().replace("-", "_")
        recoverable = raw_status in _RECOVERABLE_RESULT_STATUSES or (
            raw_status in {"completed", "success"}
            and _has_declared_pr_payload(job.result)
        )
        if recoverable:
            # 저장된 terminal 결과는 timeout보다 먼저 재적용해야 한다.
            return False
    phase_started_at = job.phase_started_at
    if phase_started_at.tzinfo is None:
        phase_started_at = phase_started_at.replace(tzinfo=timezone.utc)
    elapsed_seconds = (
        now.astimezone(timezone.utc) - phase_started_at.astimezone(timezone.utc)
    ).total_seconds()
    return elapsed_seconds > max(1, int(runtime.run_timeout_sec))


def _post_hpa_change_message(
    client: Any,
    job: HpaChangeJob,
    text: str,
    *,
    message_key: str,
) -> None:
    # 자동화 상태는 이미 같은 요청 thread에 이어지므로 별도 사용자 멘션 없이 게시한다.
    # Slack 재시도나 응답 유실에도 같은 logical 댓글이 중복 생성되지 않도록
    # task와 댓글 위치에서 결정적인 UUID를 만든다.
    client_msg_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            (
                f"boxer:hpa-change:{job.task_id}:{job.workflow_phase}:"
                f"{job.dispatch_count}:{job.workflow_run_id or 0}:{message_key}"
            ),
        )
    )
    client.chat_postMessage(
        channel=job.channel_id,
        thread_ts=job.thread_ts,
        text=text,
        client_msg_id=client_msg_id,
        unfurl_links=False,
        unfurl_media=False,
    )


def run_hpa_change_reporter_once(
    runtime: HpaChangeRuntime,
    client: Any,
    *,
    logger: logging.Logger | None = None,
    now: datetime | None = None,
) -> int:
    """SQLite 작업을 한 번 전진시키고 발송 성공 상태만 기록한다."""

    if not runtime.enabled or runtime.store is None or runtime.workflow is None:
        return 0
    actual_logger = logger or logging.getLogger(__name__)
    actual_now = now or datetime.now(timezone.utc)
    sent_count = 0

    list_reportable_jobs = getattr(runtime.store, "list_reportable_jobs", None)
    if callable(list_reportable_jobs):
        # 이미 알린 오래된 terminal row를 SQL에서 제외해
        # 누적 작업이 새 job을 가리지 않게 한다.
        jobs = list_reportable_jobs(limit=500)
    else:
        jobs = runtime.store.list_jobs(statuses=_REPORTABLE_STATUSES, limit=500)
    for listed_job in jobs:
        try:
            # terminal 알림을 이미 보낸 작업은 GitHub를 다시 조회하지 않는다.
            listed_terminal_state = {
                HpaChangeStatus.NEEDS_CLARIFICATION: HpaChangePollState.NEEDS_CLARIFICATION,
                HpaChangeStatus.PR_CREATED: HpaChangePollState.PR_OPENED,
                HpaChangeStatus.NO_CHANGE_NEEDED: HpaChangePollState.NO_CHANGE_NEEDED,
                HpaChangeStatus.FAILED: HpaChangePollState.FAILED,
                HpaChangeStatus.CANCELED: HpaChangePollState.FAILED,
            }.get(listed_job.status)
            if (
                listed_terminal_state is not None
                and listed_job.notified_status == listed_terminal_state.value
            ):
                continue

            if _is_timed_out(listed_job, runtime, actual_now):
                # timeout도 merge/deploy 같은 복구 동작 없이
                # 실패 상태와 알림만 남긴다.
                runtime.store.mark_failed(
                    listed_job.task_id,
                    "HPA 변경 worker 실행 제한 시간 초과",
                )
            poll = runtime.workflow.poll_job(listed_job.task_id)
            if poll.state is HpaChangePollState.QUEUED:
                continue
            if poll.job.notified_status == poll.state.value:
                continue

            # 검토 artifact가 누락·부분 목록·자유 문장 형식이면 Slack에 게시하거나
            # 구현 workflow를 시작하지 않고 안전하게 실패 처리한다.
            if poll.state is HpaChangePollState.REVIEW_READY and not _review_contract_is_valid(
                poll.result if isinstance(poll.result, Mapping) else {}
            ):
                failed_job = runtime.store.mark_failed(
                    poll.task_id,
                    "HPA 공개 검토 계약 또는 초기 요청 커버리지 검증 실패",
                )
                failure_message = "\n".join(
                    [
                        "*HPA 코드 변경 자동화가 완료되지 못했어*",
                        "",
                        f"• 요청 ID: `{_safe_review_line(poll.task_id, max_chars=80)}`",
                        "",
                        "• 안내: 검토 결과의 요청 범위를 안전하게 확인하지 못해 구현을 시작하지 않았어",
                    ]
                )
                _post_hpa_change_message(
                    client,
                    failed_job,
                    failure_message,
                    message_key="failed:1",
                )
                runtime.store.mark_notified(poll.task_id, HpaChangePollState.FAILED)
                sent_count += 1
                continue

            messages = _format_hpa_change_poll_messages(poll)
            if not messages:
                continue
            # 검토 요약을 먼저 올리고, 추가 질문은 다음 thread 댓글로 분리한다.
            # 어느 한 댓글이라도 실패하면 notified를 남기지 않아 다음 poll에서 재시도한다.
            for index, message in enumerate(messages, 1):
                _post_hpa_change_message(
                    client,
                    poll.job,
                    message,
                    message_key=f"{poll.state.value}:{index}",
                )
            # 검토 결과 게시를 영속화한 뒤에만 별도 구현 workflow를 시작한다.
            # 게시 직후 프로세스가 내려가도 REVIEW_POSTED 상태에서 dispatch를 재개한다.
            if poll.state is HpaChangePollState.REVIEW_READY:
                runtime.store.mark_review_posted(poll.task_id)
                runtime.store.mark_notified(poll.task_id, poll.state)
                runtime.workflow.dispatch_implementation(poll.task_id)
            else:
                # Slack 발송이 성공한 뒤에만 표시해서
                # 재시작·일시 오류 시 알림이 유실되지 않게 한다.
                runtime.store.mark_notified(poll.task_id, poll.state)
            sent_count += 1
        except Exception as exc:
            # request/result 원문과 GitHub 응답을 로그에 흘리지 않는다.
            actual_logger.warning(
                "Failed to poll or report HPA change task_id=%s error_type=%s",
                listed_job.task_id,
                type(exc).__name__,
            )
    return sent_count


def _hpa_change_reporter_loop(
    runtime: HpaChangeRuntime,
    client: Any,
    logger: logging.Logger,
) -> None:
    interval = max(1, int(runtime.poll_interval_sec))
    while True:
        run_hpa_change_reporter_once(runtime, client, logger=logger)
        threading.Event().wait(interval)


def attach_hpa_change_reporter(
    app: Any,
    runtime: HpaChangeRuntime,
    *,
    logger: logging.Logger | None = None,
) -> None:
    if not runtime.enabled:
        return
    actual_logger = logger or logging.getLogger(__name__)
    client = getattr(app, "client", None)
    if client is None:
        # 활성화 설정인데 client가 없으면 알림을 시작하지 않아
        # silently 잃어버리지 않는다.
        raise RuntimeError("HPA 변경 reporter를 시작할 Slack client가 없어")

    global _REPORTER_THREAD
    with _REPORTER_THREAD_LOCK:
        if _REPORTER_THREAD is not None and _REPORTER_THREAD.is_alive():
            return
        _REPORTER_THREAD = threading.Thread(
            target=_hpa_change_reporter_loop,
            args=(runtime, client, actual_logger),
            name="hpa-change-reporter",
            daemon=True,
        )
        _REPORTER_THREAD.start()
    actual_logger.info("Started HPA change reporter")


__all__ = [
    "attach_hpa_change_reporter",
    "run_hpa_change_reporter_once",
]
