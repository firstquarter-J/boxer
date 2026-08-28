from __future__ import annotations

from boxer_company._operation_routing_knowledge import (
    _NOTION_PLAYBOOK_ROUTE,
    _has_notion_playbook_context,
    _match_notion_playbook_route,
    _request_context_text,
)
from boxer_company.read_routing import (
    AssistantRequestScopeMismatch,
    _BARCODE_EVIDENCE_FREEFORM_ROUTE,
    looks_like_notion_playbook_followup,
    looks_like_notion_playbook_question,
    resolve_assistant_request_scope,
)

from dataclasses import dataclass
from copy import deepcopy
import logging
import re
from typing import Any, Callable
from urllib.parse import urlsplit

from boxer_company import settings as cs
from boxer_company.assistant.answer_composer import (
    CompanyEvidenceAnswerComposer,
    CompanyEvidenceAnswerPolicy,
)
from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
    SourceReference,
)
from boxer_company.assistant.freeform_prompt import (
    build_company_freeform_system_prompt,
)
from boxer_company.assistant.notion_answer_safety import (
    build_notion_document_security_refusal,
    needs_notion_document_security_refusal,
)
from boxer_company.assistant.scope_guard import (
    build_scope_mismatch_result,
)
from boxer_company.assistant.service import (
    RecordingsContextBarcodeMismatch,
    RequestScopedRecordingsContext,
)
from boxer_company.notion_playbooks import (
    _is_company_notion_configured,
    _select_notion_references,
)
from boxer_company.prompt_security import (
    build_prompt_security_refusal,
    is_prompt_exfiltration_attempt,
)
from boxer_company.retrieval_rules import (
    _build_company_retrieval_rules,
    _is_notion_doc_general_overview_question,
    _transform_company_retrieval_payload,
)
from boxer_company.routers.device_diagnostics import (
    _build_device_diagnostic_followup_fallback,
)


DiagnosticSnapshotLoader = Callable[
    [CompanyAssistantRequest],
    dict[str, Any] | None,
]
DiagnosticFallbackBuilder = Callable[[str, dict[str, Any]], str]
NotionReferenceSelector = Callable[
    [str, dict[str, Any]],
    list[dict[str, Any]],
]
NotionFallbackBuilder = Callable[[str, list[dict[str, Any]]], str]
FreeformSystemPromptBuilder = Callable[
    [CompanyAssistantRequest, str],
    str | None,
]


_NOTION_EXFILTRATION_PATTERNS = (
    re.compile(
        r"(시스템\s*(정보|프롬프트|지시문)|system\s*prompt|developer\s*prompt|internal\s*prompt|hidden\s*prompt|instruction\s*prompt)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(문서\s*(원문|전문|본문)|원문|전문|본문|raw\s*text|full\s*text|complete\s*text|entire\s*text|whole\s*text|verbatim|dump|텍스트\s*전체|전체\s*텍스트)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(하나하나\s*(오픈|열)|하나씩\s*(오픈|열)|전부\s*보여|모두\s*보여|통째로\s*보여|텍스트로\s*보여|그대로\s*보여|show\s+me|open\s+each|open\s+every|one\s+by\s+one|full\s+text|all\s+text)",
        re.IGNORECASE,
    ),
    re.compile(
        r"((i\s*am|i'?m|im)\s+(super\s+admin|admin|owner|developer|maintainer)|super\s+admin|admin\s+mode|override|ignore\s+(previous|all)\s+(rules|instructions)|bypass)",
        re.IGNORECASE,
    ),
)
_SAFE_NOTION_URL_HOSTS = {
    "www.notion.so",
    "notion.so",
    "app.notion.com",
}


def build_notion_playbook_query(
    question: str,
    context_text: str,
) -> str:
    normalized_question = (question or "").strip()
    if not looks_like_notion_playbook_followup(
        normalized_question,
        context_text,
    ):
        return normalized_question
    context_lines = [
        line.strip()
        for line in (context_text or "").splitlines()
        if line.strip()
    ]
    relevant_context = "\n".join(context_lines[-6:])
    if not relevant_context:
        return normalized_question
    return f"{relevant_context}\n{normalized_question}".strip()


def is_notion_playbook_exfiltration_attempt(
    question: str,
    context_text: str = "",
) -> bool:
    text = (question or "").strip()
    if not text:
        return False
    if not (
        looks_like_notion_playbook_question(text)
        or _has_notion_playbook_context(context_text)
    ):
        return False
    return any(pattern.search(text) for pattern in _NOTION_EXFILTRATION_PATTERNS)


def sanitize_notion_playbook_references(
    references: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """합성 입력에는 식별자·URL·원문 대신 짧은 플레이북 발췌만 남긴다."""
    sanitized: list[dict[str, Any]] = []
    for item in (
        reference
        for reference in (references or [])
        if isinstance(reference, dict)
    ):
        preview_lines = [
            str(line).strip()[:160]
            for line in (item.get("previewLines") or [])
            if str(line).strip()
        ][:5]
        sanitized.append(
            {
                "title": str(item.get("title") or "").strip()[:160],
                "section": str(item.get("section") or "").strip()[:120],
                "kind": str(item.get("kind") or "").strip()[:80],
                "priority": str(item.get("priority") or "").strip()[:40],
                "matchedKeywords": [
                    str(keyword).strip()[:80]
                    for keyword in (item.get("matchedKeywords") or [])
                    if str(keyword).strip()
                ][:4],
                "previewLines": preview_lines,
                "summary": " / ".join(preview_lines[:3]),
            }
        )
        if len(sanitized) >= 3:
            break
    return sanitized


def build_notion_playbook_fallback(
    question: str,
    references: list[dict[str, Any]],
) -> str:
    """세부 전용 formatter가 없어도 문서 발췌만으로 안전한 최소 답변을 만든다."""
    if not references:
        return (
            "**문서 기반 답변**\n"
            "• 결론: 관련 운영 문서를 찾지 못했어\n"
            "• 확인: 증상이나 키워드를 더 구체적으로 말해줘\n"
            "• 조치: 문서 제목이나 장애 증상을 같이 보내줘"
        )

    preview_lines: list[str] = []
    for reference in references[:3]:
        for raw_line in reference.get("previewLines") or []:
            line = re.sub(r"^#+\s*|^[-*•]\s*", "", str(raw_line).strip())
            line = re.sub(r"\s+", " ", line).strip()
            line = re.sub(
                r"^(결론|확인|조치|원인|상태|주의|전환|순서)\s*:\s*",
                "",
                line,
            )
            if not line or line in preview_lines:
                continue
            preview_lines.append(line[:180])
            if len(preview_lines) >= 3:
                break
        if len(preview_lines) >= 3:
            break

    if _is_notion_doc_general_overview_question(question):
        summary = " ".join(preview_lines[:3]).strip()
        if not summary:
            summary = "마미박스 운영 범위는 문서 기준 확인이 필요해."
        return f"**문서 기반 답변**\n{summary}"

    title = str(references[0].get("title") or "관련 운영 문서").strip()
    conclusion = preview_lines[0] if preview_lines else f"`{title}` 기준 확인 필요"
    confirmation = (
        preview_lines[1]
        if len(preview_lines) >= 2
        else f"`{title}`의 전제와 현장 상태를 같이 확인해"
    )
    action = (
        preview_lines[2]
        if len(preview_lines) >= 3
        else "문서 기준에 맞춰 우선 조치해"
    )
    return (
        "**문서 기반 답변**\n"
        f"• 결론: {conclusion}\n"
        f"• 확인: {confirmation}\n"
        f"• 조치: {action}"
    )


def _default_notion_reference_selector(
    query: str,
    evidence: dict[str, Any],
) -> list[dict[str, Any]]:
    return _select_notion_references(
        query,
        evidence_payload=evidence,
        max_results=3,
    )


def _default_freeform_system_prompt(
    request: CompanyAssistantRequest,
    context_text: str,
) -> str | None:
    return build_company_freeform_system_prompt(
        request.question,
        context_text,
    )


def _handle_request_by_default(request: CompanyAssistantRequest) -> bool:
    del request
    return True


def _safe_notion_source(reference: dict[str, Any]) -> SourceReference | None:
    uri = str(reference.get("url") or "").strip()
    title = str(reference.get("title") or "").strip()
    if not uri or not title:
        return None
    try:
        parsed = urlsplit(uri)
    except ValueError:
        return None
    if (
        parsed.scheme != "https"
        or parsed.hostname not in _SAFE_NOTION_URL_HOSTS
        or parsed.username
        or parsed.password
    ):
        return None
    score_value = reference.get("score")
    score = float(score_value) if isinstance(score_value, (int, float)) else None
    return SourceReference(
        source_id=uri,
        title=title,
        uri=uri,
        score=score,
    )


def _notion_answer_preserves_fallback(
    answer: str,
    fallback: str,
    *,
    overview: bool,
) -> bool:
    normalized = (answer or "").strip()
    if not normalized.startswith("**문서 기반 답변**"):
        return False
    if overview:
        lowered = normalized.lower()
        return "마미박스" in normalized and any(
            token in lowered for token in ("운영", "녹화", "업로드", "병원")
        )

    if any(
        required not in normalized
        for required in ("• 결론:", "• 확인:", "• 조치:")
    ):
        return False
    normalized_lower = normalized.lower()
    fallback_lower = (fallback or "").lower()
    # 운영상 중요한 고정 사실은 더 짧은 생성 답변에서 빠져도 fallback으로 닫는다.
    critical_pairs = (
        ("cfg1_barcode_sync_date", "cfg1_barcode_sync_date"),
        ("약 10초 이상", "10초"),
        ("device or resource busy", "resource busy"),
        ("녹화 취소 안내 음성", "녹화 취소"),
        ("추가 구매", "추가 구매"),
        ("ssh 연결 불가만으로는", "단정 못 해"),
    )
    return all(
        source not in fallback_lower or target in normalized_lower
        for source, target in critical_pairs
    )


def _result(
    *,
    route: str,
    outcome: AssistantOutcome,
    body: str,
    sources: tuple[SourceReference, ...] = (),
    used_llm: bool = False,
    fallback_reason: str | None = None,
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route=route,
        outcome=outcome,
        messages=(AssistantMessage(body=body),),
        sources=sources,
        used_llm=used_llm,
        fallback_reason=fallback_reason,
    )


@dataclass(frozen=True, slots=True)
class DeviceDiagnosticFollowupRouteDeps:
    answer_composer: CompanyEvidenceAnswerComposer
    load_snapshot: DiagnosticSnapshotLoader
    build_fallback: DiagnosticFallbackBuilder = (
        _build_device_diagnostic_followup_fallback
    )
    timeout_message: str = (
        "AI 답변 생성 시간이 초과됐어. 잠시 후 다시 시도해줘"
    )


class DeviceDiagnosticFollowupAssistantRoute:
    name = "device_diagnostic_followup"

    def __init__(
        self,
        deps: DeviceDiagnosticFollowupRouteDeps,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._deps = deps
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        if not request.question.strip():
            return None
        try:
            scope = resolve_assistant_request_scope(request)
            snapshot = self._deps.load_snapshot(request)
            if snapshot is None:
                return None
            snapshot_device = _snapshot_device_name(snapshot)
            if (
                scope.device_name
                and snapshot_device
                and scope.device_name != snapshot_device
            ):
                return build_scope_mismatch_result(
                    AssistantRequestScopeMismatch("device")
                )
            # 저장된 진단 결과만 복사한다. follow-up route에서는 MDA sshOrder나
            # SSH 재접속을 호출하지 않아 read-only 2단계 경계를 유지한다.
            evidence = deepcopy(snapshot)
            # 저장 데이터의 route 필드가 비었거나 변조돼도 합성 transform이
            # SSH host·명령 원문을 compact하는 진단 전용 경로를 강제로 탄다.
            evidence["route"] = "device_diagnostic_snapshot"
            fallback = slack_mrkdwn_to_commonmark(
                self._deps.build_fallback(request.question, evidence)
            ).strip()
        except AssistantRequestScopeMismatch as mismatch:
            return build_scope_mismatch_result(mismatch)
        except Exception as exc:
            self._logger.exception(
                "Device diagnostic followup failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                route=self.name,
                outcome="failed",
                body="장비 진단 근거를 확인하는 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                fallback_reason="diagnostic_error",
            )

        return self._deps.answer_composer.compose(
            request,
            evidence=evidence,
            policy=CompanyEvidenceAnswerPolicy(
                route=self.name,
                fallback_message=fallback,
                fallback_outcome="answered",
                fallback_on_timeout=True,
                timeout_message=self._deps.timeout_message,
                include_context=True,
                system_prompt=cs.RETRIEVAL_SYSTEM_PROMPT or None,
                extra_rules=_build_company_retrieval_rules(evidence),
                evidence_transform=_transform_company_retrieval_payload,
                max_tokens=500,
                answer_validator=_is_safe_device_diagnostic_answer,
            ),
        )


def _snapshot_device_name(snapshot: dict[str, Any]) -> str:
    for container_name in ("request", "device"):
        container = snapshot.get(container_name)
        if not isinstance(container, dict):
            continue
        name = str(container.get("deviceName") or "").strip()
        if name:
            return name
    return ""


def _is_safe_device_diagnostic_answer(text: str) -> bool:
    lowered = (text or "").lower()
    return not any(
        marker in lowered
        for marker in (
            "system prompt",
            "developer prompt",
            "authorization:",
            "bearer ",
            "private key",
            "ssh password",
            "device_ssh_password",
            "<think>",
            "</think>",
        )
    )


@dataclass(frozen=True, slots=True)
class NotionPlaybookQARouteDeps:
    answer_composer: CompanyEvidenceAnswerComposer
    looks_like_question: Callable[[str], bool] = (
        looks_like_notion_playbook_question
    )
    looks_like_followup: Callable[[str, str], bool] = (
        looks_like_notion_playbook_followup
    )
    build_query: Callable[[str, str], str] = build_notion_playbook_query
    is_exfiltration_attempt: Callable[[str, str], bool] = (
        is_notion_playbook_exfiltration_attempt
    )
    select_references: NotionReferenceSelector = (
        _default_notion_reference_selector
    )
    sanitize_references: Callable[
        [list[dict[str, Any]] | None],
        list[dict[str, Any]],
    ] = sanitize_notion_playbook_references
    build_fallback: NotionFallbackBuilder = build_notion_playbook_fallback
    is_configured: Callable[[], bool] = _is_company_notion_configured
    timeout_message: str = (
        "문서 기반 답변 시간이 초과됐어. 잠시 후 다시 시도해줘"
    )


class NotionPlaybookQAAssistantRoute:
    name = _NOTION_PLAYBOOK_ROUTE

    def __init__(
        self,
        deps: NotionPlaybookQARouteDeps,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._deps = deps
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        context_text = _request_context_text(request)
        if _match_notion_playbook_route(
            request,
            looks_like_question=self._deps.looks_like_question,
            looks_like_followup=self._deps.looks_like_followup,
            context_text=context_text,
        ) is None:
            return None

        if self._deps.is_exfiltration_attempt(
            request.question,
            context_text,
        ):
            return _result(
                route=self.name,
                outcome="denied",
                body=build_notion_document_security_refusal(),
                fallback_reason="security_refusal",
            )

        evidence: dict[str, Any] = {
            "route": self.name,
            "source": "notion",
            "request": {"question": request.question},
        }
        query = self._deps.build_query(request.question, context_text)
        if query and query != request.question:
            evidence["request"]["contextualQuestion"] = query

        try:
            references = self._deps.select_references(
                query or request.question,
                evidence,
            )
            if not references:
                configured = self._deps.is_configured()
                return _result(
                    route=self.name,
                    outcome="no_evidence",
                    body=(
                        "관련 운영 문서를 찾지 못했어. "
                        "증상이나 키워드를 조금 더 구체적으로 말해줘"
                    ),
                    fallback_reason=(
                        "no_references"
                        if configured
                        else "notion_not_configured"
                    ),
                )
            sanitized = self._deps.sanitize_references(references)
            evidence["notionPlaybooks"] = sanitized
            evidence["notionReferences"] = sanitized
            fallback = slack_mrkdwn_to_commonmark(
                self._deps.build_fallback(request.question, sanitized)
            ).strip()
            sources = tuple(
                source
                for source in (
                    _safe_notion_source(reference)
                    for reference in references[:3]
                )
                if source is not None
            )
        except TimeoutError:
            return _result(
                route=self.name,
                outcome="failed",
                body=self._deps.timeout_message,
                fallback_reason="retrieval_timeout",
            )
        except Exception as exc:
            self._logger.exception(
                "Notion playbook QA failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                route=self.name,
                outcome="failed",
                body="문서 기반 답변 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                fallback_reason="retrieval_error",
            )

        result = self._deps.answer_composer.compose(
            request,
            evidence=evidence,
            policy=CompanyEvidenceAnswerPolicy(
                route=self.name,
                fallback_message=fallback,
                fallback_outcome="answered",
                fallback_on_timeout=True,
                timeout_message=self._deps.timeout_message,
                include_context=self._deps.looks_like_followup(
                    request.question,
                    context_text,
                ),
                system_prompt=cs.RETRIEVAL_SYSTEM_PROMPT or None,
                extra_rules=_build_company_retrieval_rules(evidence),
                evidence_transform=_transform_company_retrieval_payload,
                answer_validator=lambda text: not (
                    needs_notion_document_security_refusal(
                        text,
                        "notion playbook qa",
                    )
                ),
            ),
            sources=sources,
        )
        if result.fallback_reason == "answer_validation_failed":
            return _result(
                route=self.name,
                outcome="denied",
                body=build_notion_document_security_refusal(),
                fallback_reason="unsafe_generated_answer",
            )
        if result.used_llm and not _notion_answer_preserves_fallback(
            result.messages[0].body,
            fallback,
            overview=_is_notion_doc_general_overview_question(
                request.question
            ),
        ):
            return _result(
                route=self.name,
                outcome="answered",
                body=fallback,
                sources=sources,
                fallback_reason="answer_contract_mismatch",
            )
        return result


@dataclass(frozen=True, slots=True)
class BarcodeEvidenceFreeformRouteDeps:
    recordings: RequestScopedRecordingsContext
    answer_composer: CompanyEvidenceAnswerComposer
    db_configured: Callable[[], bool]
    should_handle: Callable[[CompanyAssistantRequest], bool] = (
        _handle_request_by_default
    )
    build_system_prompt: FreeformSystemPromptBuilder = (
        _default_freeform_system_prompt
    )
    timeout_message: str = (
        "AI 답변 생성 시간이 초과됐어. 잠시 후 다시 시도해줘"
    )


class BarcodeEvidenceFreeformAssistantRoute:
    name = _BARCODE_EVIDENCE_FREEFORM_ROUTE

    def __init__(
        self,
        deps: BarcodeEvidenceFreeformRouteDeps,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._deps = deps
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        try:
            barcode = resolve_assistant_request_scope(request).barcode
            self._deps.recordings.validate_barcode(barcode)
        except AssistantRequestScopeMismatch as mismatch:
            return build_scope_mismatch_result(mismatch)
        except RecordingsContextBarcodeMismatch:
            return build_scope_mismatch_result(
                AssistantRequestScopeMismatch("barcode")
            )
        if not barcode or not request.question.strip():
            return None

        try:
            should_handle = self._deps.should_handle(request)
        except Exception as exc:
            # adapter 위임 정책 오류 시 DB를 읽지 않고 다음 안전 경로로 넘긴다.
            self._logger.warning(
                "Barcode freeform delegation policy failed "
                "request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            should_handle = False
        if not should_handle:
            return None

        context_text = _request_context_text(request)
        if is_prompt_exfiltration_attempt(request.question, context_text):
            return _result(
                route=self.name,
                outcome="denied",
                body=build_prompt_security_refusal(),
                fallback_reason="security_refusal",
            )

        evidence: dict[str, Any] = {
            "route": "llm_barcode_fallback",
            "source": "box_db.recordings",
            "request": {
                "barcode": barcode,
                "question": request.question,
            },
        }
        try:
            db_configured = self._deps.db_configured()
        except Exception as exc:
            self._logger.warning(
                "Barcode freeform DB policy failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            db_configured = False
        if not db_configured:
            evidence["warning"] = (
                "DB 접속 정보가 없어 recordings 컨텍스트를 넣지 못했어"
            )
        else:
            try:
                recordings_context = self._deps.recordings.get(
                    requested_barcode=barcode
                )
                self._deps.recordings.attach_to_evidence(
                    evidence,
                    recordings_context,
                )
            except Exception as exc:
                # 사용자·LLM 근거에는 예외 상세나 접속 정보를 넣지 않고 분류만 남긴다.
                self._logger.warning(
                    "Barcode freeform evidence failed request_id=%s error_type=%s",
                    request.request_id,
                    type(exc).__name__,
                )
                evidence["warning"] = (
                    "recordings 컨텍스트를 조회하지 못했어"
                )

        fallback, fallback_outcome = _build_barcode_evidence_fallback(
            evidence
        )
        return self._deps.answer_composer.compose(
            request,
            evidence=evidence,
            policy=CompanyEvidenceAnswerPolicy(
                route=self.name,
                fallback_message=fallback,
                fallback_outcome=fallback_outcome,
                fallback_on_timeout=True,
                timeout_message=self._deps.timeout_message,
                include_context=True,
                system_prompt=self._deps.build_system_prompt(
                    request,
                    context_text,
                ),
                extra_rules=_build_company_retrieval_rules(evidence),
                evidence_transform=_transform_company_retrieval_payload,
                answer_validator=_is_safe_barcode_freeform_answer,
            ),
        )


@dataclass(frozen=True, slots=True)
class CompanyReadOnlyKnowledgeRouteDeps:
    """채널 adapter가 정책·저장소 port만 주입하는 표준 지식 route 조립 입력이다."""

    load_diagnostic_snapshot: DiagnosticSnapshotLoader
    db_configured: Callable[[], bool]
    barcode_should_handle: Callable[[CompanyAssistantRequest], bool] = (
        _handle_request_by_default
    )
    build_barcode_system_prompt: FreeformSystemPromptBuilder = (
        _default_freeform_system_prompt
    )
    timeout_message: str = (
        "AI 답변 생성 시간이 초과됐어. 잠시 후 다시 시도해줘"
    )
    include_barcode_evidence: bool = True


def build_company_read_only_knowledge_routes(
    recordings: RequestScopedRecordingsContext,
    answer_composer: CompanyEvidenceAnswerComposer,
    deps: CompanyReadOnlyKnowledgeRouteDeps,
    *,
    logger: logging.Logger | None = None,
) -> tuple[
    DeviceDiagnosticFollowupAssistantRoute
    | NotionPlaybookQAAssistantRoute
    | BarcodeEvidenceFreeformAssistantRoute,
    ...,
]:
    """진단 snapshot→운영 문서→바코드 근거 순서를 한 곳에서 고정한다."""
    routes: list[
        DeviceDiagnosticFollowupAssistantRoute
        | NotionPlaybookQAAssistantRoute
        | BarcodeEvidenceFreeformAssistantRoute
    ] = [
        DeviceDiagnosticFollowupAssistantRoute(
            DeviceDiagnosticFollowupRouteDeps(
                answer_composer=answer_composer,
                load_snapshot=deps.load_diagnostic_snapshot,
                timeout_message=deps.timeout_message,
            ),
            logger=logger,
        ),
        NotionPlaybookQAAssistantRoute(
            NotionPlaybookQARouteDeps(
                answer_composer=answer_composer,
                timeout_message=deps.timeout_message,
            ),
            logger=logger,
        ),
    ]
    if deps.include_barcode_evidence:
        routes.append(
            BarcodeEvidenceFreeformAssistantRoute(
                BarcodeEvidenceFreeformRouteDeps(
                    recordings=recordings,
                    answer_composer=answer_composer,
                    db_configured=deps.db_configured,
                    should_handle=deps.barcode_should_handle,
                    build_system_prompt=(
                        deps.build_barcode_system_prompt
                    ),
                    timeout_message=deps.timeout_message,
                ),
                logger=logger,
            )
        )
    return tuple(routes)


def _build_barcode_evidence_fallback(
    evidence: dict[str, Any],
) -> tuple[str, AssistantOutcome]:
    warning = str(evidence.get("warning") or "").strip()
    if warning:
        return (
            "**바코드 근거 답변**\n"
            f"• 결과: {warning}\n"
            "• 안내: DB 연결 상태를 확인한 뒤 다시 질문해줘",
            "no_evidence",
        )

    summary = (
        evidence.get("recordingsSummary")
        if isinstance(evidence.get("recordingsSummary"), dict)
        else {}
    )
    count = int(summary.get("recordingCount") or 0)
    if count <= 0:
        return (
            "**바코드 근거 답변**\n"
            "• 결과: 조회된 녹화 기록이 없어\n"
            "• 안내: 근거가 없어 질문에 답을 단정할 수 없어",
            "no_evidence",
        )
    return (
        "**바코드 근거 답변**\n"
        f"• 조회 근거: 최근 녹화 기록 `{count}건`\n"
        "• 안내: AI 답변을 만들지 못해 조회 근거만 확인했어",
        "answered",
    )


def _is_safe_barcode_freeform_answer(text: str) -> bool:
    lowered = (text or "").lower()
    return not any(
        marker in lowered
        for marker in (
            "system prompt",
            "developer prompt",
            "internal prompt",
            "evidence(json)",
            "thread context",
            "authorization:",
            "bearer ",
            "<think>",
            "</think>",
        )
    )


__all__ = [
    "BarcodeEvidenceFreeformAssistantRoute",
    "BarcodeEvidenceFreeformRouteDeps",
    "CompanyReadOnlyKnowledgeRouteDeps",
    "DeviceDiagnosticFollowupAssistantRoute",
    "DeviceDiagnosticFollowupRouteDeps",
    "NotionPlaybookQAAssistantRoute",
    "NotionPlaybookQARouteDeps",
    "build_notion_playbook_fallback",
    "build_company_read_only_knowledge_routes",
    "build_notion_playbook_query",
    "is_notion_playbook_exfiltration_attempt",
    "sanitize_notion_playbook_references",
]
