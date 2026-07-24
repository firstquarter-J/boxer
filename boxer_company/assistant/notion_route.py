from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, Callable

from boxer import AnswerEngine
from boxer_company.assistant.answer_composer import (
    CompanyEvidenceAnswerComposer,
    CompanyEvidenceAnswerComposerDeps,
    CompanyEvidenceAnswerPolicy,
)
from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
    SourceReference,
)
from boxer_company.assistant.notion_answer_safety import (
    build_notion_document_security_refusal,
    needs_notion_document_security_refusal,
)
from boxer_company.notion_workspace_search import (
    CompanyNotionSearchResult,
    _extract_company_notion_search_query,
    _is_company_notion_search_allowed,
    _is_company_notion_search_configured,
    _load_company_notion_references,
    _looks_like_company_notion_search,
    _search_company_notion,
)
from boxer_company.retrieval_rules import (
    _build_company_retrieval_rules,
    _transform_company_retrieval_payload,
)


@dataclass(frozen=True, slots=True)
class CompanyNotionAssistantRouteDeps:
    answer_engine: AnswerEngine
    synthesis_enabled: bool
    provider_ready: Callable[[], bool]
    actor_allowed_for_llm: Callable[[str | None], bool]
    looks_like_search: Callable[[str], bool] = _looks_like_company_notion_search
    is_search_allowed: Callable[[str | None], bool] = _is_company_notion_search_allowed
    is_search_configured: Callable[[], bool] = _is_company_notion_search_configured
    extract_query: Callable[[str], str] = _extract_company_notion_search_query
    search: Callable[[str], list[CompanyNotionSearchResult]] = _search_company_notion
    load_references: Callable[
        [str, list[CompanyNotionSearchResult]],
        list[dict[str, Any]],
    ] = _load_company_notion_references


class CompanyNotionAssistantRoute:
    name = "company_notion"

    def __init__(
        self,
        deps: CompanyNotionAssistantRouteDeps,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._deps = deps
        self._logger = logger or logging.getLogger(__name__)
        self._composer = CompanyEvidenceAnswerComposer(
            CompanyEvidenceAnswerComposerDeps(
                answer_engine=deps.answer_engine,
                synthesis_enabled=deps.synthesis_enabled,
                provider_ready=deps.provider_ready,
                actor_allowed_for_llm=deps.actor_allowed_for_llm,
            ),
            logger=self._logger,
        )

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        if not self._deps.looks_like_search(request.question):
            return None

        if not self._deps.is_search_allowed(request.actor_id):
            return self._result(
                route="company_notion_search",
                outcome="denied",
                body="회사 Notion 검색은 아직 허용된 사용자만 쓸 수 있어",
                fallback_reason="actor_not_allowed",
            )
        if not self._deps.is_search_configured():
            return self._result(
                route="company_notion_search",
                outcome="failed",
                body="회사 Notion 검색 root 설정이 없어. Work Board 페이지 ID를 먼저 설정해줘",
                fallback_reason="not_configured",
            )

        notion_query = self._deps.extract_query(request.question)
        if not notion_query:
            return self._result(
                route="company_notion_search",
                outcome="needs_input",
                body="`회사 노션에서 커머스 찾아줘`처럼 검색할 키워드를 같이 말해줘",
                fallback_reason="missing_query",
            )

        try:
            notion_results = self._deps.search(notion_query)
            if not notion_results:
                safe_query = notion_query.replace("`", "'")
                return self._result(
                    route="company_notion_search",
                    outcome="no_evidence",
                    body=(
                        f"회사 Work Board에서 `{safe_query}` 제목의 문서를 찾지 못했어. "
                        "지금은 제목 기준 검색이라 다른 핵심 키워드로 다시 찾아줘"
                    ),
                    fallback_reason="no_search_results",
                )

            notion_references = self._deps.load_references(
                notion_query,
                notion_results,
            )
            sources = self._build_sources(
                notion_references,
                fallback_results=notion_results,
            )
            has_content_evidence = any(
                isinstance(reference, dict) and bool(reference.get("excerpts"))
                for reference in notion_references
            )
            if not has_content_evidence:
                safe_query = notion_query.replace("`", "'")
                return self._result(
                    route="company_notion_search",
                    outcome="no_evidence",
                    body=(
                        "**회사 Notion 검색**\n"
                        f"• 키워드: `{safe_query}`\n"
                        "_현재는 Work Board 범위의 제목 검색이야._"
                    ),
                    sources=sources,
                    fallback_reason="content_unavailable",
                )

            evidence_payload: dict[str, Any] = {
                "route": "company_notion_qa",
                "source": "notion.work_board",
                "request": {
                    "question": request.question,
                    "searchQuery": notion_query,
                },
                "companyNotionReferences": notion_references,
            }
            return self._answer_with_evidence(
                request,
                evidence_payload=evidence_payload,
                sources=sources,
            )
        except Exception as exc:
            # 예상 밖 retrieval 오류는 사용자 응답과 분리해 내부 traceback을 남긴다.
            self._logger.exception(
                "Company Notion assistant route failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return self._result(
                route="company_notion_search",
                outcome="failed",
                body="회사 Notion을 조회하는 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                fallback_reason="retrieval_error",
            )

    def _answer_with_evidence(
        self,
        request: CompanyAssistantRequest,
        *,
        evidence_payload: dict[str, Any],
        sources: tuple[SourceReference, ...],
    ) -> CompanyAssistantResult:
        fallback_body = (
            "**회사 Notion 문서 답변**\n"
            "관련 문서는 찾았지만 지금은 답변을 만들지 못했어. 아래 원문을 확인해줘"
        )
        # 전사 Work Board 답변은 Slack/Web 과거 대화 대신 조회한 문서 발췌만 근거로 쓴다.
        result = self._composer.compose(
            request,
            evidence=evidence_payload,
            policy=CompanyEvidenceAnswerPolicy(
                route="company_notion_qa",
                fallback_message=fallback_body,
                fallback_outcome="no_evidence",
                fallback_on_timeout=True,
                include_context=False,
                extra_rules=_build_company_retrieval_rules(evidence_payload),
                evidence_transform=_transform_company_retrieval_payload,
                max_tokens=600,
                answer_validator=lambda text: not (
                    needs_notion_document_security_refusal(
                        text,
                        "company_notion_qa",
                    )
                ),
            ),
            sources=sources,
        )
        if result.fallback_reason == "answer_validation_failed":
            # 생성 결과가 내부 지시문이나 과도한 문서 원문을 포함하면
            # 근거 링크도 덧붙이지 않고 동일한 보안 거부문으로 종료한다.
            return self._result(
                route="company_notion_qa",
                outcome="denied",
                body=build_notion_document_security_refusal(),
                fallback_reason="unsafe_generated_answer",
            )
        return result

    @staticmethod
    def _build_sources(
        references: list[dict[str, Any]],
        *,
        fallback_results: list[CompanyNotionSearchResult],
    ) -> tuple[SourceReference, ...]:
        candidates = [
            (
                str(reference.get("title") or "").strip(),
                str(reference.get("url") or "").strip(),
            )
            for reference in references
            if isinstance(reference, dict)
        ]
        if not candidates:
            candidates = [
                (str(result.title or "").strip(), str(result.url or "").strip())
                for result in fallback_results
            ]

        sources: list[SourceReference] = []
        seen_uris: set[str] = set()
        for title, uri in candidates:
            if (
                not title
                or uri in seen_uris
                or not uri.startswith(
                    ("https://www.notion.so/", "https://app.notion.com/")
                )
            ):
                continue
            seen_uris.add(uri)
            sources.append(
                SourceReference(
                    source_id=uri,
                    title=title,
                    uri=uri,
                )
            )
            if len(sources) >= 3:
                break
        return tuple(sources)

    @staticmethod
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


__all__ = [
    "CompanyNotionAssistantRoute",
    "CompanyNotionAssistantRouteDeps",
]
