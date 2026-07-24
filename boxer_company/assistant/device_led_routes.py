from __future__ import annotations

from dataclasses import replace
import logging
import re
from typing import Any, Callable

from botocore.exceptions import BotoCoreError, ClientError

from boxer_company import settings as cs
from boxer_company.assistant.answer_composer import (
    CompanyEvidenceAnswerComposer,
    CompanyEvidenceAnswerPolicy,
)
from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
    SourceReference,
)
from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.assistant.dependency_errors import (
    build_dependency_failure_message,
)
from boxer_company.retrieval_rules import (
    _build_company_retrieval_rules,
    _transform_company_retrieval_payload,
)
from boxer_company.routers.barcode_log import (
    _extract_device_name_scope,
    _extract_log_date_with_presence,
)
from boxer_company.routers.device_led_log import (
    _analyze_device_led_log,
    _format_led_log_date_required_message,
    _is_device_led_log_analysis_request,
)
from boxer_company.routers.device_status_probe import (
    _build_led_pattern_help_evidence,
    _build_led_pattern_help_reply,
    _is_device_led_pattern_help_request,
)


S3ClientProvider = Callable[[], Any]
NotionReferenceLoader = Callable[..., list[dict[str, Any]]]
_LED_PATTERN_REQUIRED_BULLETS = (
    "• 결론:",
    "• 근거:",
    "• 참고 상태:",
    "• 안내:",
)


class DeviceLedLogAssistantRoute:
    """S3 LED 로그 분석을 채널 중립 결과로 바꾼다."""

    name = "device_led_log_analysis"

    def __init__(
        self,
        get_s3_client: S3ClientProvider,
        *,
        s3_enabled: bool,
        logger: logging.Logger | None = None,
    ) -> None:
        self._get_s3_client = get_s3_client
        self._s3_enabled = s3_enabled
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        metadata_device_name = str(
            request.metadata.get("device_name")
            or request.metadata.get("deviceName")
            or ""
        ).strip() or None
        question_device_name = _extract_device_name_scope(
            request.question
        )
        device_name = _resolve_device_name(request)
        if not _is_device_led_log_analysis_request(
            request.question,
            device_name=device_name,
        ):
            return None
        if (
            metadata_device_name
            and question_device_name
            and metadata_device_name != question_device_name
        ):
            return _result(
                route="device_scope_guard",
                outcome="denied",
                body=(
                    "요청 장비와 조회 컨텍스트가 일치하지 않아. "
                    "새 요청으로 다시 시도해줘"
                ),
                fallback_reason="device_scope_mismatch",
            )

        # 기존 adapter 순서대로 로그 요청을 먼저 종결해
        # 패턴 가이드가 겹치는 질문을 가로채지 않게 한다.
        if not self._s3_enabled:
            return _result(
                route=self.name,
                outcome="failed",
                body=(
                    "LED 로그 확인 기능이 꺼져 있어. "
                    ".env에서 S3_QUERY_ENABLED=true로 설정해줘"
                ),
                fallback_reason="s3_disabled",
            )

        try:
            log_date, has_requested_date = _extract_log_date_with_presence(
                request.question
            )
        except ValueError as exc:
            return _result(
                route=self.name,
                outcome="needs_input",
                body=f"LED 로그 확인 요청 형식 오류: {exc}",
                fallback_reason="invalid_date",
            )

        if not has_requested_date:
            return _result(
                route=self.name,
                outcome="needs_input",
                body=_format_led_log_date_required_message(device_name),
                fallback_reason="missing_date",
            )

        try:
            result_text, evidence_payload = _analyze_device_led_log(
                self._get_s3_client(),
                device_name,
                log_date,
            )
        except ValueError as exc:
            return _result(
                route=self.name,
                outcome="needs_input",
                body=f"LED 로그 확인 요청 형식 오류: {exc}",
                fallback_reason="invalid_request",
            )
        except (BotoCoreError, ClientError, RuntimeError) as exc:
            self._logger.warning(
                "Device LED log dependency failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                route=self.name,
                outcome="failed",
                body=build_dependency_failure_message(
                    "LED 로그 확인",
                    exc,
                ),
                fallback_reason="dependency_error",
            )
        except Exception as exc:
            # 예상 밖 분석 오류는 사용자 응답과 분리해 내부 traceback을 남긴다.
            self._logger.exception(
                "Device LED log analysis failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                route=self.name,
                outcome="failed",
                body="LED 로그 확인 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                fallback_reason="analysis_error",
            )

        log_found = not (
            isinstance(evidence_payload, dict)
            and evidence_payload.get("logFound") is False
        )
        return _result(
            route=self.name,
            outcome="answered" if log_found else "no_evidence",
            body=_to_commonmark(result_text),
            fallback_reason=None if log_found else "log_not_found",
        )


class DeviceLedPatternGuideAssistantRoute:
    """LED 상태표와 Notion 참고 근거를 composer로 답변한다."""

    name = "device_led_pattern_guide"

    def __init__(
        self,
        composer: CompanyEvidenceAnswerComposer,
        notion_reference_loader: NotionReferenceLoader,
        *,
        timeout_message: str = (
            "AI 답변 생성 시간이 초과됐어. 잠시 후 다시 시도해줘"
        ),
        logger: logging.Logger | None = None,
    ) -> None:
        self._composer = composer
        self._notion_reference_loader = notion_reference_loader
        self._timeout_message = timeout_message
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        question = request.question
        device_name = _resolve_device_name(request)

        # 두 matcher가 겹쳐도 S3 로그 분석이 항상 먼저 처리되도록
        # 가이드 경로 자체에서도 로그 질문을 제외한다.
        if _is_device_led_log_analysis_request(
            question,
            device_name=device_name,
        ):
            return None
        if not _is_device_led_pattern_help_request(question):
            return None

        fallback_text = _to_commonmark(_build_led_pattern_help_reply(question))
        evidence_payload = _build_led_pattern_help_evidence(question)
        notion_references = self._load_notion_references(
            request,
            evidence_payload=evidence_payload,
        )
        if notion_references:
            # 기존 합성 경로가 쓰던 두 key를 모두 유지해
            # retrieval transform과 후속 문서 링크 선택이 같은 근거를 보게 한다.
            evidence_payload["notionPlaybooks"] = notion_references
            evidence_payload["notionReferences"] = notion_references

        fallback_with_references = _append_notion_reference_section(
            fallback_text,
            notion_references,
        )
        sources = _build_sources(notion_references)
        result = self._composer.compose(
            request,
            evidence=evidence_payload,
            policy=CompanyEvidenceAnswerPolicy(
                route=self.name,
                fallback_message=fallback_with_references,
                timeout_message=self._timeout_message,
                system_prompt=cs.RETRIEVAL_SYSTEM_PROMPT or None,
                extra_rules=_build_company_retrieval_rules(evidence_payload),
                evidence_transform=_transform_company_retrieval_payload,
                max_tokens=280,
                answer_validator=_is_valid_led_pattern_answer,
            ),
            sources=sources,
        )
        return _normalize_result_messages(
            result,
            notion_references=notion_references,
        )

    def _load_notion_references(
        self,
        request: CompanyAssistantRequest,
        *,
        evidence_payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        try:
            references = self._notion_reference_loader(
                request.question,
                evidence_payload=evidence_payload,
                max_results=2,
            )
        except Exception as exc:
            # 문서 조회 실패가 로컬 LED 상태표 fallback까지 막지 않게 격리한다.
            self._logger.exception(
                "Device LED guide references failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            evidence_payload["referenceWarning"] = "notion_reference_error"
            return []
        return [
            reference
            for reference in references
            if isinstance(reference, dict)
        ][:2]


def _resolve_device_name(request: CompanyAssistantRequest) -> str:
    metadata_name = (
        request.metadata.get("device_name")
        or request.metadata.get("deviceName")
    )
    return str(
        metadata_name or _extract_device_name_scope(request.question) or ""
    ).strip()


def _is_valid_led_pattern_answer(answer_text: str) -> bool:
    normalized = (answer_text or "").strip()
    has_title = normalized.startswith(
        ("*LED 증상 안내*", "**LED 증상 안내**")
    )
    return has_title and all(
        bullet in normalized
        for bullet in _LED_PATTERN_REQUIRED_BULLETS
    )


def _append_notion_reference_section(
    text: str,
    references: list[dict[str, Any]],
) -> str:
    normalized = (text or "").strip()
    if not references or "참고 플레이북" in normalized:
        return normalized

    lines = ["**참고 플레이북**"]
    for reference in references[:2]:
        title = str(reference.get("title") or "").strip() or "제목 미상"
        url = str(reference.get("url") or "").strip()
        matched_keywords = [
            str(keyword).strip()
            for keyword in (reference.get("matchedKeywords") or [])
            if str(keyword).strip()
        ]
        # 제목의 링크 문법은 제거하고 검증된 Notion URL만 링크 대상으로 쓴다.
        safe_title = _plain_reference_title(title)
        label = (
            f"[{safe_title}]({url})"
            if _is_notion_url(url)
            else safe_title
        )
        if matched_keywords:
            label += f" (`{', '.join(matched_keywords[:3])}`)"
        lines.append(f"- {label}")
    return f"{normalized}\n\n" + "\n".join(lines)


def _plain_reference_title(title: str) -> str:
    without_urls = re.sub(
        r"https?://[^\s\])]+",
        "",
        title or "",
        flags=re.IGNORECASE,
    )
    return (
        " ".join(without_urls.split())
        .replace("\\", "/")
        .replace("[", "［")
        .replace("]", "］")
        .replace("(", "（")
        .replace(")", "）")
    )


def _build_sources(
    references: list[dict[str, Any]],
) -> tuple[SourceReference, ...]:
    sources: list[SourceReference] = []
    seen_uris: set[str] = set()
    for reference in references:
        title = str(reference.get("title") or "").strip()
        uri = str(reference.get("url") or "").strip()
        if not title or not _is_notion_url(uri) or uri in seen_uris:
            continue
        seen_uris.add(uri)
        sources.append(
            SourceReference(
                source_id=str(reference.get("pageId") or uri),
                title=title,
                uri=uri,
            )
        )
    return tuple(sources)


def _is_notion_url(value: str) -> bool:
    return value.startswith(
        ("https://www.notion.so/", "https://app.notion.com/")
    )


def _normalize_result_messages(
    result: CompanyAssistantResult,
    *,
    notion_references: list[dict[str, Any]],
) -> CompanyAssistantResult:
    messages = tuple(
        replace(
            message,
            body=_append_notion_reference_section(
                _to_commonmark(message.body),
                notion_references,
            ),
        )
        for message in result.messages
    )
    return replace(result, messages=messages)


def _to_commonmark(text: str) -> str:
    return slack_mrkdwn_to_commonmark(text)


def _result(
    *,
    route: str,
    outcome: AssistantOutcome,
    body: str,
    fallback_reason: str | None = None,
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route=route,
        outcome=outcome,
        messages=(AssistantMessage(body=body),),
        fallback_reason=fallback_reason,
    )


__all__ = [
    "DeviceLedLogAssistantRoute",
    "DeviceLedPatternGuideAssistantRoute",
    "NotionReferenceLoader",
    "S3ClientProvider",
]
