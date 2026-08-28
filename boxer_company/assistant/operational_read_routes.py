from __future__ import annotations

# 주간 read matcher와 날짜 해석은 transport와 공유하는 순수 정본을 쓴다.
from boxer_company.read_routing import (
    AssistantRequestScopeMismatch,
    WEEKLY_RECORDINGS_SUMMARY_ROUTE,
    _extract_log_date_with_presence,
    _is_weekly_recordings_report_request,
    resolve_assistant_request_scope,
)

from datetime import date
import logging

import pymysql

from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.scope_guard import (
    build_scope_mismatch_result,
)
from boxer_company.weekly_recordings_report import (
    _build_weekly_recordings_report_summary,
    _coerce_weekly_recordings_report_now,
    _format_weekly_recordings_report,
    _resolve_weekly_recordings_report_question_target_date,
)


class WeeklyRecordingsSummaryAssistantRoute:
    """주간 recordings DB 집계를 채널 중립 CommonMark 응답으로 변환한다."""

    name = WEEKLY_RECORDINGS_SUMMARY_ROUTE

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        try:
            barcode = resolve_assistant_request_scope(request).barcode
        except AssistantRequestScopeMismatch as mismatch:
            return build_scope_mismatch_result(mismatch)

        try:
            target_date = _extract_weekly_target_date(request.question)
        except ValueError as exc:
            if not _is_weekly_recordings_report_request(
                request.question,
                barcode=barcode,
            ):
                return None
            return _result(
                outcome="needs_input",
                body=f"주간 영상 현황 요청 형식 오류: {exc}",
                fallback_reason="invalid_date",
            )

        if not _is_weekly_recordings_report_request(
            request.question,
            barcode=barcode,
        ):
            return None

        try:
            # 기존 Slack helper처럼 한 시각을 집계와 formatter에 함께 넘겨
            # 주간 경계와 표시 시각이 요청 도중 갈리지 않게 한다.
            local_now = _coerce_weekly_recordings_report_now()
            summary = _build_weekly_recordings_report_summary(
                target_date=target_date,
                now=local_now,
            )
            body = slack_mrkdwn_to_commonmark(
                _format_weekly_recordings_report(
                    summary,
                    now=local_now,
                )
            )
        except (pymysql.MySQLError, RuntimeError) as exc:
            self._logger.warning(
                "Weekly recordings summary dependency failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                outcome="failed",
                body=(
                    "주간 영상 현황 조회 중 오류가 발생했어. "
                    "DB 연결 정보와 네트워크 상태를 확인해줘"
                ),
                fallback_reason="dependency_error",
            )
        except Exception as exc:
            # 원문 예외는 사용자 응답에 싣지 않고 request id로만 운영 로그와 연결한다.
            self._logger.exception(
                "Weekly recordings summary failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                outcome="failed",
                body=(
                    "주간 영상 현황 조회 중 오류가 발생했어. "
                    "잠시 후 다시 시도해줘"
                ),
                fallback_reason="query_error",
            )

        has_recordings = int(summary.get("totalCount") or 0) > 0
        return _result(
            outcome="answered" if has_recordings else "no_evidence",
            body=body,
            fallback_reason=None if has_recordings else "recordings_not_found",
        )


def _extract_weekly_target_date(question: str) -> date | None:
    """실행 시각은 기존 weekly runtime의 주입 가능한 KST clock을 유지한다."""

    parsed_date, has_requested_date = _extract_log_date_with_presence(question)
    explicit_target_date = (
        date.fromisoformat(parsed_date) if has_requested_date else None
    )
    return _resolve_weekly_recordings_report_question_target_date(
        question,
        explicit_target_date=explicit_target_date,
    )


def _result(
    *,
    outcome: AssistantOutcome,
    body: str,
    fallback_reason: str | None = None,
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route=WEEKLY_RECORDINGS_SUMMARY_ROUTE,
        outcome=outcome,
        messages=(
            AssistantMessage(
                body=body,
                mention_actor=False,
                format="commonmark",
            ),
        ),
        fallback_reason=fallback_reason,
    )


__all__ = [
    "WeeklyRecordingsSummaryAssistantRoute",
]
