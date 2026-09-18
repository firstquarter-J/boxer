from __future__ import annotations

import logging
from datetime import date

import pymysql

from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.recordings_report_format import (
    format_recordings_report_sections,
)
from boxer_company.assistant.scope_guard import (
    build_scope_mismatch_result,
)

# 주간 read matcher와 날짜 해석은 transport와 공유하는 순수 정본을 쓴다.
from boxer_company.read_routing import (
    WEEKLY_RECORDINGS_SUMMARY_ROUTE,
    AssistantRequestScopeMismatch,
    _extract_log_date_with_presence,
    _extract_recordings_report_date_range,
    _is_weekly_recordings_report_request,
    resolve_assistant_request_scope,
)
from boxer_company.recordings_report_options import (
    RecordingsReportOptionsError,
    parse_recordings_report_options,
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
            date_range = _extract_recordings_report_date_range(request.question)
            target_date = None if date_range else _extract_weekly_target_date(request.question)
        except ValueError as exc:
            if not _is_weekly_recordings_report_request(
                request.question,
                barcode=barcode,
            ):
                return None
            return _result(
                outcome="needs_input",
                body=f"녹화·신규 바코드 요약 요청 형식 오류: {exc}",
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
                include_new_barcodes=True,
                options=parse_recordings_report_options(request.question),
                **({"start_date": date_range[0], "end_date": date_range[1]} if date_range else {}),
            )
            if "newBarcodes" in summary:
                bodies = format_recordings_report_sections(summary, now=local_now)
            else:
                # 구 summary를 주입하는 호출도 기존 응답 형식으로 처리한다.
                bodies = (slack_mrkdwn_to_commonmark(
                    _format_weekly_recordings_report(summary, now=local_now)
                ),)
        except RecordingsReportOptionsError as exc:
            # 잘못된 조건이나 모호한 병원명을 전체 병원·기본 조건 조회로 바꾸지 않는다.
            return _result(
                outcome="needs_input", body=f"녹화·신규 바코드 조회 조건을 확인해줘: {exc}",
                fallback_reason="invalid_report_options",
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
                    "녹화·신규 바코드 요약 조회 중 오류가 발생했어. "
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
                    "녹화·신규 바코드 요약 조회 중 오류가 발생했어. "
                    "잠시 후 다시 시도해줘"
                ),
                fallback_reason="query_error",
            )

        # 이번 주 전체가 0건이어도 전주 대비 진료실 급감은 조회된 근거가 있는 답변이다.
        has_evidence = (
            int(summary.get("totalCount") or 0) > 0
            or int(summary.get("previousTotalCount") or 0) > 0
            or bool(summary.get("roomDropRows"))
        )
        return _result(
            outcome="answered" if has_evidence else "no_evidence",
            bodies=bodies,
            fallback_reason=None if has_evidence else "recordings_not_found",
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
    body: str = "",
    bodies: tuple[str, ...] = (),
    fallback_reason: str | None = None,
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route=WEEKLY_RECORDINGS_SUMMARY_ROUTE,
        outcome=outcome,
        messages=tuple(
            AssistantMessage(
                body=message_body,
                mention_actor=False,
                format="commonmark",
            )
            for message_body in (bodies or (body,))
        ),
        fallback_reason=fallback_reason,
    )


__all__ = [
    "WeeklyRecordingsSummaryAssistantRoute",
]
