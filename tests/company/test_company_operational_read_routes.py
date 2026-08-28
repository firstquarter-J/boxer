import unittest
from datetime import datetime
from unittest.mock import Mock, patch

import pymysql

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.operational_read_routes import (
    WeeklyRecordingsSummaryAssistantRoute,
)
from boxer_company.read_routing import (
    WEEKLY_RECORDINGS_SUMMARY_ROUTE,
    match_weekly_recordings_summary_route,
)


def _request(
    question: str,
    *,
    metadata: dict[str, object] | None = None,
) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="req-weekly-1",
        tenant_id="company",
        actor_id="U123",
        channel="test",
        conversation_id="C123",
        question=question,
        locale="ko-KR",
        metadata=metadata or {},
    )


class WeeklyRecordingsSummaryRouteMatcherTests(unittest.TestCase):
    def test_matches_weekly_summary_without_external_lookup(self) -> None:
        self.assertEqual(
            match_weekly_recordings_summary_route(
                _request("2026-03-23 주간 recordings 요약")
            ),
            WEEKLY_RECORDINGS_SUMMARY_ROUTE,
        )

    def test_rejects_list_and_barcode_scoped_requests(self) -> None:
        self.assertIsNone(
            match_weekly_recordings_summary_route(
                _request("지난주 초음파 영상 목록")
            )
        )
        self.assertIsNone(
            match_weekly_recordings_summary_route(
                _request(
                    "지난주 초음파 영상 현황",
                    metadata={"barcode": "12345678910"},
                )
            )
        )

    def test_keeps_invalid_date_in_route_for_safe_input_error(self) -> None:
        self.assertEqual(
            match_weekly_recordings_summary_route(
                _request("2026-13-40 주간 초음파 영상 현황")
            ),
            WEEKLY_RECORDINGS_SUMMARY_ROUTE,
        )


class WeeklyRecordingsSummaryAssistantRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = Mock()
        self.route = WeeklyRecordingsSummaryAssistantRoute(
            logger=self.logger
        )

    def test_returns_commonmark_summary_from_shared_db_report(self) -> None:
        local_now = datetime(2026, 4, 3, 13, 55, 0)
        summary = {
            "weekStartDate": "2026-03-23",
            "weekEndDate": "2026-03-29",
            "totalCount": 42,
        }
        with (
            patch(
                "boxer_company.assistant.operational_read_routes."
                "_coerce_weekly_recordings_report_now",
                return_value=local_now,
            ),
            patch(
                "boxer_company.assistant.operational_read_routes._build_weekly_recordings_report_summary",
                return_value=summary,
            ) as summary_builder,
            patch(
                "boxer_company.assistant.operational_read_routes._format_weekly_recordings_report",
                return_value="*주간 초음파 촬영 요약*\n• 전체 row: `42개`",
            ) as formatter,
        ):
            result = self.route.handle(
                _request("2026-03-23 주간 recordings 요약")
            )

        self.assertIsNotNone(result)
        assert result is not None
        summary_builder.assert_called_once()
        self.assertEqual(
            summary_builder.call_args.kwargs["target_date"].isoformat(),
            "2026-03-23",
        )
        self.assertEqual(
            summary_builder.call_args.kwargs["now"],
            local_now,
        )
        formatter.assert_called_once_with(summary, now=local_now)
        self.assertEqual(result.route, WEEKLY_RECORDINGS_SUMMARY_ROUTE)
        self.assertEqual(result.outcome, "answered")
        self.assertEqual(
            result.messages[0].body,
            "**주간 초음파 촬영 요약**\n• 전체 row: `42개`",
        )
        self.assertEqual(result.messages[0].format, "commonmark")
        self.assertFalse(result.messages[0].mention_actor)
        self.assertFalse(result.used_llm)
        self.assertEqual(result.sources, ())

    def test_returns_no_evidence_for_empty_week(self) -> None:
        with (
            patch(
                "boxer_company.assistant.operational_read_routes._build_weekly_recordings_report_summary",
                return_value={"totalCount": 0},
            ),
            patch(
                "boxer_company.assistant.operational_read_routes._format_weekly_recordings_report",
                return_value="• 결과: 해당 주간 recordings row가 없어",
            ),
        ):
            result = self.route.handle(
                _request("지난주 초음파 영상 현황")
            )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "no_evidence")
        self.assertEqual(result.fallback_reason, "recordings_not_found")

    def test_current_week_question_passes_kst_today_as_target(self) -> None:
        with (
            patch(
                "boxer_company.weekly_recordings_report."
                "_coerce_weekly_recordings_report_now",
                return_value=datetime(2026, 4, 3, 13, 0, 0),
            ),
            patch(
                "boxer_company.assistant.operational_read_routes."
                "_build_weekly_recordings_report_summary",
                return_value={"totalCount": 1},
            ) as summary_builder,
            patch(
                "boxer_company.assistant.operational_read_routes."
                "_format_weekly_recordings_report",
                return_value="이번 주 1개",
            ),
        ):
            result = self.route.handle(_request("이번 주 초음파 영상 현황"))

        self.assertIsNotNone(result)
        self.assertEqual(
            summary_builder.call_args.kwargs["target_date"],
            datetime(2026, 4, 3).date(),
        )

    def test_returns_safe_invalid_date_without_db_lookup(self) -> None:
        with patch(
            "boxer_company.assistant.operational_read_routes._build_weekly_recordings_report_summary"
        ) as summary_builder:
            result = self.route.handle(
                _request("2026-13-40 주간 초음파 영상 현황")
            )

        self.assertIsNotNone(result)
        assert result is not None
        summary_builder.assert_not_called()
        self.assertEqual(result.outcome, "needs_input")
        self.assertEqual(result.fallback_reason, "invalid_date")
        self.assertIn("날짜 형식을 확인해줘", result.messages[0].body)

    def test_returns_safe_dependency_error_without_raw_db_message(self) -> None:
        with patch(
            "boxer_company.assistant.operational_read_routes._build_weekly_recordings_report_summary",
            side_effect=pymysql.MySQLError("password=do-not-expose"),
        ):
            result = self.route.handle(
                _request("지난주 초음파 영상 현황")
            )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "failed")
        self.assertEqual(result.fallback_reason, "dependency_error")
        self.assertNotIn("do-not-expose", result.messages[0].body)
        self.logger.warning.assert_called_once()

    def test_returns_safe_retry_message_for_unexpected_error(self) -> None:
        with patch(
            "boxer_company.assistant.operational_read_routes._build_weekly_recordings_report_summary",
            side_effect=TypeError("private-internal-detail"),
        ):
            result = self.route.handle(
                _request("지난주 초음파 영상 현황")
            )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "failed")
        self.assertEqual(result.fallback_reason, "query_error")
        self.assertNotIn("private-internal-detail", result.messages[0].body)
        self.logger.exception.assert_called_once()


if __name__ == "__main__":
    unittest.main()
