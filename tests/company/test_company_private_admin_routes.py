import ast
from pathlib import Path
import unittest
from unittest.mock import Mock

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.private_admin_routes import (
    ADMIN_READONLY_SQL_ROUTE,
    ADMIN_REQUEST_LOG_ROUTE,
    ADMIN_S3_DEVICE_LOG_ROUTE,
    ADMIN_S3_ULTRASOUND_ROUTE,
    APP_USER_BABY_ANALYSIS_ROUTE,
    APP_USER_PROFILE_ROUTE,
    BARCODE_PINK_CLASSIFICATION_ROUTE,
    BARCODE_VALIDATION_STATUS_ROUTE,
    RECORDING_STREAMING_RESTORE_ROUTE,
    AdminReadonlySqlAssistantRoute,
    AdminRequestLogAssistantRoute,
    AdminS3DeviceLogAssistantRoute,
    AdminS3UltrasoundAssistantRoute,
    AppUserBabySelectionAssistantRoute,
    AppUserProfileAssistantRoute,
    BarcodePinkClassificationAssistantRoute,
    BarcodeValidationStatusAssistantRoute,
    PrivateOperationsRouteDeps,
    RecordingStreamingRestoreAssistantRoute,
    build_private_operations_routes,
    match_private_operations_route,
)


BARCODE = "12345678910"


def _request(
    question: str,
    *,
    route_group: str | None = "operations",
    actor_id: str | None = "U123",
    metadata: dict[str, object] | None = None,
) -> CompanyAssistantRequest:
    actual_metadata: dict[str, object] = dict(metadata or {})
    if route_group is not None:
        actual_metadata["route_group"] = route_group
    return CompanyAssistantRequest(
        request_id="req-operations-1",
        tenant_id="company",
        actor_id=actor_id,
        channel="test",
        conversation_id="C123",
        question=question,
        locale="ko-KR",
        metadata=actual_metadata,
    )


def _assert_private_result(
    testcase: unittest.TestCase,
    result,
    *,
    route: str,
    delivery_scope: str,
) -> None:
    testcase.assertIsNotNone(result)
    assert result is not None
    testcase.assertEqual(result.route, route)
    testcase.assertEqual(result.messages[0].delivery_scope, delivery_scope)
    testcase.assertEqual(result.messages[0].format, "commonmark")
    testcase.assertEqual(result.sources, ())
    testcase.assertFalse(result.used_llm)
    testcase.assertIsNone(result.suggested_action)
    testcase.assertIsNone(result.async_job)


class PrivateOperationsMatcherTests(unittest.TestCase):
    def test_matches_every_operations_route_without_external_execution(self) -> None:
        cases = (
            (
                f"{BARCODE} 유저 조회 한 명만 나오는 원인 분석",
                APP_USER_BABY_ANALYSIS_ROUTE,
            ),
            (f"{BARCODE} 유저 조회", APP_USER_PROFILE_ROUTE),
            (
                f"{BARCODE} 2024년 4월 영상 복원",
                RECORDING_STREAMING_RESTORE_ROUTE,
            ),
            (
                f"{BARCODE} 핑크 바코드로 분류 안 된 이유",
                BARCODE_PINK_CLASSIFICATION_ROUTE,
            ),
            (
                f"{BARCODE} 유효성 검사에 걸리는 바코드야?",
                BARCODE_VALIDATION_STATUS_ROUTE,
            ),
            (f"s3 영상 {BARCODE}", ADMIN_S3_ULTRASOUND_ROUTE),
            (
                "s3 로그 MB2-C00419 2026-03-04",
                ADMIN_S3_DEVICE_LOG_ROUTE,
            ),
            (
                "db 조회 select seq from recordings limit 1",
                ADMIN_READONLY_SQL_ROUTE,
            ),
            ("요청 로그 오늘 최근 5", ADMIN_REQUEST_LOG_ROUTE),
        )

        for question, expected in cases:
            with self.subTest(route=expected):
                self.assertEqual(
                    match_private_operations_route(_request(question)),
                    expected,
                )

    def test_requires_exact_operations_route_group(self) -> None:
        question = f"{BARCODE} 유저 조회"
        self.assertIsNone(
            match_private_operations_route(
                _request(question, route_group=None)
            )
        )
        self.assertIsNone(
            match_private_operations_route(
                _request(question, route_group="barcode")
            )
        )

    def test_scope_mismatch_does_not_return_a_remote_route(self) -> None:
        self.assertIsNone(
            match_private_operations_route(
                _request(
                    f"{BARCODE} 유저 조회",
                    metadata={"barcode": "99999999999"},
                )
            )
        )

    def test_malformed_s3_intent_stays_in_safe_specific_route(self) -> None:
        self.assertEqual(
            match_private_operations_route(_request("s3 영상")),
            ADMIN_S3_ULTRASOUND_ROUTE,
        )
        self.assertEqual(
            match_private_operations_route(_request("s3 로그 MB2-C00419")),
            ADMIN_S3_DEVICE_LOG_ROUTE,
        )

    def test_module_has_no_slack_import(self) -> None:
        module_path = (
            Path(__file__).resolve().parents[2]
            / "boxer_company"
            / "assistant"
            / "private_admin_routes.py"
        )
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        imported_modules = {
            alias.name
            for node in ast.walk(tree)
            if isinstance(node, ast.Import)
            for alias in node.names
        }
        imported_modules.update(
            str(node.module or "")
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom)
        )
        self.assertFalse(
            any("slack" in module.lower() for module in imported_modules)
        )


class PrivateOperationsReadRouteTests(unittest.TestCase):
    def test_app_user_routes_are_requester_only_and_commonmark(self) -> None:
        profile_query = Mock(
            return_value=(
                "*바코드 조회 결과*\n"
                "• `userPhoneNumber`: `010-0000-0000`"
            )
        )
        profile_result = AppUserProfileAssistantRoute(profile_query).handle(
            _request(f"{BARCODE} 유저 조회")
        )
        _assert_private_result(
            self,
            profile_result,
            route=APP_USER_PROFILE_ROUTE,
            delivery_scope="requester",
        )
        assert profile_result is not None
        self.assertEqual(profile_result.outcome, "answered")
        self.assertIn("**바코드 조회 결과**", profile_result.messages[0].body)
        profile_query.assert_called_once_with(BARCODE)

        baby_query = Mock(return_value="Lambda 조회 결과 원인을 확인했어")
        baby_result = AppUserBabySelectionAssistantRoute(baby_query).handle(
            _request(f"{BARCODE} 유저 조회 한 명만 나오는 원인 분석")
        )
        _assert_private_result(
            self,
            baby_result,
            route=APP_USER_BABY_ANALYSIS_ROUTE,
            delivery_scope="requester",
        )
        baby_query.assert_called_once_with(BARCODE)

    def test_app_user_no_evidence_is_requester_only(self) -> None:
        result = AppUserProfileAssistantRoute(
            Mock(return_value=f"바코드 {BARCODE}로 조회된 유저가 없어")
        ).handle(_request(f"{BARCODE} 유저 조회"))

        _assert_private_result(
            self,
            result,
            route=APP_USER_PROFILE_ROUTE,
            delivery_scope="requester",
        )
        assert result is not None
        self.assertEqual(result.outcome, "no_evidence")

    def test_barcode_validation_routes_use_conversation_delivery(self) -> None:
        validation_query = Mock(return_value="*바코드 유효성 검사 확인*\n• 결론: 통과")
        validation_result = BarcodeValidationStatusAssistantRoute(
            validation_query
        ).handle(_request(f"{BARCODE} 유효성 검사에 걸려?"))
        _assert_private_result(
            self,
            validation_result,
            route=BARCODE_VALIDATION_STATUS_ROUTE,
            delivery_scope="conversation",
        )
        validation_query.assert_called_once_with(BARCODE)

        pink_query = Mock(return_value="*핑크바코드 분류 확인*\n• 결론: 미분류")
        pink_result = BarcodePinkClassificationAssistantRoute(
            pink_query
        ).handle(_request(f"{BARCODE} 핑크 바코드 분류 안 된 이유"))
        _assert_private_result(
            self,
            pink_result,
            route=BARCODE_PINK_CLASSIFICATION_ROUTE,
            delivery_scope="conversation",
        )
        pink_query.assert_called_once_with(BARCODE)

    def test_s3_queries_build_client_lazily_and_use_requester_delivery(self) -> None:
        s3_client = object()
        client_factory = Mock(return_value=s3_client)
        ultrasound_query = Mock(return_value="*S3 초음파 객체 조회 결과*\n• 영상: 1개")
        ultrasound_route = AdminS3UltrasoundAssistantRoute(
            s3_client_factory=client_factory,
            query=ultrasound_query,
        )

        self.assertIsNone(
            ultrasound_route.handle(
                _request(f"s3 영상 {BARCODE}", route_group="barcode")
            )
        )
        client_factory.assert_not_called()

        ultrasound_result = ultrasound_route.handle(
            _request(f"s3 영상 {BARCODE}")
        )
        _assert_private_result(
            self,
            ultrasound_result,
            route=ADMIN_S3_ULTRASOUND_ROUTE,
            delivery_scope="requester",
        )
        ultrasound_query.assert_called_once_with(s3_client, BARCODE)

        log_query = Mock(return_value="*S3 로그 조회 결과*\n```text\nok\n```")
        log_result = AdminS3DeviceLogAssistantRoute(
            s3_client_factory=client_factory,
            query=log_query,
        ).handle(_request("s3 로그 MB2-C00419 2026-03-04"))
        _assert_private_result(
            self,
            log_result,
            route=ADMIN_S3_DEVICE_LOG_ROUTE,
            delivery_scope="requester",
        )
        log_query.assert_called_once_with(
            s3_client,
            "MB2-C00419",
            "2026-03-04",
        )

    def test_malformed_s3_does_not_build_client(self) -> None:
        client_factory = Mock()
        result = AdminS3UltrasoundAssistantRoute(
            s3_client_factory=client_factory,
        ).handle(_request("s3 영상"))

        _assert_private_result(
            self,
            result,
            route=ADMIN_S3_ULTRASOUND_ROUTE,
            delivery_scope="requester",
        )
        assert result is not None
        self.assertEqual(result.outcome, "needs_input")
        client_factory.assert_not_called()

    def test_readonly_sql_validates_before_query_and_is_requester_only(self) -> None:
        validate = Mock(return_value="select seq from recordings limit 1")
        query = Mock(return_value={"rows": [{"seq": 7}], "rowcount": 1})
        formatter = Mock(return_value="*DB 조회 결과*\n```json\n[{\"seq\": 7}]\n```")
        result = AdminReadonlySqlAssistantRoute(
            validate=validate,
            query=query,
            formatter=formatter,
        ).handle(_request("db 조회 select seq from recordings limit 1"))

        _assert_private_result(
            self,
            result,
            route=ADMIN_READONLY_SQL_ROUTE,
            delivery_scope="requester",
        )
        assert result is not None
        self.assertEqual(result.outcome, "answered")
        self.assertIn("**DB 조회 결과**", result.messages[0].body)
        validate.assert_called_once_with("select seq from recordings limit 1")
        query.assert_called_once_with("select seq from recordings limit 1")
        formatter.assert_called_once_with(
            {"rows": [{"seq": 7}], "rowcount": 1}
        )

    def test_readonly_sql_rejects_invalid_sql_without_raw_error(self) -> None:
        query = Mock()
        result = AdminReadonlySqlAssistantRoute(
            validate=Mock(side_effect=ValueError("password=do-not-expose")),
            query=query,
        ).handle(_request("db 조회 delete from users"))

        _assert_private_result(
            self,
            result,
            route=ADMIN_READONLY_SQL_ROUTE,
            delivery_scope="requester",
        )
        assert result is not None
        self.assertEqual(result.outcome, "needs_input")
        self.assertNotIn("do-not-expose", result.messages[0].body)
        query.assert_not_called()

    def test_request_log_is_queried_in_api_store_and_requester_only(self) -> None:
        query = Mock(return_value="*요청 로그 최근 조회 결과*\n• 전체 요청: `3건`")
        result = AdminRequestLogAssistantRoute(
            query,
            enabled=lambda: True,
        ).handle(_request("요청 로그 오늘 최근 5"))

        _assert_private_result(
            self,
            result,
            route=ADMIN_REQUEST_LOG_ROUTE,
            delivery_scope="requester",
        )
        assert result is not None
        self.assertEqual(result.outcome, "answered")
        query.assert_called_once()
        spec = query.call_args.args[0]
        self.assertEqual(spec.mode, "recent")
        self.assertEqual(spec.limit, 5)

    def test_request_log_feature_disabled_fails_closed(self) -> None:
        query = Mock()
        result = AdminRequestLogAssistantRoute(
            query,
            enabled=lambda: False,
        ).handle(_request("요청 통계 오늘"))

        _assert_private_result(
            self,
            result,
            route=ADMIN_REQUEST_LOG_ROUTE,
            delivery_scope="requester",
        )
        assert result is not None
        self.assertEqual(result.outcome, "denied")
        query.assert_not_called()


class PrivateOperationsMutationRouteTests(unittest.TestCase):
    def test_streaming_restore_calls_existing_domain_once_for_actor(self) -> None:
        restore = Mock(return_value="*스트리밍 종료 영상 복원 결과*\n• 결과: 복원 완료")
        result = RecordingStreamingRestoreAssistantRoute(
            restore,
            enabled=lambda: True,
        ).handle(_request(f"{BARCODE} 2024년 4월 영상 복원"))

        _assert_private_result(
            self,
            result,
            route=RECORDING_STREAMING_RESTORE_ROUTE,
            delivery_scope="requester",
        )
        assert result is not None
        self.assertEqual(result.outcome, "answered")
        restore.assert_called_once_with(
            BARCODE,
            f"{BARCODE} 2024년 4월 영상 복원",
            requester="U123",
            requester_name=None,
        )

    def test_streaming_restore_fails_closed_without_actor_or_feature(self) -> None:
        restore = Mock()
        missing_actor = RecordingStreamingRestoreAssistantRoute(
            restore,
            enabled=lambda: True,
        ).handle(
            _request(
                f"{BARCODE} 2024년 4월 영상 복원",
                actor_id=None,
            )
        )
        disabled = RecordingStreamingRestoreAssistantRoute(
            restore,
            enabled=lambda: False,
        ).handle(_request(f"{BARCODE} 2024년 4월 영상 복원"))

        assert missing_actor is not None
        assert disabled is not None
        self.assertEqual(missing_actor.outcome, "denied")
        self.assertEqual(missing_actor.fallback_reason, "missing_actor")
        self.assertEqual(disabled.outcome, "denied")
        self.assertEqual(disabled.fallback_reason, "feature_disabled")
        restore.assert_not_called()

    def test_streaming_restore_question_does_not_call_domain_function(self) -> None:
        restore = Mock()
        result = RecordingStreamingRestoreAssistantRoute(
            restore,
            enabled=lambda: True,
        ).handle(
            _request(
                f"{BARCODE} 2024년 4월 영상 복원 가능한지"
            )
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "needs_input")
        self.assertEqual(
            result.fallback_reason,
            "explicit_execution_required",
        )
        restore.assert_not_called()


class PrivateOperationsSafetyAndBuilderTests(unittest.TestCase):
    def test_dependency_error_does_not_expose_raw_error_or_create_sources(self) -> None:
        logger = Mock()
        result = AppUserProfileAssistantRoute(
            Mock(side_effect=RuntimeError("token=do-not-expose")),
            logger=logger,
        ).handle(_request(f"{BARCODE} 유저 조회"))

        _assert_private_result(
            self,
            result,
            route=APP_USER_PROFILE_ROUTE,
            delivery_scope="requester",
        )
        assert result is not None
        self.assertEqual(result.outcome, "failed")
        self.assertNotIn("do-not-expose", result.messages[0].body)
        logger.warning.assert_called_once()

    def test_scope_mismatch_fails_closed_before_query(self) -> None:
        query = Mock()
        result = AppUserProfileAssistantRoute(query).handle(
            _request(
                f"{BARCODE} 유저 조회",
                metadata={"barcode": "99999999999"},
            )
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.route, "barcode_scope_guard")
        self.assertEqual(result.outcome, "denied")
        query.assert_not_called()

    def test_builder_has_fixed_unique_route_order(self) -> None:
        deps = PrivateOperationsRouteDeps(
            streaming_restore_enabled=lambda: True,
        )
        routes = build_private_operations_routes(deps)

        self.assertEqual(
            tuple(route.name for route in routes),
            (
                APP_USER_BABY_ANALYSIS_ROUTE,
                APP_USER_PROFILE_ROUTE,
                RECORDING_STREAMING_RESTORE_ROUTE,
                BARCODE_PINK_CLASSIFICATION_ROUTE,
                BARCODE_VALIDATION_STATUS_ROUTE,
                ADMIN_S3_ULTRASOUND_ROUTE,
                ADMIN_S3_DEVICE_LOG_ROUTE,
                ADMIN_REQUEST_LOG_ROUTE,
                ADMIN_READONLY_SQL_ROUTE,
            ),
        )
        self.assertEqual(
            len({route.name for route in routes}),
            len(routes),
        )


if __name__ == "__main__":
    unittest.main()
