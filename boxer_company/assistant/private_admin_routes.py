from __future__ import annotations

from boxer_company._operation_routing_private import (
    _is_operations_request,
    _match_private_operations_route_strict,
)
from boxer_company.operation_routing import (
    ADMIN_READONLY_SQL_ROUTE,
    ADMIN_REQUEST_LOG_ROUTE,
    ADMIN_S3_DEVICE_LOG_ROUTE,
    ADMIN_S3_ULTRASOUND_ROUTE,
    APP_USER_BABY_ANALYSIS_ROUTE,
    APP_USER_PROFILE_ROUTE,
    BARCODE_PINK_CLASSIFICATION_ROUTE,
    BARCODE_VALIDATION_STATUS_ROUTE,
    RECORDING_STREAMING_RESTORE_ROUTE,
    RequestLogQuerySpec,
    _extract_db_query,
    _extract_request_log_query,
    _extract_s3_request,
)

from collections.abc import Callable
from dataclasses import dataclass
import logging
import re
from typing import Any

from botocore.exceptions import BotoCoreError, ClientError
import pymysql

from boxer.retrieval.connectors.db import _query_db, _validate_readonly_sql
from boxer.retrieval.connectors.s3 import _build_s3_client
from boxer.core import settings as core_settings
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
    DeliveryScope,
)
from boxer_company.assistant.scope_guard import (
    build_scope_mismatch_result,
)
from boxer_company.read_routing import (
    AssistantRequestScopeMismatch,
    resolve_assistant_request_scope,
)
from boxer_company.retrieval_rules import (
    _build_company_retrieval_rules,
    _transform_company_retrieval_payload,
)
from boxer_company.routers.app_user import (
    _analyze_app_user_baby_selection_by_barcode,
    _lookup_app_user_by_barcode,
)
from boxer_company.routers.barcode_validation import (
    _query_barcode_pink_classification_reason,
    _query_barcode_validation_status,
)
from boxer_company.routers.db_query import (
    _format_db_query_result,
)
from boxer_company.routers.recording_streaming_restore import (
    _query_recording_streaming_restore_by_barcode_month,
)
from boxer_company.routers.request_log_query import (
    _query_request_log_text,
)
from boxer_company.routers.s3_domain import (
    _query_s3_device_log,
    _query_s3_ultrasound_by_barcode,
)


TextQuery = Callable[[str], str]
S3TextQuery = Callable[[Any, str], str]
S3LogQuery = Callable[[Any, str, str], str]
RestoreQuery = Callable[..., str]
RequestLogQuery = Callable[[RequestLogQuerySpec], str]
ValidationErrorMessage = Callable[[ValueError], str]


_DEFAULT_SYNTHESIS_TIMEOUT_MESSAGE = (
    "AI 답변 생성 시간이 초과됐어. 잠시 후 다시 시도해줘"
)

# 기존 Slack은 domain ValueError 본문을 사용자에게 붙여 보냈다. 공통 API는
# parser/validator가 실제로 만드는 고정 문구만 허용해 transport·credential
# 문자열이 ValueError에 섞여도 응답으로 다시 나가지 않게 한다.
_S3_VALIDATION_DETAILS = frozenset(
    {
        "로그 조회는 날짜가 필요해. 예: s3 로그 <device-name> 2026-03-04",
        "날짜 형식은 YYYY-MM-DD로 입력해줘",
        "장비명을 같이 입력해줘. 예: s3 로그 <device-name> 2026-03-04",
        "영상 조회는 바코드(11자리 숫자)가 필요해. 예: s3 영상 12345678910",
        "지원 형식: s3 영상 <바코드> 또는 s3 로그 <장비명> <YYYY-MM-DD>",
    }
)
_BARCODE_VALIDATION_DETAILS = frozenset({"바코드가 필요해"})
_RESTORE_VALIDATION_DETAILS = frozenset(
    {
        "복원할 연도와 월을 같이 입력해줘. 예: `35033165423 2024년 4월 영상 복원`",
        "월은 1월부터 12월까지만 입력할 수 있어",
        "바코드가 필요해",
        "대상 recordings row에 hospitalSeq가 없어 MDA 복원 대상을 확정할 수 없어",
    }
)
_RESTORE_MISSING_ROWS_DETAIL = re.compile(
    r"`\d{11}` `20\d{2}-(?:0[1-9]|1[0-2])` recordings DB row가 없어"
)


def _streaming_restore_enabled() -> bool:
    return bool(cs.RECORDING_STREAMING_RESTORE_ENABLED)


def _s3_query_enabled() -> bool:
    return bool(core_settings.S3_QUERY_ENABLED)


def _db_query_enabled() -> bool:
    return bool(core_settings.DB_QUERY_ENABLED)


def _legacy_validation_message(
    error: ValueError,
    *,
    prefix: str,
    allowed_details: frozenset[str],
    fallback: str,
    allowed_patterns: tuple[re.Pattern[str], ...] = (),
) -> str:
    """허용한 domain 입력 오류만 기존 Slack 형식으로 되돌린다."""

    detail = str(error).strip()
    if detail in allowed_details or any(
        pattern.fullmatch(detail) for pattern in allowed_patterns
    ):
        return f"{prefix}: {detail}"
    return fallback


def _sql_validation_message(error: ValueError) -> str:
    # 길이 제한은 설정값에 따라 달라지므로 validator 실행 시점 값으로만
    # 허용하고, 주입된 임의 ValueError 본문은 일반 안내로 치환한다.
    allowed_details = frozenset(
        {
            f"SQL 길이는 최대 {core_settings.DB_QUERY_MAX_SQL_CHARS}자까지 허용해",
            "한 번에 한 쿼리만 실행할 수 있어",
            "SQL 주석 문법은 허용하지 않아",
            "읽기 전용 쿼리(SELECT/SHOW/DESCRIBE/EXPLAIN/WITH)만 허용해",
            "쓰기/변경 쿼리는 허용하지 않아",
            "파일 입출력/적재 쿼리는 허용하지 않아",
            "잠금 조회(SELECT ... FOR UPDATE)는 허용하지 않아",
        }
    )
    return _legacy_validation_message(
        error,
        prefix="DB 조회 요청 형식 오류",
        allowed_details=allowed_details,
        fallback="DB 조회는 한 개의 읽기 전용 SQL만 사용할 수 있어",
    )


@dataclass(frozen=True, slots=True)
class PrivateOperationsRouteDeps:
    """운영 route의 기존 domain 함수를 주입해 API 조립과 테스트를 분리한다."""

    app_user_profile_query: TextQuery = _lookup_app_user_by_barcode
    app_user_baby_analysis_query: TextQuery = (
        _analyze_app_user_baby_selection_by_barcode
    )
    barcode_validation_query: TextQuery = _query_barcode_validation_status
    barcode_pink_query: TextQuery = _query_barcode_pink_classification_reason
    s3_client_factory: Callable[[], Any] = _build_s3_client
    s3_ultrasound_query: S3TextQuery = _query_s3_ultrasound_by_barcode
    s3_device_log_query: S3LogQuery = _query_s3_device_log
    validate_readonly_sql: Callable[[str], str] = _validate_readonly_sql
    query_db: Callable[[str], dict[str, Any]] = _query_db
    format_db_result: Callable[[dict[str, Any]], str] = _format_db_query_result
    request_log_query: RequestLogQuery = _query_request_log_text
    streaming_restore_query: RestoreQuery = (
        _query_recording_streaming_restore_by_barcode_month
    )
    streaming_restore_enabled: Callable[[], bool] = (
        _streaming_restore_enabled
    )
    # 공통 API도 기존 Slack local의 조회 kill switch를 그대로 따른다.
    s3_query_enabled: Callable[[], bool] = _s3_query_enabled
    db_query_enabled: Callable[[], bool] = _db_query_enabled
    # 실제 composer는 API factory가 조립한다. 단위 route는 주입이 없으면
    # 기존 조회 문자열을 그대로 반환해 local rollback과 테스트를 보존한다.
    answer_composer: CompanyEvidenceAnswerComposer | None = None
    timeout_message: str = _DEFAULT_SYNTHESIS_TIMEOUT_MESSAGE


def _match_for_execution(
    request: CompanyAssistantRequest,
) -> str | CompanyAssistantResult | None:
    if not _is_operations_request(request):
        return None
    try:
        return _match_private_operations_route_strict(request)
    except AssistantRequestScopeMismatch as mismatch:
        return build_scope_mismatch_result(mismatch)


class _PrivateOperationRoute:
    name: str

    def __init__(self, *, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger(__name__)

    def _match(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | bool:
        matched = _match_for_execution(request)
        if isinstance(matched, CompanyAssistantResult):
            return matched
        return matched == self.name

    def _safe_failure(
        self,
        request: CompanyAssistantRequest,
        *,
        body: str,
        fallback_reason: str,
        delivery_scope: DeliveryScope,
        error: Exception,
    ) -> CompanyAssistantResult:
        # 오류 원문과 조회 scope는 로그·응답 어디에도 남기지 않는다.
        self._logger.warning(
            "Private operation failed route=%s request_id=%s error_type=%s",
            self.name,
            request.request_id,
            type(error).__name__,
        )
        return _result(
            route=self.name,
            outcome="failed",
            body=body,
            delivery_scope=delivery_scope,
            fallback_reason=fallback_reason,
        )


class AppUserProfileAssistantRoute(_PrivateOperationRoute):
    name = APP_USER_PROFILE_ROUTE

    def __init__(
        self,
        query: TextQuery = _lookup_app_user_by_barcode,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(logger=logger)
        self._query = query

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        matched = self._match(request)
        if isinstance(matched, CompanyAssistantResult):
            return matched
        if not matched:
            return None
        barcode = resolve_assistant_request_scope(request).barcode or ""
        return _run_text_query(
            route=self,
            request=request,
            query=lambda: self._query(barcode),
            delivery_scope="conversation",
            no_evidence=lambda text: "조회된 유저가 없어" in text,
            dependency_error="바코드 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
        )


class AppUserBabySelectionAssistantRoute(_PrivateOperationRoute):
    name = APP_USER_BABY_ANALYSIS_ROUTE

    def __init__(
        self,
        query: TextQuery = _analyze_app_user_baby_selection_by_barcode,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(logger=logger)
        self._query = query

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        matched = self._match(request)
        if isinstance(matched, CompanyAssistantResult):
            return matched
        if not matched:
            return None
        barcode = resolve_assistant_request_scope(request).barcode or ""
        return _run_text_query(
            route=self,
            request=request,
            query=lambda: self._query(barcode),
            delivery_scope="conversation",
            no_evidence=lambda text: "조회된 유저가 없어" in text,
            dependency_error="유저 조회 원인분석 중 오류가 발생했어. 잠시 후 다시 시도해줘",
        )


class BarcodeValidationStatusAssistantRoute(_PrivateOperationRoute):
    name = BARCODE_VALIDATION_STATUS_ROUTE

    def __init__(
        self,
        query: TextQuery = _query_barcode_validation_status,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(logger=logger)
        self._query = query

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        matched = self._match(request)
        if isinstance(matched, CompanyAssistantResult):
            return matched
        if not matched:
            return None
        barcode = resolve_assistant_request_scope(request).barcode or ""
        return _run_text_query(
            route=self,
            request=request,
            query=lambda: self._query(barcode),
            delivery_scope="conversation",
            input_error="바코드 유효성 검사 요청 형식을 확인해줘",
            validation_error_message=lambda error: (
                _legacy_validation_message(
                    error,
                    prefix="바코드 유효성 검사 확인 요청 형식 오류",
                    allowed_details=_BARCODE_VALIDATION_DETAILS,
                    fallback="바코드 유효성 검사 요청 형식을 확인해줘",
                )
            ),
            runtime_error="바코드 유효성 검사 확인 중 오류가 발생했어. MDA 연결 상태를 확인해줘",
            dependency_error="바코드 유효성 검사 확인 중 오류가 발생했어. 잠시 후 다시 시도해줘",
        )


class BarcodePinkClassificationAssistantRoute(_PrivateOperationRoute):
    name = BARCODE_PINK_CLASSIFICATION_ROUTE

    def __init__(
        self,
        query: TextQuery = _query_barcode_pink_classification_reason,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(logger=logger)
        self._query = query

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        matched = self._match(request)
        if isinstance(matched, CompanyAssistantResult):
            return matched
        if not matched:
            return None
        barcode = resolve_assistant_request_scope(request).barcode or ""
        return _run_text_query(
            route=self,
            request=request,
            query=lambda: self._query(barcode),
            delivery_scope="conversation",
            input_error="핑크 바코드 분류 확인 요청 형식을 확인해줘",
            validation_error_message=lambda error: (
                _legacy_validation_message(
                    error,
                    prefix="핑크 바코드 분류 근거 확인 요청 형식 오류",
                    allowed_details=_BARCODE_VALIDATION_DETAILS,
                    fallback="핑크 바코드 분류 확인 요청 형식을 확인해줘",
                )
            ),
            runtime_error="핑크 바코드 분류 근거 확인 중 오류가 발생했어. DB/MDA 연결 상태를 확인해줘",
            dependency_error="핑크 바코드 분류 근거 확인 중 오류가 발생했어. 잠시 후 다시 시도해줘",
        )


class AdminS3UltrasoundAssistantRoute(_PrivateOperationRoute):
    name = ADMIN_S3_ULTRASOUND_ROUTE

    def __init__(
        self,
        *,
        s3_client_factory: Callable[[], Any] = _build_s3_client,
        query: S3TextQuery = _query_s3_ultrasound_by_barcode,
        enabled: Callable[[], bool] = _s3_query_enabled,
        answer_composer: CompanyEvidenceAnswerComposer | None = None,
        timeout_message: str = _DEFAULT_SYNTHESIS_TIMEOUT_MESSAGE,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(logger=logger)
        self._s3_client_factory = s3_client_factory
        self._query = query
        self._enabled = enabled
        self._answer_composer = answer_composer
        self._timeout_message = timeout_message

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        matched = self._match(request)
        if isinstance(matched, CompanyAssistantResult):
            return matched
        if not matched:
            return None
        try:
            parsed = _extract_s3_request(request.question)
        except ValueError as exc:
            return _result(
                route=self.name,
                outcome="needs_input",
                body=_legacy_validation_message(
                    exc,
                    prefix="S3 조회 요청 형식 오류",
                    allowed_details=_S3_VALIDATION_DETAILS,
                    fallback="S3 영상 조회는 11자리 바코드를 같이 입력해줘",
                ),
                delivery_scope="conversation",
                fallback_reason="invalid_request",
            )
        if parsed is None or parsed.get("kind") != "ultrasound":
            return None
        if not self._enabled():
            return _s3_disabled_result(self.name)
        return _run_s3_text_query(
            self,
            request,
            lambda: self._query(
                self._s3_client_factory(), parsed["barcode"]
            ),
            no_evidence=lambda text: "조회 결과가 없어" in text,
            answer_composer=self._answer_composer,
            timeout_message=self._timeout_message,
            evidence_builder=lambda result_text: {
                "route": "s3_ultrasound",
                "source": "s3",
                "request": {
                    "kind": "ultrasound",
                    "barcode": parsed["barcode"],
                },
                "result": result_text,
            },
        )


class AdminS3DeviceLogAssistantRoute(_PrivateOperationRoute):
    name = ADMIN_S3_DEVICE_LOG_ROUTE

    def __init__(
        self,
        *,
        s3_client_factory: Callable[[], Any] = _build_s3_client,
        query: S3LogQuery = _query_s3_device_log,
        enabled: Callable[[], bool] = _s3_query_enabled,
        answer_composer: CompanyEvidenceAnswerComposer | None = None,
        timeout_message: str = _DEFAULT_SYNTHESIS_TIMEOUT_MESSAGE,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(logger=logger)
        self._s3_client_factory = s3_client_factory
        self._query = query
        self._enabled = enabled
        self._answer_composer = answer_composer
        self._timeout_message = timeout_message

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        matched = self._match(request)
        if isinstance(matched, CompanyAssistantResult):
            return matched
        if not matched:
            return None
        try:
            parsed = _extract_s3_request(request.question)
        except ValueError as exc:
            return _result(
                route=self.name,
                outcome="needs_input",
                body=_legacy_validation_message(
                    exc,
                    prefix="S3 조회 요청 형식 오류",
                    allowed_details=_S3_VALIDATION_DETAILS,
                    fallback=(
                        "S3 로그 조회는 장비명과 YYYY-MM-DD 날짜를 같이 입력해줘"
                    ),
                ),
                delivery_scope="conversation",
                fallback_reason="invalid_request",
            )
        if parsed is None or parsed.get("kind") != "log":
            return None
        if not self._enabled():
            return _s3_disabled_result(self.name)
        # 두 S3 route는 기존 Slack local과 같은 오류 분류·문구를 공유한다.
        return _run_s3_text_query(
            self,
            request,
            lambda: self._query(
                self._s3_client_factory(),
                parsed["device_name"],
                parsed["log_date"],
            ),
            no_evidence=lambda text: "찾지 못했어" in text,
            answer_composer=self._answer_composer,
            timeout_message=self._timeout_message,
            evidence_builder=lambda result_text: {
                "route": "s3_device_log",
                "source": "s3",
                "request": {
                    "kind": "log",
                    "deviceName": parsed["device_name"],
                    "logDate": parsed["log_date"],
                },
                "result": result_text,
            },
        )


def _s3_disabled_result(route: str) -> CompanyAssistantResult:
    return _result(
        route=route,
        outcome="denied",
        body=(
            "S3 조회 기능이 꺼져 있어. "
            ".env에서 S3_QUERY_ENABLED=true로 설정해줘"
        ),
        delivery_scope="conversation",
        fallback_reason="feature_disabled",
    )


def _run_s3_text_query(
    route: _PrivateOperationRoute,
    request: CompanyAssistantRequest,
    query: Callable[[], str],
    *,
    no_evidence: Callable[[str], bool],
    answer_composer: CompanyEvidenceAnswerComposer | None,
    timeout_message: str,
    evidence_builder: Callable[[str], dict[str, Any]],
) -> CompanyAssistantResult:
    """기존 Slack S3 route의 provider/일반 오류 분기를 그대로 보존한다."""

    try:
        raw_body = str(query() or "").strip()
    except (BotoCoreError, ClientError) as exc:
        return route._safe_failure(
            request,
            body="S3 조회 중 오류가 발생했어. 버킷 권한/리전/키 경로를 확인해줘",
            fallback_reason="dependency_error",
            delivery_scope="conversation",
            error=exc,
        )
    except Exception as exc:
        return route._safe_failure(
            request,
            body="S3 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
            fallback_reason="dependency_error",
            delivery_scope="conversation",
            error=exc,
        )

    if not raw_body:
        return _result(
            route=route.name,
            outcome="failed",
            body="S3 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
            delivery_scope="conversation",
            fallback_reason="empty_result",
        )
    has_no_evidence = no_evidence(raw_body)
    outcome: AssistantOutcome = (
        "no_evidence" if has_no_evidence else "answered"
    )
    return _compose_admin_retrieval_result(
        composer=answer_composer,
        request=request,
        route=route.name,
        fallback_body=slack_mrkdwn_to_commonmark(raw_body),
        fallback_outcome=outcome,
        fallback_reason="no_evidence" if has_no_evidence else None,
        timeout_message=timeout_message,
        evidence=evidence_builder(raw_body),
    )


class AdminReadonlySqlAssistantRoute(_PrivateOperationRoute):
    name = ADMIN_READONLY_SQL_ROUTE

    def __init__(
        self,
        *,
        validate: Callable[[str], str] = _validate_readonly_sql,
        query: Callable[[str], dict[str, Any]] = _query_db,
        formatter: Callable[[dict[str, Any]], str] = _format_db_query_result,
        enabled: Callable[[], bool] = _db_query_enabled,
        answer_composer: CompanyEvidenceAnswerComposer | None = None,
        timeout_message: str = _DEFAULT_SYNTHESIS_TIMEOUT_MESSAGE,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(logger=logger)
        self._validate = validate
        self._query = query
        self._formatter = formatter
        self._enabled = enabled
        self._answer_composer = answer_composer
        self._timeout_message = timeout_message

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        matched = self._match(request)
        if isinstance(matched, CompanyAssistantResult):
            return matched
        if not matched:
            return None
        if not self._enabled():
            return _result(
                route=self.name,
                outcome="denied",
                body=(
                    "DB 조회 기능이 꺼져 있어. "
                    ".env에서 DB_QUERY_ENABLED=true로 설정해줘"
                ),
                delivery_scope="conversation",
                fallback_reason="feature_disabled",
            )

        raw_sql = _extract_db_query(request.question)
        if raw_sql is None:
            return None
        try:
            safe_sql = self._validate(raw_sql)
            db_result = self._query(safe_sql)
            formatted_result = str(self._formatter(db_result) or "").strip()
            body = slack_mrkdwn_to_commonmark(formatted_result)
        except ValueError as exc:
            return _result(
                route=self.name,
                outcome="needs_input",
                body=_sql_validation_message(exc),
                delivery_scope="conversation",
                fallback_reason="invalid_sql",
            )
        except pymysql.MySQLError as exc:
            return self._safe_failure(
                request,
                body="DB 조회 중 오류가 발생했어. 연결 정보와 네트워크 상태를 확인해줘",
                fallback_reason="dependency_error",
                delivery_scope="conversation",
                error=exc,
            )
        except Exception as exc:
            return self._safe_failure(
                request,
                body="DB 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                fallback_reason="dependency_error",
                delivery_scope="conversation",
                error=exc,
            )

        has_rows = bool(db_result.get("rows"))
        outcome: AssistantOutcome = "answered" if has_rows else "no_evidence"
        return _compose_admin_retrieval_result(
            composer=self._answer_composer,
            request=request,
            route=self.name,
            fallback_body=body or "DB 조회 결과가 없어",
            fallback_outcome=outcome,
            fallback_reason=None if has_rows else "rows_not_found",
            timeout_message=self._timeout_message,
            evidence={
                "route": "db_query",
                "source": "db",
                "request": {
                    "question": request.question,
                    "sql": safe_sql,
                },
                "dbResult": db_result,
                "formattedResult": formatted_result,
            },
        )


def _compose_admin_retrieval_result(
    *,
    composer: CompanyEvidenceAnswerComposer | None,
    request: CompanyAssistantRequest,
    route: str,
    fallback_body: str,
    fallback_outcome: AssistantOutcome,
    fallback_reason: str | None,
    timeout_message: str,
    evidence: dict[str, Any],
) -> CompanyAssistantResult:
    """S3/SQL의 기존 evidence 합성을 재사용하고 미주입 시 직접 응답한다."""

    if composer is None:
        return _result(
            route=route,
            outcome=fallback_outcome,
            body=fallback_body,
            delivery_scope="conversation",
            fallback_reason=fallback_reason,
        )

    return composer.compose(
        request,
        evidence=evidence,
        policy=CompanyEvidenceAnswerPolicy(
            route=route,
            fallback_message=fallback_body,
            fallback_outcome=fallback_outcome,
            include_context=bool(
                core_settings.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT
            ),
            timeout_message=(
                str(timeout_message or "").strip()
                or _DEFAULT_SYNTHESIS_TIMEOUT_MESSAGE
            ),
            system_prompt=cs.RETRIEVAL_SYSTEM_PROMPT or None,
            extra_rules=_build_company_retrieval_rules(evidence),
            evidence_transform=_transform_company_retrieval_payload,
        ),
    )


class AdminRequestLogAssistantRoute(_PrivateOperationRoute):
    """API 프로세스가 소유한 감사 저장소를 기존 대화 scope로 반환한다."""

    name = ADMIN_REQUEST_LOG_ROUTE

    def __init__(
        self,
        query: RequestLogQuery = _query_request_log_text,
        *,
        enabled: Callable[[], bool] = lambda: bool(
            core_settings.REQUEST_LOG_SQLITE_ENABLED
        ),
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(logger=logger)
        self._query = query
        self._enabled = enabled

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        matched = self._match(request)
        if isinstance(matched, CompanyAssistantResult):
            return matched
        if not matched:
            return None
        if not self._enabled():
            return _result(
                route=self.name,
                outcome="denied",
                body=(
                    "요청 로그 저장 기능이 꺼져 있어. "
                    ".env에서 REQUEST_LOG_SQLITE_ENABLED=true로 설정해줘"
                ),
                delivery_scope="conversation",
                fallback_reason="feature_disabled",
            )
        spec = _extract_request_log_query(request.question)
        if spec is None:
            return None
        return _run_text_query(
            route=self,
            request=request,
            query=lambda: self._query(spec),
            delivery_scope="conversation",
            dependency_error=(
                "요청 로그 조회 중 오류가 발생했어. SQLite 파일과 권한 상태를 확인해줘"
            ),
            no_evidence=lambda text: "요청 로그가 없어" in text,
        )


class RecordingStreamingRestoreAssistantRoute(_PrivateOperationRoute):
    name = RECORDING_STREAMING_RESTORE_ROUTE

    def __init__(
        self,
        query: RestoreQuery = _query_recording_streaming_restore_by_barcode_month,
        *,
        enabled: Callable[[], bool] = _streaming_restore_enabled,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(logger=logger)
        self._query = query
        self._enabled = enabled

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        matched = self._match(request)
        if isinstance(matched, CompanyAssistantResult):
            return matched
        if not matched:
            return None
        if not self._enabled():
            return _result(
                route=self.name,
                outcome="denied",
                body=(
                    "스트리밍 종료 영상 복원 기능이 꺼져 있어. "
                    ".env에서 RECORDING_STREAMING_RESTORE_ENABLED=true로 "
                    "설정해줘"
                ),
                delivery_scope="conversation",
                fallback_reason="feature_disabled",
            )
        actor_id = str(request.actor_id or "").strip()
        if not actor_id:
            return _result(
                route=self.name,
                outcome="denied",
                body="복원 요청자를 확인할 수 없어 실행하지 않았어",
                delivery_scope="conversation",
                fallback_reason="missing_actor",
            )
        barcode = resolve_assistant_request_scope(request).barcode or ""
        requester_name = str(
            request.metadata.get("actor_name") or ""
        ).strip() or None
        return _run_text_query(
            route=self,
            request=request,
            query=lambda: self._query(
                barcode,
                request.question,
                requester=actor_id,
                requester_name=requester_name,
            ),
            delivery_scope="conversation",
            no_evidence=lambda text: "복원 가능한 영상이 없어" in text,
            input_error="복원할 바코드와 연도·월을 같이 입력해줘",
            validation_error_message=lambda error: (
                _legacy_validation_message(
                    error,
                    prefix="스트리밍 종료 영상 복원 요청 형식 오류",
                    allowed_details=_RESTORE_VALIDATION_DETAILS,
                    allowed_patterns=(_RESTORE_MISSING_ROWS_DETAIL,),
                    fallback="복원할 바코드와 연도·월을 같이 입력해줘",
                )
            ),
            runtime_error="스트리밍 종료 영상 복원 중 오류가 발생했어. MDA 연결 상태를 확인해줘",
            dependency_error="스트리밍 종료 영상 복원 중 오류가 발생했어. 잠시 후 다시 시도해줘",
        )


def _run_text_query(
    *,
    route: _PrivateOperationRoute,
    request: CompanyAssistantRequest,
    query: Callable[[], str],
    delivery_scope: DeliveryScope,
    dependency_error: str,
    input_error: str | None = None,
    validation_error_message: ValidationErrorMessage | None = None,
    runtime_error: str | None = None,
    no_evidence: Callable[[str], bool] | None = None,
) -> CompanyAssistantResult:
    try:
        raw_body = str(query() or "").strip()
    except ValueError as exc:
        return _result(
            route=route.name,
            outcome="needs_input",
            body=(
                validation_error_message(exc)
                if validation_error_message is not None
                else input_error or dependency_error
            ),
            delivery_scope=delivery_scope,
            fallback_reason="invalid_request",
        )
    except RuntimeError as exc:
        return route._safe_failure(
            request,
            body=runtime_error or dependency_error,
            fallback_reason="dependency_error",
            delivery_scope=delivery_scope,
            error=exc,
        )
    except Exception as exc:
        return route._safe_failure(
            request,
            body=dependency_error,
            fallback_reason="dependency_error",
            delivery_scope=delivery_scope,
            error=exc,
        )

    if not raw_body:
        return _result(
            route=route.name,
            outcome="failed",
            body=dependency_error,
            delivery_scope=delivery_scope,
            fallback_reason="empty_result",
        )
    has_no_evidence = bool(no_evidence and no_evidence(raw_body))
    return _result(
        route=route.name,
        outcome="no_evidence" if has_no_evidence else "answered",
        body=slack_mrkdwn_to_commonmark(raw_body),
        delivery_scope=delivery_scope,
        fallback_reason="no_evidence" if has_no_evidence else None,
    )


def _result(
    *,
    route: str,
    outcome: AssistantOutcome,
    body: str,
    delivery_scope: DeliveryScope,
    fallback_reason: str | None,
) -> CompanyAssistantResult:
    # direct 결과는 기존 Slack reply처럼 대화 본문 한 개로만 전달한다.
    # S3/SQL LLM 합성은 위의 선택적 composer 경로에서만 명시적으로 실행한다.
    return CompanyAssistantResult(
        route=route,
        outcome=outcome,
        messages=(
            AssistantMessage(
                body=body,
                delivery_scope=delivery_scope,
                mention_actor=delivery_scope == "conversation",
                format="commonmark",
            ),
        ),
        sources=(),
        used_llm=False,
        fallback_reason=fallback_reason,
    )


def build_private_operations_routes(
    deps: PrivateOperationsRouteDeps | None = None,
    *,
    answer_composer: CompanyEvidenceAnswerComposer | None = None,
    timeout_message: str | None = None,
    logger: logging.Logger | None = None,
) -> tuple[_PrivateOperationRoute, ...]:
    """operations stage가 사용하는 고정 우선순위 route 묶음을 만든다."""

    actual = deps or PrivateOperationsRouteDeps()
    actual_answer_composer = answer_composer or actual.answer_composer
    actual_timeout_message = (
        str(timeout_message or actual.timeout_message or "").strip()
        or _DEFAULT_SYNTHESIS_TIMEOUT_MESSAGE
    )
    return (
        AppUserBabySelectionAssistantRoute(
            actual.app_user_baby_analysis_query,
            logger=logger,
        ),
        AppUserProfileAssistantRoute(
            actual.app_user_profile_query,
            logger=logger,
        ),
        BarcodePinkClassificationAssistantRoute(
            actual.barcode_pink_query,
            logger=logger,
        ),
        BarcodeValidationStatusAssistantRoute(
            actual.barcode_validation_query,
            logger=logger,
        ),
        RecordingStreamingRestoreAssistantRoute(
            actual.streaming_restore_query,
            enabled=actual.streaming_restore_enabled,
            logger=logger,
        ),
        AdminS3UltrasoundAssistantRoute(
            s3_client_factory=actual.s3_client_factory,
            query=actual.s3_ultrasound_query,
            enabled=actual.s3_query_enabled,
            answer_composer=actual_answer_composer,
            timeout_message=actual_timeout_message,
            logger=logger,
        ),
        AdminS3DeviceLogAssistantRoute(
            s3_client_factory=actual.s3_client_factory,
            query=actual.s3_device_log_query,
            enabled=actual.s3_query_enabled,
            answer_composer=actual_answer_composer,
            timeout_message=actual_timeout_message,
            logger=logger,
        ),
        AdminRequestLogAssistantRoute(
            actual.request_log_query,
            logger=logger,
        ),
        AdminReadonlySqlAssistantRoute(
            validate=actual.validate_readonly_sql,
            query=actual.query_db,
            formatter=actual.format_db_result,
            enabled=actual.db_query_enabled,
            answer_composer=actual_answer_composer,
            timeout_message=actual_timeout_message,
            logger=logger,
        ),
    )


__all__ = [
    "AdminReadonlySqlAssistantRoute",
    "AdminRequestLogAssistantRoute",
    "AdminS3DeviceLogAssistantRoute",
    "AdminS3UltrasoundAssistantRoute",
    "AppUserBabySelectionAssistantRoute",
    "AppUserProfileAssistantRoute",
    "BarcodePinkClassificationAssistantRoute",
    "BarcodeValidationStatusAssistantRoute",
    "PrivateOperationsRouteDeps",
    "RecordingStreamingRestoreAssistantRoute",
    "build_private_operations_routes",
]
