from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import logging
from typing import Any

from boxer.retrieval.connectors.db import _query_db, _validate_readonly_sql
from boxer.retrieval.connectors.s3 import _build_s3_client
from boxer.core import settings as core_settings
from boxer_company import settings as cs
from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
    DeliveryScope,
)
from boxer_company.assistant.scope_guard import (
    AssistantRequestScopeMismatch,
    build_scope_mismatch_result,
    resolve_assistant_request_scope,
)
from boxer_company.assistant.operation_intent import (
    is_explicit_operation_execution,
)
from boxer_company.routers.app_user import (
    _analyze_app_user_baby_selection_by_barcode,
    _lookup_app_user_by_barcode,
    _should_analyze_app_user_baby_selection,
    _should_lookup_barcode,
)
from boxer_company.routers.barcode_validation import (
    _is_barcode_pink_classification_reason_request,
    _is_barcode_validation_status_request,
    _query_barcode_pink_classification_reason,
    _query_barcode_validation_status,
)
from boxer_company.routers.db_query import (
    _extract_db_query,
    _format_db_query_result,
)
from boxer_company.routers.recording_streaming_restore import (
    _is_recording_streaming_restore_request,
    _query_recording_streaming_restore_by_barcode_month,
)
from boxer_company.routers.request_log_query import (
    RequestLogQuerySpec,
    _extract_request_log_query,
    _query_request_log_text,
)
from boxer_company.routers.s3_domain import (
    _extract_s3_request,
    _query_s3_device_log,
    _query_s3_ultrasound_by_barcode,
)


OPERATIONS_ROUTE_GROUP = "operations"

APP_USER_PROFILE_ROUTE = "app_user_lookup"
APP_USER_BABY_ANALYSIS_ROUTE = "app_user_baby_selection_analysis"
BARCODE_VALIDATION_STATUS_ROUTE = "barcode_validation_status"
BARCODE_PINK_CLASSIFICATION_ROUTE = "barcode_pink_classification_reason"
ADMIN_S3_ULTRASOUND_ROUTE = "admin_s3_ultrasound"
ADMIN_S3_DEVICE_LOG_ROUTE = "admin_s3_device_log"
ADMIN_READONLY_SQL_ROUTE = "admin_readonly_sql"
ADMIN_REQUEST_LOG_ROUTE = "admin_request_log"
RECORDING_STREAMING_RESTORE_ROUTE = "recording_streaming_restore"


TextQuery = Callable[[str], str]
S3TextQuery = Callable[[Any, str], str]
S3LogQuery = Callable[[Any, str, str], str]
RestoreQuery = Callable[..., str]
RequestLogQuery = Callable[[RequestLogQuerySpec], str]


def _streaming_restore_enabled() -> bool:
    return bool(cs.RECORDING_STREAMING_RESTORE_ENABLED)


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


def match_private_operations_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """외부 조회나 mutation 없이 operations 요청의 정확한 route만 고른다."""

    if not _is_operations_request(request):
        return None
    try:
        return _match_private_operations_route_strict(request)
    except AssistantRequestScopeMismatch:
        # 실제 실행 route가 같은 불일치를 값 노출 없는 guard 응답으로 닫는다.
        return None


def _is_operations_request(request: CompanyAssistantRequest) -> bool:
    return (
        str(request.metadata.get("route_group") or "").strip()
        == OPERATIONS_ROUTE_GROUP
    )


def _match_private_operations_route_strict(
    request: CompanyAssistantRequest,
) -> str | None:
    question = request.question
    barcode = resolve_assistant_request_scope(request).barcode

    # 더 구체적인 PII 원인 분석을 일반 profile 조회보다 먼저 고정한다.
    if barcode and _should_analyze_app_user_baby_selection(question, barcode):
        return APP_USER_BABY_ANALYSIS_ROUTE
    if barcode and _should_lookup_barcode(question, barcode):
        return APP_USER_PROFILE_ROUTE

    # 복원은 영상·날짜 표현이 다른 조회 matcher와 겹칠 수 있어 mutation 의도를 먼저 고른다.
    if barcode and _is_recording_streaming_restore_request(question, barcode):
        return RECORDING_STREAMING_RESTORE_ROUTE
    if barcode and _is_barcode_pink_classification_reason_request(
        question,
        barcode,
    ):
        return BARCODE_PINK_CLASSIFICATION_ROUTE
    if barcode and _is_barcode_validation_status_request(question, barcode):
        return BARCODE_VALIDATION_STATUS_ROUTE

    s3_route = _match_s3_operation(question)
    if s3_route is not None:
        return s3_route
    if _extract_request_log_query(question) is not None:
        return ADMIN_REQUEST_LOG_ROUTE
    if _extract_db_query(question) is not None:
        return ADMIN_READONLY_SQL_ROUTE
    return None


def _match_s3_operation(question: str) -> str | None:
    """S3 parser만 실행하며 잘못된 형식도 전용 route의 안전한 안내로 보낸다."""

    try:
        request = _extract_s3_request(question)
    except ValueError:
        normalized = " ".join(str(question or "").split())
        lowered = normalized.lower()
        if not lowered.startswith("s3 ") and lowered != "s3":
            return None
        if "로그" in normalized or "log" in lowered:
            return ADMIN_S3_DEVICE_LOG_ROUTE
        if any(token in normalized for token in ("영상", "초음파")) or (
            "ultrasound" in lowered
        ):
            return ADMIN_S3_ULTRASOUND_ROUTE
        return None

    if request is None:
        return None
    if request.get("kind") == "ultrasound":
        return ADMIN_S3_ULTRASOUND_ROUTE
    if request.get("kind") == "log":
        return ADMIN_S3_DEVICE_LOG_ROUTE
    return None


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
            delivery_scope="requester",
            no_evidence=lambda text: "조회된 유저가 없어" in text,
            input_error="바코드 유저 조회 요청 형식을 확인해줘",
            dependency_error="바코드 유저 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
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
            delivery_scope="requester",
            no_evidence=lambda text: "조회된 유저가 없어" in text,
            input_error="유저 선택 원인 분석 요청 형식을 확인해줘",
            dependency_error="유저 선택 원인 분석 중 오류가 발생했어. 잠시 후 다시 시도해줘",
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
            dependency_error="핑크 바코드 분류 확인 중 오류가 발생했어. 잠시 후 다시 시도해줘",
        )


class AdminS3UltrasoundAssistantRoute(_PrivateOperationRoute):
    name = ADMIN_S3_ULTRASOUND_ROUTE

    def __init__(
        self,
        *,
        s3_client_factory: Callable[[], Any] = _build_s3_client,
        query: S3TextQuery = _query_s3_ultrasound_by_barcode,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(logger=logger)
        self._s3_client_factory = s3_client_factory
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
        try:
            parsed = _extract_s3_request(request.question)
        except ValueError:
            return _result(
                route=self.name,
                outcome="needs_input",
                body="S3 영상 조회는 11자리 바코드를 같이 입력해줘",
                delivery_scope="requester",
                fallback_reason="invalid_request",
            )
        if parsed is None or parsed.get("kind") != "ultrasound":
            return None
        return _run_text_query(
            route=self,
            request=request,
            query=lambda: self._query(
                self._s3_client_factory(),
                parsed["barcode"],
            ),
            delivery_scope="requester",
            no_evidence=lambda text: "조회 결과가 없어" in text,
            input_error="S3 영상 조회 요청 형식을 확인해줘",
            dependency_error="S3 영상 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
        )


class AdminS3DeviceLogAssistantRoute(_PrivateOperationRoute):
    name = ADMIN_S3_DEVICE_LOG_ROUTE

    def __init__(
        self,
        *,
        s3_client_factory: Callable[[], Any] = _build_s3_client,
        query: S3LogQuery = _query_s3_device_log,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(logger=logger)
        self._s3_client_factory = s3_client_factory
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
        try:
            parsed = _extract_s3_request(request.question)
        except ValueError:
            return _result(
                route=self.name,
                outcome="needs_input",
                body="S3 로그 조회는 장비명과 YYYY-MM-DD 날짜를 같이 입력해줘",
                delivery_scope="requester",
                fallback_reason="invalid_request",
            )
        if parsed is None or parsed.get("kind") != "log":
            return None
        return _run_text_query(
            route=self,
            request=request,
            query=lambda: self._query(
                self._s3_client_factory(),
                parsed["device_name"],
                parsed["log_date"],
            ),
            delivery_scope="requester",
            no_evidence=lambda text: "찾지 못했어" in text,
            input_error="S3 로그 조회 요청 형식을 확인해줘",
            dependency_error="S3 로그 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
        )


class AdminReadonlySqlAssistantRoute(_PrivateOperationRoute):
    name = ADMIN_READONLY_SQL_ROUTE

    def __init__(
        self,
        *,
        validate: Callable[[str], str] = _validate_readonly_sql,
        query: Callable[[str], dict[str, Any]] = _query_db,
        formatter: Callable[[dict[str, Any]], str] = _format_db_query_result,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(logger=logger)
        self._validate = validate
        self._query = query
        self._formatter = formatter

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        matched = self._match(request)
        if isinstance(matched, CompanyAssistantResult):
            return matched
        if not matched:
            return None

        raw_sql = _extract_db_query(request.question)
        if raw_sql is None:
            return None
        try:
            safe_sql = self._validate(raw_sql)
            db_result = self._query(safe_sql)
            body = slack_mrkdwn_to_commonmark(self._formatter(db_result))
        except ValueError:
            return _result(
                route=self.name,
                outcome="needs_input",
                body="DB 조회는 한 개의 읽기 전용 SQL만 사용할 수 있어",
                delivery_scope="requester",
                fallback_reason="invalid_sql",
            )
        except Exception as exc:
            return self._safe_failure(
                request,
                body="DB 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                fallback_reason="dependency_error",
                delivery_scope="requester",
                error=exc,
            )

        return _result(
            route=self.name,
            outcome="answered" if db_result.get("rows") else "no_evidence",
            body=body or "DB 조회 결과가 없어",
            delivery_scope="requester",
            fallback_reason=(
                None if db_result.get("rows") else "rows_not_found"
            ),
        )


class AdminRequestLogAssistantRoute(_PrivateOperationRoute):
    """API 프로세스가 소유한 감사 저장소만 조회해 요청자 DM으로 반환한다."""

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
                body="API 요청 로그 저장 기능이 꺼져 있어",
                delivery_scope="requester",
                fallback_reason="feature_disabled",
            )
        spec = _extract_request_log_query(request.question)
        if spec is None:
            return None
        return _run_text_query(
            route=self,
            request=request,
            query=lambda: self._query(spec),
            delivery_scope="requester",
            input_error="요청 로그 조회 형식을 확인해줘",
            dependency_error=(
                "요청 로그 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘"
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
        if not is_explicit_operation_execution(request.question):
            return _result(
                route=self.name,
                outcome="needs_input",
                body=(
                    "실제 복원 요청이면 질문형 없이 "
                    "복원 작업을 명시해서 다시 요청해줘"
                ),
                delivery_scope="requester",
                fallback_reason="explicit_execution_required",
            )
        if not self._enabled():
            return _result(
                route=self.name,
                outcome="denied",
                body="스트리밍 종료 영상 복원 기능이 꺼져 있어",
                delivery_scope="requester",
                fallback_reason="feature_disabled",
            )
        actor_id = str(request.actor_id or "").strip()
        if not actor_id:
            return _result(
                route=self.name,
                outcome="denied",
                body="복원 요청자를 확인할 수 없어 실행하지 않았어",
                delivery_scope="requester",
                fallback_reason="missing_actor",
            )
        barcode = resolve_assistant_request_scope(request).barcode or ""
        return _run_text_query(
            route=self,
            request=request,
            query=lambda: self._query(
                barcode,
                request.question,
                requester=actor_id,
                requester_name=None,
            ),
            delivery_scope="requester",
            no_evidence=lambda text: "복원 가능한 영상이 없어" in text,
            input_error="복원할 바코드와 연도·월을 같이 입력해줘",
            dependency_error="스트리밍 종료 영상 복원 중 오류가 발생했어. 잠시 후 다시 시도해줘",
        )


def _run_text_query(
    *,
    route: _PrivateOperationRoute,
    request: CompanyAssistantRequest,
    query: Callable[[], str],
    delivery_scope: DeliveryScope,
    input_error: str,
    dependency_error: str,
    no_evidence: Callable[[str], bool] | None = None,
) -> CompanyAssistantResult:
    try:
        raw_body = str(query() or "").strip()
    except ValueError:
        return _result(
            route=route.name,
            outcome="needs_input",
            body=input_error,
            delivery_scope=delivery_scope,
            fallback_reason="invalid_request",
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
    # 민감 운영 결과는 본문 한 개로만 전달하고 source·LLM·후속 action으로 복제하지 않는다.
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
        suggested_action=None,
        async_job=None,
    )


def build_private_operations_routes(
    deps: PrivateOperationsRouteDeps | None = None,
    *,
    logger: logging.Logger | None = None,
) -> tuple[_PrivateOperationRoute, ...]:
    """operations stage가 사용하는 고정 우선순위 route 묶음을 만든다."""

    actual = deps or PrivateOperationsRouteDeps()
    return (
        AppUserBabySelectionAssistantRoute(
            actual.app_user_baby_analysis_query,
            logger=logger,
        ),
        AppUserProfileAssistantRoute(
            actual.app_user_profile_query,
            logger=logger,
        ),
        RecordingStreamingRestoreAssistantRoute(
            actual.streaming_restore_query,
            enabled=actual.streaming_restore_enabled,
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
        AdminS3UltrasoundAssistantRoute(
            s3_client_factory=actual.s3_client_factory,
            query=actual.s3_ultrasound_query,
            logger=logger,
        ),
        AdminS3DeviceLogAssistantRoute(
            s3_client_factory=actual.s3_client_factory,
            query=actual.s3_device_log_query,
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
            logger=logger,
        ),
    )


__all__ = [
    "ADMIN_READONLY_SQL_ROUTE",
    "ADMIN_REQUEST_LOG_ROUTE",
    "ADMIN_S3_DEVICE_LOG_ROUTE",
    "ADMIN_S3_ULTRASOUND_ROUTE",
    "APP_USER_BABY_ANALYSIS_ROUTE",
    "APP_USER_PROFILE_ROUTE",
    "BARCODE_PINK_CLASSIFICATION_ROUTE",
    "BARCODE_VALIDATION_STATUS_ROUTE",
    "OPERATIONS_ROUTE_GROUP",
    "RECORDING_STREAMING_RESTORE_ROUTE",
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
    "match_private_operations_route",
]
