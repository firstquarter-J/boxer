from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import logging
import re
import threading
from typing import Callable, Protocol
from urllib.parse import urlsplit

from boxer_company.assistant import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.notion_workspace_search import (
    _looks_like_company_notion_search,
)
from boxer_company.routers.barcode_log import (
    _extract_log_date_with_presence,
)
from boxer_company.assistant.barcode_log_route import (
    match_barcode_log_route,
)
from boxer_company.assistant.barcode_query_route import (
    match_barcode_query_route,
)
from boxer_company.assistant.device_led_routes import (
    match_device_read_route,
)
from boxer_company.assistant.recording_failure_route import (
    match_recording_failure_route,
)
from boxer_company.assistant.structured_route import (
    match_structured_device_count_route,
    match_structured_read_route,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiAmbiguousTimeoutError,
    CompanyApiAvailabilityError,
    CompanyApiClientError,
    CompanyApiClientSettings,
    CompanyApiContractError,
    CompanyApiPolicyError,
    CompanyAssistantApiClient,
)


_ALLOWED_NOTION_ROUTES = frozenset(
    {
        "company_notion_search",
        "company_notion_qa",
    }
)
_ALLOWED_STRUCTURED_ROUTES = frozenset(
    {
        "hospitals_filter",
        "hospital_rooms_filter",
        "ultrasound_captures_filter",
        "recordings_filter",
    }
)
_ALLOWED_DEVICE_ROUTES = frozenset(
    {
        "device_led_log_analysis",
        "device_led_pattern_guide",
    }
)
_ALLOWED_DEVICE_FILTER_ROUTES = frozenset({"devices_filter"})
_ALLOWED_RECORDING_FAILURE_ROUTES = frozenset(
    {"recording_failure_analysis"}
)
_ALLOWED_BARCODE_LOG_ROUTES = frozenset({"barcode_log_analysis"})
_ALLOWED_BARCODE_ROUTES = frozenset(
    {
        "barcode_video_count",
        "barcode_video_info",
        "barcode_video_list",
        "barcode_video_length",
        "barcode_all_recorded_dates",
    }
)
_SAFE_REQUEST_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$"
)
_NOTION_SOURCE_HOSTS = frozenset(
    {
        "app.notion.com",
        "notion.so",
        "www.notion.so",
    }
)


class _LocalAssistantService(Protocol):
    def answer(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None: ...


class _ShadowRunner(Protocol):
    def submit(self, task: Callable[[], None]) -> bool | None: ...


class BoundedShadowRunner:
    """동시에 남아 있을 수 있는 shadow 작업 수를 제한한 daemon 실행기다."""

    def __init__(
        self,
        *,
        max_pending: int = 4,
        logger: logging.Logger | None = None,
    ) -> None:
        if max_pending < 1:
            raise ValueError("max_pending must be positive")
        self._slots = threading.BoundedSemaphore(max_pending)
        self._logger = logger or logging.getLogger(__name__)

    def submit(self, task: Callable[[], None]) -> bool:
        # Slack event 처리를 막지 않고, API 지연이 쌓이면
        # 새 비교 작업만 버린다.
        if not self._slots.acquire(blocking=False):
            return False

        def run() -> None:
            try:
                task()
            except Exception as exc:
                # 질문·응답·token이 예외 문자열에 섞일 수 있어
                # 타입만 남긴다.
                self._logger.warning(
                    "Company API shadow task failed error_type=%s",
                    type(exc).__name__,
                )
            finally:
                self._slots.release()

        thread = threading.Thread(
            target=run,
            name="boxer-company-api-shadow",
            daemon=True,
        )
        try:
            thread.start()
        except Exception:
            self._slots.release()
            raise
        return True


_DEFAULT_SHADOW_RUNNER = BoundedShadowRunner()


@dataclass(frozen=True, slots=True)
class _RemoteResultValidation:
    accepted: bool
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class _RolloutProfile:
    log_label: str
    allowed_routes: frozenset[str]
    matches_request: Callable[[CompanyAssistantRequest], bool]
    validate_result: Callable[
        [CompanyAssistantResult | None],
        _RemoteResultValidation,
    ]
    failure_route: str
    failure_body: str
    expected_route: (
        Callable[[CompanyAssistantRequest], str | None]
        | None
    ) = None
    fallback_on_unexpected_route: bool = False


class _CompanyApiRolloutService:
    """허용된 read-only route 묶음만 공통 API로 점진 전환한다."""

    def __init__(
        self,
        local_service: _LocalAssistantService,
        *,
        mode: str,
        fallback_enabled: bool,
        profile: _RolloutProfile,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
        shadow_runner: _ShadowRunner,
    ) -> None:
        self._local_service = local_service
        self._mode = mode
        self._fallback_enabled = fallback_enabled
        self._profile = profile
        self._api_client = api_client
        self._logger = logger
        self._shadow_runner = shadow_runner

    def answer(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        # 순수 matcher를 먼저 적용해 다른 read-only route나 mutation 문장을
        # 전체-stage HTTP endpoint가 선점하지 못하게 한다.
        if not self._profile.matches_request(request):
            return self._local_service.answer(request)

        if self._mode == "shadow":
            return self._answer_shadow(request)
        if self._mode == "remote":
            return self._answer_remote(request)
        return self._local_service.answer(request)

    def answer_with_progress(
        self,
        request: CompanyAssistantRequest,
        on_partial_result: Callable[[CompanyAssistantResult], None],
    ) -> CompanyAssistantResult | None:
        """barcode log의 부분 응답 계약을 local/shadow 전환 중에도 보존한다."""

        local_answer = lambda: self._answer_local_with_progress(
            request,
            on_partial_result,
        )
        if not self._profile.matches_request(request):
            return local_answer()
        if self._mode == "remote":
            # API는 한 번의 HTTP 응답에 전체 메시지를 반환하므로 최종
            # 결과로 전달하고 Slack renderer가 같은 순서로 출력한다.
            return self._answer_remote(
                request,
                local_answer=local_answer,
            )
        if self._mode != "shadow":
            return local_answer()

        partial_results: list[CompanyAssistantResult] = []

        def forward_partial(result: CompanyAssistantResult) -> None:
            partial_results.append(result)
            on_partial_result(result)

        local_result = self._answer_local_with_progress(
            request,
            forward_partial,
        )
        comparable_result = _combine_progress_results(
            partial_results,
            local_result,
        )
        if _is_shadow_eligible_result(
            comparable_result,
            self._profile.allowed_routes,
        ):
            assert comparable_result is not None
            self._submit_shadow_comparison(
                request,
                comparable_result,
            )
        return local_result

    def _answer_shadow(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        local_result = self._local_service.answer(request)
        if not _is_shadow_eligible_result(
            local_result,
            self._profile.allowed_routes,
        ):
            return local_result
        self._submit_shadow_comparison(request, local_result)
        return local_result

    def _submit_shadow_comparison(
        self,
        request: CompanyAssistantRequest,
        local_result: CompanyAssistantResult,
    ) -> None:
        def compare_remote() -> None:
            self._compare_shadow_result(request, local_result)

        try:
            accepted = self._shadow_runner.submit(compare_remote)
        except Exception as exc:
            self._logger.warning(
                "%s shadow submission failed "
                "request_id=%s error_type=%s",
                self._profile.log_label,
                _safe_request_id(request.request_id),
                type(exc).__name__,
            )
            return
        # 테스트용 inline submitter는 반환값이 없을 수 있어 명시적 False만
        # capacity 거부로 해석한다.
        if accepted is False:
            self._logger.warning(
                "%s shadow skipped request_id=%s reason=capacity",
                self._profile.log_label,
                _safe_request_id(request.request_id),
            )
        # shadow 결과는 renderer로 절대 반환하지 않아 Slack 중복 응답을 막는다.

    def _compare_shadow_result(
        self,
        request: CompanyAssistantRequest,
        local_result: CompanyAssistantResult,
    ) -> None:
        try:
            remote_result = self._call_api(request)
        except CompanyApiAmbiguousTimeoutError:
            self._log_shadow_error(request, "ambiguous_timeout")
            return
        except CompanyApiAvailabilityError:
            self._log_shadow_error(request, "availability")
            return
        except CompanyApiPolicyError:
            self._log_shadow_error(request, "policy")
            return
        except CompanyApiContractError:
            self._log_shadow_error(request, "contract")
            return
        except CompanyApiClientError:
            self._log_shadow_error(request, "client")
            return
        except Exception:
            self._log_shadow_error(request, "unexpected")
            return

        validation = self._validate_remote_result(
            request,
            remote_result,
        )
        if not validation.accepted:
            self._logger.info(
                "%s shadow comparison "
                "request_id=%s accepted=false reason=%s",
                self._profile.log_label,
                _safe_request_id(request.request_id),
                validation.reason or "unknown",
            )
            return

        assert remote_result is not None
        self._logger.info(
            "%s shadow comparison "
            "request_id=%s accepted=true "
            "route_match=%s outcome_match=%s fallback_match=%s "
            "used_llm_match=%s source_set_match=%s "
            "message_scope_match=%s message_body_match=%s "
            "local_source_count=%s remote_source_count=%s "
            "local_message_count=%s remote_message_count=%s",
            self._profile.log_label,
            _safe_request_id(request.request_id),
            local_result.route == remote_result.route,
            local_result.outcome == remote_result.outcome,
            local_result.fallback_reason
            == remote_result.fallback_reason,
            local_result.used_llm == remote_result.used_llm,
            _source_set(local_result) == _source_set(remote_result),
            _message_scopes(local_result)
            == _message_scopes(remote_result),
            _message_body_digests(local_result)
            == _message_body_digests(remote_result),
            len(local_result.sources),
            len(remote_result.sources),
            len(local_result.messages),
            len(remote_result.messages),
        )

    def _answer_remote(
        self,
        request: CompanyAssistantRequest,
        *,
        local_answer: Callable[[], CompanyAssistantResult | None]
        | None = None,
    ) -> CompanyAssistantResult | None:
        fallback = local_answer or (
            lambda: self._local_service.answer(request)
        )
        try:
            remote_result = self._call_api(request)
        except CompanyApiAmbiguousTimeoutError:
            return self._fail_closed(request, "ambiguous_timeout")
        except CompanyApiAvailabilityError:
            if self._fallback_enabled:
                self._logger.warning(
                    "%s local fallback "
                    "request_id=%s reason=availability",
                    self._profile.log_label,
                    _safe_request_id(request.request_id),
                )
                return fallback()
            return self._fail_closed(request, "availability")
        except CompanyApiPolicyError:
            return self._fail_closed(request, "policy")
        except CompanyApiContractError:
            return self._fail_closed(request, "contract")
        except CompanyApiClientError:
            return self._fail_closed(request, "client")
        except Exception:
            return self._fail_closed(request, "unexpected")

        validation = self._validate_remote_result(
            request,
            remote_result,
        )
        if validation.accepted:
            return remote_result
        if (
            validation.reason == "unexpected_route"
            and self._profile.fallback_on_unexpected_route
        ):
            self._logger.warning(
                "%s local fallback "
                "request_id=%s reason=unexpected_route",
                self._profile.log_label,
                _safe_request_id(request.request_id),
            )
            return fallback()
        # 허용 route가 requester DM을 지시하면 transport 계약 위반이므로
        # local 권한 경계로 우회하지 않고 안전한 실패로 닫는다.
        return self._fail_closed(
            request,
            validation.reason or "contract",
        )

    def _answer_local_with_progress(
        self,
        request: CompanyAssistantRequest,
        on_partial_result: Callable[[CompanyAssistantResult], None],
    ) -> CompanyAssistantResult | None:
        answer_with_progress = getattr(
            self._local_service,
            "answer_with_progress",
            None,
        )
        if callable(answer_with_progress):
            return answer_with_progress(request, on_partial_result)
        return self._local_service.answer(request)

    def _validate_remote_result(
        self,
        request: CompanyAssistantRequest,
        result: CompanyAssistantResult | None,
    ) -> _RemoteResultValidation:
        validation = self._profile.validate_result(result)
        if not validation.accepted:
            return validation

        expected_route = self._profile.expected_route
        if (
            expected_route is not None
            and result is not None
            and result.route != expected_route(request)
        ):
            # 같은 allowlist 안의 다른 DB route도 질문 분류 drift이므로
            # 응답하거나 local로 우회하지 않고 계약 오류로 닫는다.
            return _RemoteResultValidation(
                accepted=False,
                reason="route_mismatch",
            )
        return validation

    def _call_api(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        if self._api_client is None:
            raise CompanyApiAvailabilityError("client_not_configured")
        return self._api_client.answer(request)

    def _log_shadow_error(
        self,
        request: CompanyAssistantRequest,
        reason: str,
    ) -> None:
        self._logger.warning(
            "%s shadow failed request_id=%s reason=%s",
            self._profile.log_label,
            _safe_request_id(request.request_id),
            reason,
        )

    def _fail_closed(
        self,
        request: CompanyAssistantRequest,
        reason: str,
    ) -> CompanyAssistantResult:
        self._logger.warning(
            "%s failed closed request_id=%s reason=%s",
            self._profile.log_label,
            _safe_request_id(request.request_id),
            reason,
        )
        return CompanyAssistantResult(
            route=self._profile.failure_route,
            outcome="failed",
            messages=(
                AssistantMessage(
                    body=self._profile.failure_body,
                ),
            ),
            fallback_reason=f"company_api_{reason}",
        )


class CompanyNotionApiRolloutService(
    _CompanyApiRolloutService
):
    """회사 Notion route만 local/shadow/remote로 점진 전환한다."""

    def __init__(
        self,
        local_service: _LocalAssistantService,
        *,
        settings: CompanyApiClientSettings,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
        shadow_runner: _ShadowRunner,
    ) -> None:
        super().__init__(
            local_service,
            mode=settings.notion_mode,
            fallback_enabled=settings.notion_fallback_enabled,
            profile=_RolloutProfile(
                log_label="Company Notion API",
                allowed_routes=_ALLOWED_NOTION_ROUTES,
                matches_request=lambda request: (
                    _looks_like_company_notion_search(
                        request.question
                    )
                ),
                validate_result=_validate_remote_notion_result,
                failure_route="company_notion_search",
                failure_body=(
                    "회사 Notion 답변 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                fallback_on_unexpected_route=True,
            ),
            api_client=api_client,
            logger=logger,
            shadow_runner=shadow_runner,
        )


class CompanyStructuredApiRolloutService(
    _CompanyApiRolloutService
):
    """순수 DB 구조화 route만 공통 API로 점진 전환한다."""

    def __init__(
        self,
        local_service: _LocalAssistantService,
        *,
        settings: CompanyApiClientSettings,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
        shadow_runner: _ShadowRunner,
    ) -> None:
        super().__init__(
            local_service,
            mode=settings.structured_mode,
            fallback_enabled=(
                settings.structured_fallback_enabled
            ),
            profile=_RolloutProfile(
                log_label="Company Structured API",
                allowed_routes=_ALLOWED_STRUCTURED_ROUTES,
                matches_request=_matches_structured_api_request,
                validate_result=_validate_remote_structured_result,
                failure_route="structured_read",
                failure_body=(
                    "구조화 조회 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                expected_route=match_structured_read_route,
            ),
            api_client=api_client,
            logger=logger,
            shadow_runner=shadow_runner,
        )


class CompanyDeviceApiRolloutService(_CompanyApiRolloutService):
    """장비 접속 없이 끝나는 LED 조회·가이드만 공통 API로 전환한다."""

    def __init__(
        self,
        local_service: _LocalAssistantService,
        *,
        settings: CompanyApiClientSettings,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
        shadow_runner: _ShadowRunner,
    ) -> None:
        super().__init__(
            local_service,
            mode=settings.device_mode,
            fallback_enabled=settings.device_fallback_enabled,
            profile=_RolloutProfile(
                log_label="Company Device API",
                allowed_routes=_ALLOWED_DEVICE_ROUTES,
                matches_request=lambda request: (
                    match_device_read_route(request)
                    in _ALLOWED_DEVICE_ROUTES
                ),
                validate_result=_validate_remote_device_result,
                failure_route="device_read",
                failure_body=(
                    "장비 조회 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                expected_route=match_device_read_route,
            ),
            api_client=api_client,
            logger=logger,
            shadow_runner=shadow_runner,
        )


class CompanyDeviceFilterApiRolloutService(
    _CompanyApiRolloutService
):
    """MDA/SSH 보강을 제거한 장비 DB 조회만 공통 API로 전환한다."""

    def __init__(
        self,
        local_service: _LocalAssistantService,
        *,
        settings: CompanyApiClientSettings,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
        shadow_runner: _ShadowRunner,
    ) -> None:
        super().__init__(
            local_service,
            mode=settings.device_mode,
            fallback_enabled=settings.device_fallback_enabled,
            profile=_RolloutProfile(
                log_label="Company Device Filter API",
                allowed_routes=_ALLOWED_DEVICE_FILTER_ROUTES,
                matches_request=lambda request: (
                    match_structured_device_count_route(request)
                    == "devices_filter"
                ),
                validate_result=_validate_remote_device_filter_result,
                failure_route="devices_filter",
                failure_body=(
                    "장비 DB 조회 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                expected_route=match_structured_device_count_route,
            ),
            api_client=api_client,
            logger=logger,
            shadow_runner=shadow_runner,
        )


class CompanyRecordingFailureApiRolloutService(
    _CompanyApiRolloutService
):
    """녹화 실패 DB/S3 분석을 공통 API로 점진 전환한다."""

    def __init__(
        self,
        local_service: _LocalAssistantService,
        *,
        settings: CompanyApiClientSettings,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
        shadow_runner: _ShadowRunner,
    ) -> None:
        super().__init__(
            local_service,
            mode=settings.recording_failure_mode,
            fallback_enabled=(
                settings.recording_failure_fallback_enabled
            ),
            profile=_RolloutProfile(
                log_label="Company Recording Failure API",
                allowed_routes=_ALLOWED_RECORDING_FAILURE_ROUTES,
                matches_request=lambda request: (
                    match_recording_failure_route(request)
                    == "recording_failure_analysis"
                    and _has_explicit_log_date(request)
                ),
                validate_result=_validate_remote_recording_failure_result,
                failure_route="recording_failure_analysis",
                failure_body=(
                    "녹화 실패 분석 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                expected_route=match_recording_failure_route,
            ),
            api_client=api_client,
            logger=logger,
            shadow_runner=shadow_runner,
        )


class CompanyBarcodeLogApiRolloutService(
    _CompanyApiRolloutService
):
    """바코드 DB/S3 로그 분석을 공통 API로 점진 전환한다."""

    def __init__(
        self,
        local_service: _LocalAssistantService,
        *,
        settings: CompanyApiClientSettings,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
        shadow_runner: _ShadowRunner,
    ) -> None:
        super().__init__(
            local_service,
            mode=settings.barcode_log_mode,
            fallback_enabled=settings.barcode_log_fallback_enabled,
            profile=_RolloutProfile(
                log_label="Company Barcode Log API",
                allowed_routes=_ALLOWED_BARCODE_LOG_ROUTES,
                matches_request=lambda request: (
                    match_barcode_log_route(request)
                    == "barcode_log_analysis"
                    and _has_explicit_log_date(request)
                ),
                validate_result=_validate_remote_barcode_log_result,
                failure_route="barcode_log_analysis",
                failure_body=(
                    "바코드 로그 분석 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                expected_route=match_barcode_log_route,
            ),
            api_client=api_client,
            logger=logger,
            shadow_runner=shadow_runner,
        )


class CompanyBarcodeApiRolloutService(_CompanyApiRolloutService):
    """PII와 복원 mutation을 제외한 바코드 조회만 공통 API로 전환한다."""

    def __init__(
        self,
        local_service: _LocalAssistantService,
        *,
        settings: CompanyApiClientSettings,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
        shadow_runner: _ShadowRunner,
    ) -> None:
        super().__init__(
            local_service,
            mode=settings.barcode_mode,
            fallback_enabled=settings.barcode_fallback_enabled,
            profile=_RolloutProfile(
                log_label="Company Barcode API",
                allowed_routes=_ALLOWED_BARCODE_ROUTES,
                matches_request=lambda request: (
                    match_barcode_query_route(request)
                    in _ALLOWED_BARCODE_ROUTES
                ),
                validate_result=_validate_remote_barcode_result,
                failure_route="barcode_query",
                failure_body=(
                    "바코드 조회 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                expected_route=match_barcode_query_route,
            ),
            api_client=api_client,
            logger=logger,
            shadow_runner=shadow_runner,
        )


def wrap_company_notion_service(
    local_service: _LocalAssistantService,
    settings: CompanyApiClientSettings,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
    shadow_runner: _ShadowRunner | None = None,
) -> _LocalAssistantService:
    """local 객체를 유지하고 전환 모드에서만 decorator를 만든다."""

    if settings.notion_mode == "local":
        return local_service
    return CompanyNotionApiRolloutService(
        local_service,
        settings=settings,
        api_client=api_client,
        logger=logger,
        shadow_runner=shadow_runner or _DEFAULT_SHADOW_RUNNER,
    )


def wrap_company_structured_service(
    local_service: _LocalAssistantService,
    settings: CompanyApiClientSettings,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
    shadow_runner: _ShadowRunner | None = None,
) -> _LocalAssistantService:
    """장비 enrichment를 뺀 구조화 DB route만 decorator로 전환한다."""

    if settings.structured_mode == "local":
        return local_service
    return CompanyStructuredApiRolloutService(
        local_service,
        settings=settings,
        api_client=api_client,
        logger=logger,
        shadow_runner=shadow_runner or _DEFAULT_SHADOW_RUNNER,
    )


def wrap_company_device_service(
    local_service: _LocalAssistantService,
    settings: CompanyApiClientSettings,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
    shadow_runner: _ShadowRunner | None = None,
) -> _LocalAssistantService:
    """LED read-only route만 장비 전환 스위치로 감싼다."""

    if settings.device_mode == "local":
        return local_service
    return CompanyDeviceApiRolloutService(
        local_service,
        settings=settings,
        api_client=api_client,
        logger=logger,
        shadow_runner=shadow_runner or _DEFAULT_SHADOW_RUNNER,
    )


def wrap_company_device_filter_service(
    local_service: _LocalAssistantService,
    settings: CompanyApiClientSettings,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
    shadow_runner: _ShadowRunner | None = None,
) -> _LocalAssistantService:
    """structured stage의 devices_filter만 장비 전환 스위치로 감싼다."""

    if settings.device_mode == "local":
        return local_service
    return CompanyDeviceFilterApiRolloutService(
        local_service,
        settings=settings,
        api_client=api_client,
        logger=logger,
        shadow_runner=shadow_runner or _DEFAULT_SHADOW_RUNNER,
    )


def wrap_company_recording_failure_service(
    local_service: _LocalAssistantService,
    settings: CompanyApiClientSettings,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
    shadow_runner: _ShadowRunner | None = None,
) -> _LocalAssistantService:
    if settings.recording_failure_mode == "local":
        return local_service
    return CompanyRecordingFailureApiRolloutService(
        local_service,
        settings=settings,
        api_client=api_client,
        logger=logger,
        shadow_runner=shadow_runner or _DEFAULT_SHADOW_RUNNER,
    )


def wrap_company_barcode_log_service(
    local_service: _LocalAssistantService,
    settings: CompanyApiClientSettings,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
    shadow_runner: _ShadowRunner | None = None,
) -> _LocalAssistantService:
    if settings.barcode_log_mode == "local":
        return local_service
    return CompanyBarcodeLogApiRolloutService(
        local_service,
        settings=settings,
        api_client=api_client,
        logger=logger,
        shadow_runner=shadow_runner or _DEFAULT_SHADOW_RUNNER,
    )


def wrap_company_barcode_service(
    local_service: _LocalAssistantService,
    settings: CompanyApiClientSettings,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
    shadow_runner: _ShadowRunner | None = None,
) -> _LocalAssistantService:
    if settings.barcode_mode == "local":
        return local_service
    return CompanyBarcodeApiRolloutService(
        local_service,
        settings=settings,
        api_client=api_client,
        logger=logger,
        shadow_runner=shadow_runner or _DEFAULT_SHADOW_RUNNER,
    )


def _is_allowed_route(
    result: CompanyAssistantResult | None,
    allowed_routes: frozenset[str],
) -> bool:
    return bool(
        result is not None
        and result.route in allowed_routes
    )


def _has_explicit_log_date(
    request: CompanyAssistantRequest,
) -> bool:
    """초기 S3 전환은 장기간 자동 탐색 없이 날짜가 있는 요청으로 제한한다."""

    try:
        _, has_requested_date = _extract_log_date_with_presence(
            request.question
        )
    except ValueError:
        return False
    return has_requested_date


def _is_shadow_eligible_result(
    result: CompanyAssistantResult | None,
    allowed_routes: frozenset[str],
) -> bool:
    # local actor 정책에서 거부한 질문은 API allowlist가 drift했더라도
    # shadow 조회나 LLM 실행을 시작하지 않는다.
    return bool(
        _is_allowed_route(result, allowed_routes)
        and result is not None
        and result.outcome != "denied"
    )


def _validate_remote_notion_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    base_validation = _validate_remote_result_base(
        result,
        _ALLOWED_NOTION_ROUTES,
    )
    if not base_validation.accepted:
        return base_validation
    assert result is not None
    if any(
        not _is_notion_source_uri(source.uri)
        for source in result.sources
    ):
        return _RemoteResultValidation(
            accepted=False,
            reason="unsafe_source_host",
        )
    return _RemoteResultValidation(accepted=True)


def _matches_structured_api_request(
    request: CompanyAssistantRequest,
) -> bool:
    # 기존 네 route만 structured 스위치가 담당하고, DB-only 장비 개수는
    # 별도 device 스위치가 담당해 상세/status와 섞이지 않게 한다.
    return (
        match_structured_read_route(request)
        in _ALLOWED_STRUCTURED_ROUTES
    )


def _validate_remote_structured_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    base_validation = _validate_remote_result_base(
        result,
        _ALLOWED_STRUCTURED_ROUTES,
    )
    if not base_validation.accepted:
        return base_validation
    assert result is not None
    if result.sources:
        return _RemoteResultValidation(
            accepted=False,
            reason="unexpected_sources",
        )
    if result.used_llm:
        return _RemoteResultValidation(
            accepted=False,
            reason="unexpected_llm",
        )
    return _RemoteResultValidation(accepted=True)


def _validate_remote_device_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    base_validation = _validate_remote_result_base(
        result,
        _ALLOWED_DEVICE_ROUTES,
    )
    if not base_validation.accepted:
        return base_validation
    assert result is not None
    if result.route == "device_led_log_analysis":
        if result.sources:
            return _RemoteResultValidation(False, "unexpected_sources")
        if result.used_llm:
            return _RemoteResultValidation(False, "unexpected_llm")
        return _RemoteResultValidation(True)
    if any(
        not _is_notion_source_uri(source.uri)
        for source in result.sources
    ):
        return _RemoteResultValidation(False, "unsafe_source_host")
    return _RemoteResultValidation(True)


def _validate_remote_device_filter_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    return _validate_remote_deterministic_result(
        result,
        _ALLOWED_DEVICE_FILTER_ROUTES,
    )


def _validate_remote_recording_failure_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    return _validate_remote_analysis_result(
        result,
        _ALLOWED_RECORDING_FAILURE_ROUTES,
    )


def _validate_remote_barcode_log_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    return _validate_remote_analysis_result(
        result,
        _ALLOWED_BARCODE_LOG_ROUTES,
    )


def _validate_remote_barcode_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    return _validate_remote_deterministic_result(
        result,
        _ALLOWED_BARCODE_ROUTES,
    )


def _combine_progress_results(
    partial_results: list[CompanyAssistantResult],
    final_result: CompanyAssistantResult | None,
) -> CompanyAssistantResult | None:
    """부분·최종 메시지를 API의 단일 전체 결과와 비교할 형태로 합친다."""

    if not partial_results:
        return final_result
    base_result = final_result or partial_results[-1]
    messages = tuple(
        message
        for result in partial_results
        for message in result.messages
    ) + tuple(final_result.messages if final_result is not None else ())
    sources = tuple(
        source
        for result in (*partial_results, *((final_result,) if final_result else ()))
        for source in result.sources
    )
    return replace(
        base_result,
        messages=messages,
        sources=sources,
        used_llm=bool(
            base_result.used_llm
            or any(result.used_llm for result in partial_results)
        ),
    )


def _validate_remote_analysis_result(
    result: CompanyAssistantResult | None,
    allowed_routes: frozenset[str],
) -> _RemoteResultValidation:
    base_validation = _validate_remote_result_base(
        result,
        allowed_routes,
    )
    if not base_validation.accepted:
        return base_validation
    assert result is not None
    if result.sources:
        return _RemoteResultValidation(False, "unexpected_sources")
    return _RemoteResultValidation(True)


def _validate_remote_deterministic_result(
    result: CompanyAssistantResult | None,
    allowed_routes: frozenset[str],
) -> _RemoteResultValidation:
    base_validation = _validate_remote_analysis_result(
        result,
        allowed_routes,
    )
    if not base_validation.accepted:
        return base_validation
    assert result is not None
    if result.used_llm:
        return _RemoteResultValidation(False, "unexpected_llm")
    return _RemoteResultValidation(True)


def _validate_remote_result_base(
    result: CompanyAssistantResult | None,
    allowed_routes: frozenset[str],
) -> _RemoteResultValidation:
    if not _is_allowed_route(result, allowed_routes):
        # API가 다른 정책 route에서 명시적으로 거부한 결과는 local
        # route로 우회하지 않는다.
        if result is not None and result.outcome == "denied":
            return _RemoteResultValidation(
                accepted=False,
                reason="policy",
            )
        return _RemoteResultValidation(
            accepted=False,
            reason="unexpected_route",
        )
    assert result is not None
    if any(
        message.delivery_scope != "conversation"
        for message in result.messages
    ):
        return _RemoteResultValidation(
            accepted=False,
            reason="unsafe_message_scope",
        )
    if (
        result.suggested_action is not None
        or result.async_job is not None
    ):
        return _RemoteResultValidation(
            accepted=False,
            reason="unsafe_action",
        )
    return _RemoteResultValidation(accepted=True)


def _source_set(
    result: CompanyAssistantResult,
) -> frozenset[tuple[str, str, str, float | None]]:
    # 실제 source 식별자는 비교에만 쓰고 observability 로그에는 넣지 않는다.
    return frozenset(
        (
            source.source_id,
            source.title,
            source.uri,
            source.score,
        )
        for source in result.sources
    )


def _message_scopes(
    result: CompanyAssistantResult,
) -> tuple[tuple[str, bool, str], ...]:
    # HTTP의 길이 windowing으로 생긴 연속 chunk는 하나의 의미 메시지로
    # 합치고, 원문 없이 전달 범위·멘션·포맷만 비교한다.
    return tuple(
        (scope, mention_actor, message_format)
        for scope, mention_actor, message_format, _body in (
            _group_semantic_messages(result)
        )
    )


def _message_body_digests(
    result: CompanyAssistantResult,
) -> tuple[str, ...]:
    # transport chunk 경계는 무시하고 의미 단위 digest만 남긴다.
    return tuple(
        hashlib.sha256(body.encode("utf-8")).hexdigest()
        for _scope, _mention, _format, body in (
            _group_semantic_messages(result)
        )
    )


def _group_semantic_messages(
    result: CompanyAssistantResult,
) -> list[tuple[str, bool, str, str]]:
    grouped: list[tuple[str, bool, str, str]] = []
    for message in result.messages:
        if (
            grouped
            and grouped[-1][0] == message.delivery_scope
            and grouped[-1][2] == message.format
        ):
            scope, mention_actor, message_format, body = grouped[-1]
            grouped[-1] = (
                scope,
                mention_actor or message.mention_actor,
                message_format,
                body + message.body,
            )
        else:
            grouped.append(
                (
                    message.delivery_scope,
                    message.mention_actor,
                    message.format,
                    message.body,
                )
            )
    return grouped


def _is_notion_source_uri(value: object) -> bool:
    try:
        parsed = urlsplit(str(value or ""))
    except ValueError:
        return False
    return bool(
        parsed.scheme.lower() == "https"
        and parsed.hostname is not None
        and parsed.hostname.rstrip(".").lower()
        in _NOTION_SOURCE_HOSTS
        and parsed.username is None
        and parsed.password is None
    )


def _safe_request_id(value: object) -> str:
    normalized = str(value or "").strip()
    if _SAFE_REQUEST_ID_PATTERN.fullmatch(normalized):
        return normalized
    return "unavailable"


__all__ = [
    "BoundedShadowRunner",
    "CompanyBarcodeApiRolloutService",
    "CompanyBarcodeLogApiRolloutService",
    "CompanyDeviceApiRolloutService",
    "CompanyDeviceFilterApiRolloutService",
    "CompanyNotionApiRolloutService",
    "CompanyRecordingFailureApiRolloutService",
    "CompanyStructuredApiRolloutService",
    "wrap_company_barcode_log_service",
    "wrap_company_barcode_service",
    "wrap_company_device_filter_service",
    "wrap_company_device_service",
    "wrap_company_notion_service",
    "wrap_company_recording_failure_service",
    "wrap_company_structured_service",
]
