from __future__ import annotations

from dataclasses import dataclass, replace
import logging
import re
from typing import Callable, Protocol
from urllib.parse import urlsplit

from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.read_routing import (
    BARCODE_TIMELINE_ROUTES,
    _looks_like_company_notion_search,
    is_safe_baby_magic_source_uri,
    match_barcode_evidence_freeform_route,
    match_barcode_log_route,
    match_barcode_query_route,
    match_barcode_timeline_route,
    match_common_api_barcode_query_route,
    match_company_freeform_route,
    match_device_detail_route,
    match_device_read_route,
    match_notion_playbook_route,
    match_recording_failure_route,
    match_structured_device_count_route,
    match_structured_read_route,
    match_usage_help_rollout_route,
    match_weekly_recordings_summary_route,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiAmbiguousTimeoutError,
    CompanyApiAvailabilityError,
    CompanyApiClientError,
    CompanyApiContractError,
    CompanyApiPolicyError,
    CompanyAssistantApiClient,
)


_ALLOWED_NOTION_ROUTES = frozenset(
    {"company_notion_search", "company_notion_qa"}
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
    {"device_led_log_analysis", "device_led_pattern_guide"}
)
_ALLOWED_DEVICE_FILTER_ROUTES = frozenset({"devices_filter"})
_ALLOWED_DEVICE_DETAIL_ROUTES = frozenset(
    {"device_detail", "devices_filter"}
)
_ALLOWED_WEEKLY_SUMMARY_ROUTES = frozenset(
    {"weekly_recordings_summary"}
)
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
_ALLOWED_BARCODE_RESIDUAL_ROUTES = frozenset(
    {"baby_ai_list", "barcode_baby_ai_list"}
)
_ALLOWED_BARCODE_TIMELINE_ROUTES = BARCODE_TIMELINE_ROUTES
_ALLOWED_PLAYBOOK_ROUTES = frozenset({"notion_playbook_qa"})
_ALLOWED_BARCODE_FREEFORM_ROUTES = frozenset(
    {"barcode_evidence_freeform"}
)
_ALLOWED_USAGE_HELP_ROUTES = frozenset({"usage_help"})
_ALLOWED_FREEFORM_ROUTES = frozenset({"company_freeform"})
_SAFE_REQUEST_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$"
)
_NOTION_SOURCE_HOSTS = frozenset(
    {"app.notion.com", "notion.so", "www.notion.so"}
)


def _match_company_freeform_rollout(
    request: CompanyAssistantRequest,
) -> str | None:
    """Slack 원문을 실행 없이 final freeform stage로 좁혀 재분류한다."""

    metadata = dict(request.metadata)
    metadata["route_group"] = "freeform"
    return match_company_freeform_route(
        replace(request, metadata=metadata)
    )


class _AssistantService(Protocol):
    def answer(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None: ...


@dataclass(frozen=True, slots=True)
class _RemoteResultValidation:
    accepted: bool
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class _RemoteProfile:
    log_label: str
    matches_request: Callable[[CompanyAssistantRequest], bool]
    validate_result: Callable[
        [CompanyAssistantResult | None],
        _RemoteResultValidation,
    ]
    failure_route: str
    failure_body: str
    route_group: str
    expected_route: (
        Callable[[CompanyAssistantRequest], str | None] | None
    ) = None


class _CompanyApiRemoteService:
    """순수 matcher가 선택한 요청만 API로 보내고 결과 계약을 검증한다."""

    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        profile: _RemoteProfile,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        self._next_service = next_service
        self._profile = profile
        self._api_client = api_client
        self._logger = logger

    def answer(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        # matcher 불일치는 다음 remote stage에만 넘긴다. Slack-local domain
        # handler를 실행하는 fallback 경로는 이 wrapper에 존재하지 않는다.
        if not self._profile.matches_request(request):
            return self._answer_next(request)
        return self._answer_remote(request)

    def answer_with_progress(
        self,
        request: CompanyAssistantRequest,
        on_partial_result: Callable[[CompanyAssistantResult], None],
    ) -> CompanyAssistantResult | None:
        if not self._profile.matches_request(request):
            if self._next_service is None:
                return None
            answer_with_progress = getattr(
                self._next_service,
                "answer_with_progress",
                None,
            )
            if callable(answer_with_progress):
                return answer_with_progress(
                    request,
                    on_partial_result,
                )
            return self._next_service.answer(request)

        def validate_and_forward(
            partial_result: CompanyAssistantResult,
        ) -> None:
            validation = self._validate_remote_result(
                request,
                partial_result,
            )
            if not validation.accepted:
                raise CompanyApiContractError(
                    "company_api_progress_result_invalid",
                    request_id=request.request_id,
                )
            on_partial_result(partial_result)

        try:
            if self._api_client is None:
                raise CompanyApiAvailabilityError(
                    "company_api_client_not_configured"
                )
            result = self._api_client.answer_with_progress(
                request,
                route_group=self._profile.route_group,
                on_partial_result=validate_and_forward,
            )
        except CompanyApiAmbiguousTimeoutError:
            return self._fail_closed(request, "ambiguous_timeout")
        except CompanyApiAvailabilityError:
            return self._fail_closed(request, "availability")
        except CompanyApiPolicyError:
            return self._fail_closed(request, "policy")
        except CompanyApiContractError:
            return self._fail_closed(request, "contract")
        except CompanyApiClientError:
            return self._fail_closed(request, "client")
        except Exception:
            return self._fail_closed(request, "unexpected")

        validation = self._validate_remote_result(request, result)
        if validation.accepted:
            return result
        return self._fail_closed(
            request,
            validation.reason or "contract",
        )

    def _answer_next(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        if self._next_service is None:
            return None
        return self._next_service.answer(request)

    def _answer_remote(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult:
        try:
            if self._api_client is None:
                raise CompanyApiAvailabilityError(
                    "company_api_client_not_configured"
                )
            result = self._api_client.answer(
                request,
                route_group=self._profile.route_group,
            )
        except CompanyApiAmbiguousTimeoutError:
            return self._fail_closed(request, "ambiguous_timeout")
        except CompanyApiAvailabilityError:
            return self._fail_closed(request, "availability")
        except CompanyApiPolicyError:
            return self._fail_closed(request, "policy")
        except CompanyApiContractError:
            return self._fail_closed(request, "contract")
        except CompanyApiClientError:
            return self._fail_closed(request, "client")
        except Exception:
            return self._fail_closed(request, "unexpected")

        validation = self._validate_remote_result(request, result)
        if validation.accepted:
            assert result is not None
            return result
        return self._fail_closed(
            request,
            validation.reason or "contract",
        )

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
            return _RemoteResultValidation(False, "route_mismatch")
        return validation

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
                AssistantMessage(body=self._profile.failure_body),
            ),
            fallback_reason=f"company_api_{reason}",
        )


class CompanyNotionApiRolloutService(_CompanyApiRemoteService):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Notion API",
                matches_request=lambda request: (
                    _looks_like_company_notion_search(request.question)
                ),
                validate_result=_validate_remote_notion_result,
                failure_route="company_notion_search",
                failure_body=(
                    "회사 Notion 답변 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                route_group="notion",
            ),
            api_client=api_client,
            logger=logger,
        )


class CompanyStructuredApiRolloutService(_CompanyApiRemoteService):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Structured API",
                matches_request=_matches_structured_api_request,
                validate_result=_validate_remote_structured_result,
                failure_route="structured_read",
                failure_body=(
                    "구조화 조회 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                route_group="structured",
                expected_route=match_structured_read_route,
            ),
            api_client=api_client,
            logger=logger,
        )


class CompanyDeviceApiRolloutService(_CompanyApiRemoteService):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Device API",
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
                route_group="device",
                expected_route=match_device_read_route,
            ),
            api_client=api_client,
            logger=logger,
        )


class CompanyDeviceFilterApiRolloutService(
    _CompanyApiRemoteService
):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Device Filter API",
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
                route_group="structured",
                expected_route=match_structured_device_count_route,
            ),
            api_client=api_client,
            logger=logger,
        )


class CompanyDeviceDbDetailApiRolloutService(
    _CompanyApiRemoteService
):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Device Detail API",
                matches_request=lambda request: (
                    match_device_detail_route(request)
                    in _ALLOWED_DEVICE_DETAIL_ROUTES
                ),
                validate_result=_validate_remote_device_detail_result,
                failure_route="device_detail",
                failure_body=(
                    "장비 상세 조회 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                route_group="device_detail",
                expected_route=match_device_detail_route,
            ),
            api_client=api_client,
            logger=logger,
        )


class CompanyWeeklySummaryApiRolloutService(
    _CompanyApiRemoteService
):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Weekly Summary API",
                matches_request=lambda request: (
                    match_weekly_recordings_summary_route(request)
                    == "weekly_recordings_summary"
                ),
                validate_result=_validate_remote_weekly_summary_result,
                failure_route="weekly_recordings_summary",
                failure_body=(
                    "주간 영상 현황 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                route_group="structured",
                expected_route=match_weekly_recordings_summary_route,
            ),
            api_client=api_client,
            logger=logger,
        )


class CompanyRecordingFailureApiRolloutService(
    _CompanyApiRemoteService
):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Recording Failure API",
                matches_request=lambda request: (
                    match_recording_failure_route(request)
                    == "recording_failure_analysis"
                ),
                validate_result=_validate_remote_recording_failure_result,
                failure_route="recording_failure_analysis",
                failure_body=(
                    "녹화 실패 분석 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                route_group="failure",
                expected_route=match_recording_failure_route,
            ),
            api_client=api_client,
            logger=logger,
        )


class CompanyBarcodeLogApiRolloutService(
    _CompanyApiRemoteService
):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Barcode Log API",
                matches_request=lambda request: (
                    match_barcode_log_route(request)
                    == "barcode_log_analysis"
                ),
                validate_result=_validate_remote_barcode_log_result,
                failure_route="barcode_log_analysis",
                failure_body=(
                    "바코드 로그 분석 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                route_group="log",
                expected_route=match_barcode_log_route,
            ),
            api_client=api_client,
            logger=logger,
        )


class CompanyBarcodeApiRolloutService(_CompanyApiRemoteService):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Barcode API",
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
                route_group="barcode",
                expected_route=match_barcode_query_route,
            ),
            api_client=api_client,
            logger=logger,
        )


class CompanyPlaybookApiRolloutService(
    _CompanyApiRemoteService
):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Playbook API",
                matches_request=lambda request: (
                    match_notion_playbook_route(request)
                    == "notion_playbook_qa"
                ),
                validate_result=_validate_remote_playbook_result,
                failure_route="notion_playbook_qa",
                failure_body=(
                    "운영 문서 답변 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                route_group="knowledge",
                expected_route=match_notion_playbook_route,
            ),
            api_client=api_client,
            logger=logger,
        )


class CompanyBarcodeFreeformApiRolloutService(
    _CompanyApiRemoteService
):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Barcode Freeform API",
                matches_request=lambda request: (
                    match_barcode_evidence_freeform_route(request)
                    == "barcode_evidence_freeform"
                ),
                validate_result=_validate_remote_barcode_freeform_result,
                failure_route="barcode_evidence_freeform",
                failure_body=(
                    "바코드 녹화 근거 답변 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                route_group="knowledge",
                expected_route=match_barcode_evidence_freeform_route,
            ),
            api_client=api_client,
            logger=logger,
        )


class CompanyFreeformApiRolloutService(
    _CompanyApiRemoteService
):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Freeform API",
                matches_request=lambda request: (
                    _match_company_freeform_rollout(request)
                    == "company_freeform"
                ),
                validate_result=_validate_remote_freeform_result,
                failure_route="company_freeform",
                failure_body=(
                    "일반 답변 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                route_group="freeform",
                expected_route=_match_company_freeform_rollout,
            ),
            api_client=api_client,
            logger=logger,
        )

    def answer(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        # final freeform은 앞선 remote knowledge chain이 답하지 않았을 때만
        # 호출한다. local LLM fallback은 존재하지 않는다.
        previous_result = self._answer_next(request)
        if previous_result is not None:
            return previous_result
        if not self._profile.matches_request(request):
            return None
        return self._answer_remote(request)


class CompanyUsageHelpApiRolloutService(
    _CompanyApiRemoteService
):
    """사용법 문구도 회사 API 정본에서 받아 Slack은 렌더링만 한다."""

    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Usage Help API",
                matches_request=lambda request: (
                    match_usage_help_rollout_route(request)
                    == "usage_help"
                ),
                validate_result=_validate_remote_usage_help_result,
                failure_route="usage_help",
                failure_body=(
                    "사용법 안내 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                route_group="freeform",
                expected_route=match_usage_help_rollout_route,
            ),
            api_client=api_client,
            logger=logger,
        )


class CompanyBarcodeResidualApiRolloutService(
    _CompanyApiRemoteService
):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Barcode Residual API",
                matches_request=lambda request: (
                    match_common_api_barcode_query_route(request)
                    in _ALLOWED_BARCODE_RESIDUAL_ROUTES
                ),
                validate_result=_validate_remote_barcode_residual_result,
                failure_route="barcode_query",
                failure_body=(
                    "바코드 추가 조회 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                route_group="barcode",
                expected_route=match_common_api_barcode_query_route,
            ),
            api_client=api_client,
            logger=logger,
        )


class CompanyBarcodeTimelineApiRolloutService(
    _CompanyApiRemoteService
):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
    ) -> None:
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Barcode Timeline API",
                matches_request=lambda request: (
                    match_barcode_timeline_route(request)
                    in _ALLOWED_BARCODE_TIMELINE_ROUTES
                ),
                validate_result=_validate_remote_barcode_timeline_result,
                failure_route="barcode_timeline",
                failure_body=(
                    "바코드 녹화 시점 조회 서비스 상태를 확인할 수 없어. "
                    "잠시 후 다시 시도해줘"
                ),
                route_group="barcode",
                expected_route=match_barcode_timeline_route,
            ),
            api_client=api_client,
            logger=logger,
        )


class CompanyOperationsApiRolloutService(
    _CompanyApiRemoteService
):
    def __init__(
        self,
        next_service: _AssistantService | None,
        *,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
        matcher: Callable[[CompanyAssistantRequest], str | None],
        allowed_routes: frozenset[str],
    ) -> None:
        normalized_routes = _normalize_operation_routes(allowed_routes)
        super().__init__(
            next_service,
            profile=_RemoteProfile(
                log_label="Company Operations API",
                matches_request=lambda request: (
                    matcher(request) in normalized_routes
                ),
                validate_result=lambda result: (
                    _validate_remote_operation_result(
                        result,
                        normalized_routes,
                    )
                ),
                failure_route="operations",
                failure_body=(
                    "작업 요청 상태를 확인할 수 없어. "
                    "작업을 다시 실행하지 말고 운영자에게 확인해줘"
                ),
                route_group="operations",
                expected_route=matcher,
            ),
            api_client=api_client,
            logger=logger,
        )


def wrap_company_notion_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyNotionApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_structured_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyStructuredApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_device_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyDeviceApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_device_filter_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyDeviceFilterApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_device_db_detail_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyDeviceDbDetailApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_weekly_summary_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyWeeklySummaryApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_recording_failure_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyRecordingFailureApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_barcode_log_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyBarcodeLogApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_barcode_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyBarcodeApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_playbook_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyPlaybookApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_barcode_freeform_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyBarcodeFreeformApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_freeform_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyFreeformApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_usage_help_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyUsageHelpApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_barcode_residual_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyBarcodeResidualApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_barcode_timeline_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
) -> _AssistantService:
    return CompanyBarcodeTimelineApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
    )


def wrap_company_operations_service(
    next_service: _AssistantService | None,
    api_client: CompanyAssistantApiClient | None,
    logger: logging.Logger,
    matcher: Callable[[CompanyAssistantRequest], str | None],
    allowed_routes: frozenset[str],
) -> _AssistantService:
    return CompanyOperationsApiRolloutService(
        next_service,
        api_client=api_client,
        logger=logger,
        matcher=matcher,
        allowed_routes=allowed_routes,
    )


def _is_allowed_route(
    result: CompanyAssistantResult | None,
    allowed_routes: frozenset[str],
) -> bool:
    return bool(
        result is not None and result.route in allowed_routes
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
        return _RemoteResultValidation(False, "unsafe_source_host")
    return _RemoteResultValidation(True)


def _matches_structured_api_request(
    request: CompanyAssistantRequest,
) -> bool:
    return (
        match_structured_read_route(request)
        in _ALLOWED_STRUCTURED_ROUTES
    )


def _validate_remote_structured_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    return _validate_remote_deterministic_result(
        result,
        _ALLOWED_STRUCTURED_ROUTES,
    )


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


def _validate_remote_device_detail_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    return _validate_remote_deterministic_result(
        result,
        _ALLOWED_DEVICE_DETAIL_ROUTES,
    )


def _validate_remote_operation_result(
    result: CompanyAssistantResult | None,
    allowed_routes: frozenset[str],
) -> _RemoteResultValidation:
    if not _is_allowed_route(result, allowed_routes):
        if result is not None and result.outcome == "denied":
            return _RemoteResultValidation(False, "policy")
        return _RemoteResultValidation(False, "unexpected_route")
    assert result is not None
    # operations는 requester DM과 typed activity receipt를 정상 계약으로
    # 허용하고 Slack adapter가 전달 완료 뒤 exact ACK만 수행한다.
    return _RemoteResultValidation(True)


def _validate_remote_weekly_summary_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    return _validate_remote_deterministic_result(
        result,
        _ALLOWED_WEEKLY_SUMMARY_ROUTES,
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


def _validate_remote_playbook_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    base_validation = _validate_remote_result_base(
        result,
        _ALLOWED_PLAYBOOK_ROUTES,
    )
    if not base_validation.accepted:
        return base_validation
    assert result is not None
    if any(
        not _is_notion_source_uri(source.uri)
        for source in result.sources
    ):
        return _RemoteResultValidation(False, "unsafe_source_host")
    return _RemoteResultValidation(True)


def _validate_remote_barcode_freeform_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    return _validate_remote_analysis_result(
        result,
        _ALLOWED_BARCODE_FREEFORM_ROUTES,
    )


def _validate_remote_freeform_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    return _validate_remote_analysis_result(
        result,
        _ALLOWED_FREEFORM_ROUTES,
    )


def _validate_remote_usage_help_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    base_validation = _validate_remote_deterministic_result(
        result,
        _ALLOWED_USAGE_HELP_ROUTES,
    )
    if not base_validation.accepted:
        return base_validation
    assert result is not None
    if (
        result.outcome != "answered"
        or len(result.messages) != 1
        or result.messages[0].mention_actor
        or result.messages[0].private_links
        or not result.messages[0].body.strip()
        or result.fallback_reason is not None
        or result.operation_result is not None
    ):
        # 도움말은 정적 conversation 문구 하나뿐이다. API가 다른 전달
        # 의미를 섞으면 Slack에서 해석하지 않고 계약 오류로 닫는다.
        return _RemoteResultValidation(False, "invalid_presentation")
    return _RemoteResultValidation(True)


def _validate_remote_barcode_residual_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    base_validation = _validate_remote_result_base(
        result,
        _ALLOWED_BARCODE_RESIDUAL_ROUTES,
    )
    if not base_validation.accepted:
        return base_validation
    assert result is not None
    if result.used_llm:
        return _RemoteResultValidation(False, "unexpected_llm")
    if result.route != "barcode_baby_ai_list" and result.sources:
        return _RemoteResultValidation(False, "unexpected_sources")
    if any(
        not is_safe_baby_magic_source_uri(source.uri)
        for source in result.sources
    ):
        return _RemoteResultValidation(False, "unsafe_source_host")
    return _RemoteResultValidation(True)


def _validate_remote_barcode_timeline_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    return _validate_remote_analysis_result(
        result,
        _ALLOWED_BARCODE_TIMELINE_ROUTES,
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
        if result is not None and result.outcome == "denied":
            return _RemoteResultValidation(False, "policy")
        return _RemoteResultValidation(False, "unexpected_route")
    assert result is not None
    if any(
        message.delivery_scope != "conversation"
        for message in result.messages
    ):
        return _RemoteResultValidation(False, "unsafe_message_scope")
    return _RemoteResultValidation(True)


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


def _normalize_operation_routes(
    routes: frozenset[str],
) -> frozenset[str]:
    normalized = frozenset(
        route.strip()
        for route in routes
        if isinstance(route, str) and route.strip()
    )
    if not normalized or len(normalized) != len(routes):
        raise CompanyApiContractError(
            "company_api_operations_routes_invalid"
        )
    return normalized


__all__ = [
    "CompanyBarcodeApiRolloutService",
    "CompanyBarcodeFreeformApiRolloutService",
    "CompanyBarcodeLogApiRolloutService",
    "CompanyBarcodeResidualApiRolloutService",
    "CompanyBarcodeTimelineApiRolloutService",
    "CompanyDeviceApiRolloutService",
    "CompanyDeviceDbDetailApiRolloutService",
    "CompanyDeviceFilterApiRolloutService",
    "CompanyFreeformApiRolloutService",
    "CompanyNotionApiRolloutService",
    "CompanyOperationsApiRolloutService",
    "CompanyPlaybookApiRolloutService",
    "CompanyRecordingFailureApiRolloutService",
    "CompanyStructuredApiRolloutService",
    "CompanyUsageHelpApiRolloutService",
    "CompanyWeeklySummaryApiRolloutService",
    "wrap_company_barcode_freeform_service",
    "wrap_company_barcode_log_service",
    "wrap_company_barcode_residual_service",
    "wrap_company_barcode_timeline_service",
    "wrap_company_barcode_service",
    "wrap_company_device_db_detail_service",
    "wrap_company_device_filter_service",
    "wrap_company_device_service",
    "wrap_company_freeform_service",
    "wrap_company_notion_service",
    "wrap_company_operations_service",
    "wrap_company_playbook_service",
    "wrap_company_recording_failure_service",
    "wrap_company_structured_service",
    "wrap_company_usage_help_service",
    "wrap_company_weekly_summary_service",
]
