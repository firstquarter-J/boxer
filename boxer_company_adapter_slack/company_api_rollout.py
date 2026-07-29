from __future__ import annotations

from dataclasses import dataclass
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


class CompanyNotionApiRolloutService:
    """회사 Notion 한 route만 local/shadow/remote로 점진 전환한다."""

    def __init__(
        self,
        local_service: _LocalAssistantService,
        *,
        settings: CompanyApiClientSettings,
        api_client: CompanyAssistantApiClient | None,
        logger: logging.Logger,
        shadow_runner: _ShadowRunner,
    ) -> None:
        self._local_service = local_service
        self._settings = settings
        self._api_client = api_client
        self._logger = logger
        self._shadow_runner = shadow_runner

    def answer(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        # 순수 matcher를 먼저 적용해 다른 read-only route나 mutation 문장을
        # 전체-stage HTTP endpoint가 선점하지 못하게 한다.
        if not _looks_like_company_notion_search(request.question):
            return self._local_service.answer(request)

        mode = self._settings.notion_mode
        if mode == "shadow":
            return self._answer_shadow(request)
        if mode == "remote":
            return self._answer_remote(request)
        return self._local_service.answer(request)

    def _answer_shadow(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        local_result = self._local_service.answer(request)
        if not _is_shadow_eligible_notion_result(local_result):
            return local_result

        def compare_remote() -> None:
            self._compare_shadow_result(request, local_result)

        try:
            accepted = self._shadow_runner.submit(compare_remote)
        except Exception as exc:
            self._logger.warning(
                "Company Notion API shadow submission failed "
                "request_id=%s error_type=%s",
                _safe_request_id(request.request_id),
                type(exc).__name__,
            )
            return local_result
        # 테스트용 inline submitter는 반환값이 없을 수 있어 명시적 False만
        # capacity 거부로 해석한다.
        if accepted is False:
            self._logger.warning(
                "Company Notion API shadow skipped request_id=%s reason=capacity",
                _safe_request_id(request.request_id),
            )
        # shadow 결과는 renderer로 절대 반환하지 않아 Slack 중복 응답을 막는다.
        return local_result

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

        validation = _validate_remote_notion_result(remote_result)
        if not validation.accepted:
            self._logger.info(
                "Company Notion API shadow comparison "
                "request_id=%s accepted=false reason=%s",
                _safe_request_id(request.request_id),
                validation.reason or "unknown",
            )
            return

        assert remote_result is not None
        self._logger.info(
            "Company Notion API shadow comparison "
            "request_id=%s accepted=true "
            "route_match=%s outcome_match=%s fallback_match=%s "
            "used_llm_match=%s source_set_match=%s "
            "message_scope_match=%s "
            "local_source_count=%s remote_source_count=%s "
            "local_message_count=%s remote_message_count=%s",
            _safe_request_id(request.request_id),
            local_result.route == remote_result.route,
            local_result.outcome == remote_result.outcome,
            local_result.fallback_reason
            == remote_result.fallback_reason,
            local_result.used_llm == remote_result.used_llm,
            _source_set(local_result) == _source_set(remote_result),
            _message_scopes(local_result)
            == _message_scopes(remote_result),
            len(local_result.sources),
            len(remote_result.sources),
            len(local_result.messages),
            len(remote_result.messages),
        )

    def _answer_remote(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        try:
            remote_result = self._call_api(request)
        except CompanyApiAmbiguousTimeoutError:
            return self._fail_closed(request, "ambiguous_timeout")
        except CompanyApiAvailabilityError:
            if self._settings.notion_fallback_enabled:
                self._logger.warning(
                    "Company Notion API local fallback "
                    "request_id=%s reason=availability",
                    _safe_request_id(request.request_id),
                )
                return self._local_service.answer(request)
            return self._fail_closed(request, "availability")
        except CompanyApiPolicyError:
            return self._fail_closed(request, "policy")
        except CompanyApiContractError:
            return self._fail_closed(request, "contract")
        except CompanyApiClientError:
            return self._fail_closed(request, "client")
        except Exception:
            return self._fail_closed(request, "unexpected")

        validation = _validate_remote_notion_result(remote_result)
        if validation.accepted:
            return remote_result
        if validation.reason == "unexpected_route":
            self._logger.warning(
                "Company Notion API local fallback "
                "request_id=%s reason=unexpected_route",
                _safe_request_id(request.request_id),
            )
            return self._local_service.answer(request)
        # 허용 route가 requester DM을 지시하면 transport 계약 위반이므로
        # local 권한 경계로 우회하지 않고 안전한 실패로 닫는다.
        return self._fail_closed(
            request,
            validation.reason or "contract",
        )

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
            "Company Notion API shadow failed request_id=%s reason=%s",
            _safe_request_id(request.request_id),
            reason,
        )

    def _fail_closed(
        self,
        request: CompanyAssistantRequest,
        reason: str,
    ) -> CompanyAssistantResult:
        self._logger.warning(
            "Company Notion API failed closed request_id=%s reason=%s",
            _safe_request_id(request.request_id),
            reason,
        )
        return CompanyAssistantResult(
            route="company_notion_search",
            outcome="failed",
            messages=(
                AssistantMessage(
                    body=(
                        "회사 Notion 답변 서비스 상태를 확인할 수 없어. "
                        "잠시 후 다시 시도해줘"
                    )
                ),
            ),
            fallback_reason=f"company_api_{reason}",
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


def _is_allowed_notion_route(
    result: CompanyAssistantResult | None,
) -> bool:
    return bool(
        result is not None
        and result.route in _ALLOWED_NOTION_ROUTES
    )


def _is_shadow_eligible_notion_result(
    result: CompanyAssistantResult | None,
) -> bool:
    # local actor 정책에서 거부한 질문은 API allowlist가 drift했더라도
    # shadow 조회나 LLM 실행을 시작하지 않는다.
    return bool(
        _is_allowed_notion_route(result)
        and result is not None
        and result.outcome != "denied"
    )


def _validate_remote_notion_result(
    result: CompanyAssistantResult | None,
) -> _RemoteResultValidation:
    if not _is_allowed_notion_route(result):
        # API가 다른 정책 route에서 명시적으로 거부한 결과는 local
        # Notion으로 우회하지 않는다.
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
    if any(
        not _is_notion_source_uri(source.uri)
        for source in result.sources
    ):
        return _RemoteResultValidation(
            accepted=False,
            reason="unsafe_source_host",
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
) -> tuple[str, ...]:
    # 본문은 비교하거나 기록하지 않고 전달 범위와 메시지 개수만
    # 비교한다.
    return tuple(message.delivery_scope for message in result.messages)


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
    "CompanyNotionApiRolloutService",
    "wrap_company_notion_service",
]
