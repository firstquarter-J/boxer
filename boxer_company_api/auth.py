from __future__ import annotations

from dataclasses import dataclass
import hmac
from typing import Sequence

from boxer_company_api.problems import CompanyApiProblem
from boxer_company_api.settings import (
    CompanyApiCallerSettings,
    CompanyApiSettings,
)


@dataclass(frozen=True, slots=True)
class CallerPrincipal:
    """인증 후에는 원본 token을 버리고 권한 정보만 요청 범위에 전달한다."""

    caller_id: str
    tenant_ids: frozenset[str]
    channels: frozenset[str]
    actor_ids: frozenset[str]
    capabilities: frozenset[str]


class CallerRegistry:
    def __init__(
        self,
        callers: (
            Sequence[CompanyApiCallerSettings]
            | CompanyApiSettings
        ),
    ) -> None:
        if isinstance(callers, CompanyApiSettings):
            self._callers = tuple(callers.callers)
            self._configuration_error = callers.configuration_error
        else:
            self._callers = tuple(callers)
            self._configuration_error = None

    @property
    def configured(self) -> bool:
        return bool(self._callers) and self._configuration_error is None

    def authenticate(
        self,
        authorization: str | None,
        request_id: str | None = None,
    ) -> CallerPrincipal:
        candidate_token = _extract_bearer_token(authorization) or ""
        matched: CompanyApiCallerSettings | None = None

        # 모든 등록 token을 비교한 뒤 결과를 결정해 caller 위치에 따른 시간 차이를 줄인다.
        for caller in self._callers:
            if hmac.compare_digest(
                candidate_token.encode("utf-8"),
                caller.token.encode("utf-8"),
            ):
                matched = caller

        if (
            not self.configured
            or not candidate_token
            or matched is None
        ):
            raise CompanyApiProblem(
                status=401,
                code="authentication_failed",
                request_id=request_id,
                headers={"WWW-Authenticate": "Bearer"},
            )
        return CallerPrincipal(
            caller_id=matched.caller_id,
            tenant_ids=matched.tenant_ids,
            channels=matched.channels,
            actor_ids=matched.actor_ids,
            capabilities=matched.capabilities,
        )


def _extract_bearer_token(
    authorization: str | None,
) -> str | None:
    if not isinstance(authorization, str):
        return None
    scheme, separator, credentials = authorization.strip().partition(" ")
    token = credentials.strip()
    if (
        not separator
        or scheme.lower() != "bearer"
        or not token
        or any(character.isspace() for character in token)
    ):
        return None
    return token


__all__ = [
    "CallerPrincipal",
    "CallerRegistry",
]
