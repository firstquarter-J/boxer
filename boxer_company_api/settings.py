from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
import re
from typing import Any, Mapping


_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 8010
_CALLER_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9._~+/=-]{32,512}$")
_SCOPED_ID_PATTERN = re.compile(
    r"^(?:\*|[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255})$"
)
_CAPABILITY_PATTERN = re.compile(
    r"^[a-z][a-z0-9._:-]{0,127}$"
)
_ALLOWED_CHANNELS = frozenset({"slack", "web", "api"})
_REQUIRED_TURN_CAPABILITY = "assistant.turn.read"


@dataclass(frozen=True, slots=True)
class CompanyApiCallerSettings:
    """서버 설정에서만 만들어지는 내부 API 호출자 권한이다."""

    caller_id: str
    token: str = field(repr=False)
    tenant_ids: frozenset[str]
    channels: frozenset[str]
    actor_ids: frozenset[str]
    allow_anonymous_actor: bool
    capabilities: frozenset[str]


@dataclass(frozen=True, slots=True)
class CompanyApiSettings:
    host: str
    port: int
    callers: tuple[CompanyApiCallerSettings, ...]
    configuration_error: str | None = None

    @property
    def authentication_configured(self) -> bool:
        return bool(self.callers) and self.configuration_error is None


def load_company_api_settings(
    env: Mapping[str, str] | None = None,
) -> CompanyApiSettings:
    """환경변수의 caller registry를 검증하고 오류 상세 없이 fail closed한다."""

    source = env if env is not None else os.environ
    host, port, server_error = _load_server_settings(source)
    raw_registry = str(
        source.get("BOXER_COMPANY_API_CALLERS_JSON", "")
    ).strip()
    if not raw_registry:
        return CompanyApiSettings(
            host=host,
            port=port,
            callers=(),
            configuration_error=(
                server_error or "caller_registry_missing"
            ),
        )

    try:
        parsed = json.loads(raw_registry)
        callers = _parse_caller_registry(parsed)
    except (TypeError, ValueError, json.JSONDecodeError):
        return CompanyApiSettings(
            host=host,
            port=port,
            callers=(),
            configuration_error=(
                server_error or "caller_registry_invalid"
            ),
        )

    if server_error is not None:
        return CompanyApiSettings(
            host=host,
            port=port,
            callers=(),
            configuration_error=server_error,
        )
    return CompanyApiSettings(
        host=host,
        port=port,
        callers=callers,
    )


def _load_server_settings(
    env: Mapping[str, str],
) -> tuple[str, int, str | None]:
    host = str(
        env.get("BOXER_COMPANY_API_HOST", _DEFAULT_HOST)
    ).strip()
    raw_port = str(
        env.get("BOXER_COMPANY_API_PORT", _DEFAULT_PORT)
    ).strip()
    try:
        port = int(raw_port)
    except (TypeError, ValueError):
        return _DEFAULT_HOST, _DEFAULT_PORT, "server_configuration_invalid"
    if not host or len(host) > 255 or not 1 <= port <= 65535:
        return _DEFAULT_HOST, _DEFAULT_PORT, "server_configuration_invalid"
    return host, port, None


def _parse_caller_registry(
    payload: Any,
) -> tuple[CompanyApiCallerSettings, ...]:
    if not isinstance(payload, list) or not payload:
        raise ValueError("caller registry must be a non-empty list")

    callers: list[CompanyApiCallerSettings] = []
    caller_ids: set[str] = set()
    tokens: set[str] = set()
    for item in payload:
        caller = _parse_caller(item)
        if caller.caller_id in caller_ids or caller.token in tokens:
            raise ValueError("caller registry contains a duplicate")
        caller_ids.add(caller.caller_id)
        tokens.add(caller.token)
        callers.append(caller)
    return tuple(callers)


def _parse_caller(payload: Any) -> CompanyApiCallerSettings:
    if not isinstance(payload, dict):
        raise ValueError("caller must be an object")
    allowed_keys = {
        "callerId",
        "token",
        "tenantIds",
        "channels",
        "actorIds",
        "allowAnonymousActor",
        "capabilities",
    }
    if set(payload) - allowed_keys:
        raise ValueError("caller contains unsupported fields")

    caller_id = _required_text(payload.get("callerId"))
    token = _required_text(payload.get("token"))
    allow_anonymous_actor = payload.get("allowAnonymousActor", False)
    if (
        not _CALLER_ID_PATTERN.fullmatch(caller_id)
        or not _TOKEN_PATTERN.fullmatch(token)
        or not isinstance(allow_anonymous_actor, bool)
        or allow_anonymous_actor
    ):
        raise ValueError("caller identity is invalid")

    tenant_ids = _parse_string_set(
        payload.get("tenantIds"),
        pattern=_SCOPED_ID_PATTERN,
    )
    channels = _parse_string_set(
        payload.get("channels"),
        allowed_values=_ALLOWED_CHANNELS,
    )
    actor_ids = _parse_string_set(
        payload.get("actorIds"),
        pattern=_SCOPED_ID_PATTERN,
    )
    capabilities = _parse_string_set(
        payload.get("capabilities"),
        pattern=_CAPABILITY_PATTERN,
    )
    if _REQUIRED_TURN_CAPABILITY not in capabilities:
        raise ValueError("caller lacks the required turn capability")
    return CompanyApiCallerSettings(
        caller_id=caller_id,
        token=token,
        tenant_ids=tenant_ids,
        channels=channels,
        actor_ids=actor_ids,
        allow_anonymous_actor=allow_anonymous_actor,
        capabilities=capabilities,
    )


def _parse_string_set(
    value: Any,
    *,
    pattern: re.Pattern[str] | None = None,
    allowed_values: frozenset[str] | None = None,
) -> frozenset[str]:
    if not isinstance(value, list) or not value:
        raise ValueError("caller scope must be a non-empty list")
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError("caller scope value must be text")
        text = item.strip()
        if (
            not text
            or (pattern is not None and not pattern.fullmatch(text))
            or (
                allowed_values is not None
                and text not in allowed_values
            )
        ):
            raise ValueError("caller scope value is invalid")
        normalized.append(text)
    if len(normalized) != len(set(normalized)):
        raise ValueError("caller scope contains a duplicate")
    return frozenset(normalized)


def _required_text(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("required text is missing")
    text = value.strip()
    if not text:
        raise ValueError("required text is empty")
    return text


__all__ = [
    "CompanyApiCallerSettings",
    "CompanyApiSettings",
    "load_company_api_settings",
]
