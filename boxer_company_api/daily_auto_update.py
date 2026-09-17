"""API와 scheduler가 같은 flock·revision으로 읽는 자동 업데이트 설정이다."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from boxer_company._operation_routing_automation import AUTO_UPDATE_OPTION_KEYS
from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.daily_auto_update_route import DailyAutoUpdateStatus
from boxer_company_api.automation import (
    AutomationCycleContractError,
    JsonAutomationCycleStateStore,
)

_CONTROL_KIND = "daily_auto_update_control"
_ENV_KEYS = {
    "agent": "DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT",
    "box_free": "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE",
    "box_paid": "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID",
}


def load_daily_auto_update_defaults(
    env: Mapping[str, str] | None = None,
) -> dict[str, bool]:
    source = os.environ if env is None else env
    values: dict[str, bool] = {}
    for target, key in _ENV_KEYS.items():
        raw = str(source.get(key, "")).strip().lower()
        if raw not in {"", "0", "false", "no", "off", "1", "true", "yes", "on"}:
            raise ValueError("daily auto update default is invalid")
        values[target] = raw in {"1", "true", "yes", "on"}
    return values


def _control_key(tenant_id: str) -> str:
    # 날짜별 순회 cursor와 겹치지 않는 예약 slot이다. pending delivery나
    # inFlight를 갖지 않으므로 기존 pull/ACK와 복구 marker를 건드리지 않는다.
    return hashlib.sha256(
        f"{tenant_id}\0daily_device_round\0settings".encode()
    ).hexdigest()


def validate_daily_auto_update_state(state: Mapping[str, Any], state_key: str) -> None:
    if (
        set(state) != {"kind", "tenantId", "overrides"}
        or state.get("kind") != _CONTROL_KIND
        or not isinstance(state.get("tenantId"), str)
        or not state["tenantId"].strip()
        or _control_key(state["tenantId"]) != state_key
        or not isinstance(state.get("overrides"), dict)
        or not set(state["overrides"]).issubset(AUTO_UPDATE_OPTION_KEYS)
    ):
        raise AutomationCycleContractError("daily auto update state is invalid")
    for row in state["overrides"].values():
        if (
            not isinstance(row, dict)
            or set(row) != {"enabled", "updatedAt", "updatedBy", "requestId"}
            or type(row.get("enabled")) is not bool
            or any(
                not isinstance(row.get(key), str) or not row[key].strip()
                for key in ("updatedAt", "updatedBy", "requestId")
            )
        ):
            raise AutomationCycleContractError("daily auto update override is invalid")
        try:
            updated_at = datetime.fromisoformat(row["updatedAt"])
        except ValueError as exc:
            raise AutomationCycleContractError(
                "daily auto update time is invalid"
            ) from exc
        if updated_at.tzinfo is None:
            raise AutomationCycleContractError("daily auto update time is invalid")


def _read_overrides(
    store: JsonAutomationCycleStateStore, tenant_id: str
) -> dict[str, Any]:
    key = _control_key(tenant_id)
    with store.locked_snapshot() as snapshot:
        if not snapshot.exists:
            raise AutomationCycleContractError("automation state is missing")
        exists, state = snapshot.cycle(key)
        if not exists:
            return {}
        validate_daily_auto_update_state(state, key)
        return dict(state["overrides"])


def resolve_daily_auto_update_options(
    store: JsonAutomationCycleStateStore,
    tenant_id: str,
    defaults: Mapping[str, bool],
) -> dict[str, bool]:
    # 각 병원 cycle을 시작하기 직전에 다시 읽어 별도 API process의 저장을
    # 재시작 없이 반영한다. 정리·전원 종료 옵션은 원래 scheduler 값을 유지한다.
    options = dict(defaults)
    for target, row in _read_overrides(store, tenant_id).items():
        options[AUTO_UPDATE_OPTION_KEYS[target]] = row["enabled"]
    return options


class DailyAutoUpdateController:
    def __init__(
        self,
        store: JsonAutomationCycleStateStore,
        *,
        tenant_id: str,
        defaults: Mapping[str, bool],
        daily_enabled: bool,
    ) -> None:
        tenant_id = tenant_id.strip()
        if not tenant_id.strip() or tenant_id == "*":
            raise ValueError("daily auto update tenant is invalid")
        if set(defaults) != set(AUTO_UPDATE_OPTION_KEYS) or any(
            type(value) is not bool for value in defaults.values()
        ):
            raise ValueError("daily auto update defaults are invalid")
        self._store = store
        self._tenant_id = tenant_id
        self._defaults = dict(defaults)
        self._daily_enabled = daily_enabled

    def _check_request(self, request: CompanyAssistantRequest) -> None:
        # 이 설정은 API scheduler의 concrete Slack tenant 하나에만 적용된다.
        if (
            request.tenant_id != self._tenant_id
            or request.channel != "slack"
            or not request.actor_id
            or not request.request_id
        ):
            raise ValueError("daily auto update scope is invalid")

    def _status(self, overrides: Mapping[str, Any]) -> DailyAutoUpdateStatus:
        return DailyAutoUpdateStatus(
            enabled={
                target: overrides.get(target, {}).get("enabled", default)
                for target, default in self._defaults.items()
            },
            daily_enabled=self._daily_enabled,
        )

    def read(self, request: CompanyAssistantRequest) -> DailyAutoUpdateStatus:
        self._check_request(request)
        return self._status(_read_overrides(self._store, self._tenant_id))

    def set_enabled(
        self,
        request: CompanyAssistantRequest,
        target: str,
        enabled: bool,
    ) -> DailyAutoUpdateStatus:
        self._check_request(request)
        if target not in AUTO_UPDATE_OPTION_KEYS or type(enabled) is not bool:
            raise ValueError("daily auto update target is invalid")
        key = _control_key(self._tenant_id)

        def update(
            exists: bool, current: dict[str, Any]
        ) -> tuple[dict[str, Any], DailyAutoUpdateStatus]:
            if exists:
                validate_daily_auto_update_state(current, key)
            else:
                current = {
                    "kind": _CONTROL_KIND,
                    "tenantId": self._tenant_id,
                    "overrides": {},
                }
            overrides = dict(current["overrides"])
            previous = overrides.get(target, {})
            if previous.get("requestId") == request.request_id:
                if (
                    previous["enabled"] != enabled
                    or previous["updatedBy"] != request.actor_id
                ):
                    raise ValueError("daily auto update request conflict")
                return current, self._status(overrides)
            overrides[target] = {
                "enabled": enabled,
                "updatedAt": datetime.now(UTC).isoformat(),
                "updatedBy": request.actor_id,
                "requestId": request.request_id,
            }
            return {**current, "overrides": overrides}, self._status(overrides)

        # 성공 응답은 fsync·원자 교체가 끝난 뒤에만 반환된다. 다른 cycle이나
        # 다른 대상의 동시 변경은 기존 host-local flock 아래 그대로 보존한다.
        return self._store.mutate_cycle(key, update, require_existing_document=True)
