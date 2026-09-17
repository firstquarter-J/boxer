from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from boxer_company._operation_routing_automation import (
    AUTO_UPDATE_LABELS,
    DAILY_AUTO_UPDATE_ROUTE,
    parse_daily_auto_update_command,
)
from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)


@dataclass(frozen=True, slots=True)
class DailyAutoUpdateStatus:
    enabled: Mapping[str, bool]
    daily_enabled: bool


class DailyAutoUpdateControl(Protocol):
    """설정 저장소는 API가 주입하며 회사 route는 저장 위치를 알지 않는다."""

    def read(self, request: CompanyAssistantRequest) -> DailyAutoUpdateStatus: ...

    def set_enabled(
        self,
        request: CompanyAssistantRequest,
        target: str,
        enabled: bool,
    ) -> DailyAutoUpdateStatus: ...


class DailyAutoUpdateAssistantRoute:
    name = DAILY_AUTO_UPDATE_ROUTE

    def __init__(
        self,
        control: DailyAutoUpdateControl | None = None,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._control = control
        self._logger = logger or logging.getLogger(__name__)

    def handle(self, request: CompanyAssistantRequest) -> CompanyAssistantResult | None:
        if request.metadata.get("route_group") != "operations":
            return None
        command = parse_daily_auto_update_command(request.question)
        if command is None:
            return None
        if command.target == "box_qualifier_required":
            return self._result(
                "needs_input",
                "마미박스 자동 업데이트는 `무료병원` 또는 `유료병원` 중 "
                "한 대상을 정확히 써줘. 예: `마미박스 무료병원 자동 업데이트 꺼`",
            )
        if command.action == "needs_input" or (
            command.action != "status" and command.target not in AUTO_UPDATE_LABELS
        ):
            return self._result(
                "needs_input",
                "자동 업데이트는 `에이전트`, `마미박스 무료병원`, "
                "`마미박스 유료병원` 중 한 대상씩 `켜` 또는 `꺼`로 요청해줘. "
                "조회는 `데일리 자동 업데이트 상태`로 요청할 수 있어",
            )
        if self._control is None:
            return self._result("denied", "자동 업데이트 설정 제어가 준비되지 않았어")
        try:
            if command.action == "status":
                status = self._control.read(request)
                heading = "**데일리 자동 업데이트 설정**"
            else:
                status = self._control.set_enabled(
                    request,
                    str(command.target),
                    command.action == "enable",
                )
                action = "켰어" if command.action == "enable" else "껐어"
                heading = f"{AUTO_UPDATE_LABELS[str(command.target)]} 자동 업데이트를 {action}"
        except (OSError, ValueError, RuntimeError) as exc:
            # 저장 성공을 확인하지 못하면 성공 응답이나 문서 fallback으로 숨기지 않는다.
            self._logger.error(
                "Daily auto update control failed error_type=%s", type(exc).__name__
            )
            return self._result(
                "failed",
                "자동 업데이트 설정을 확인하거나 저장하지 못했어. 설정 상태를 확인해줘",
                fallback_reason=(
                    "daily_auto_update_read_failed"
                    if command.action == "status"
                    else "daily_auto_update_save_failed"
                ),
            )
        lines = [heading, ""]
        lines.extend(
            f"- {label}: **{'켜짐' if status.enabled[target] else '꺼짐'}**"
            for target, label in AUTO_UPDATE_LABELS.items()
        )
        if not status.daily_enabled:
            lines.extend(("", "현재 일일 장비 순회는 중지 상태야"))
        if command.action != "status":
            lines.extend(
                (
                    "",
                    "설정은 다음 병원 순회부터 적용돼. 이미 시작한 업데이트는 중단하지 않아",
                )
            )
        return self._result("answered", "\n".join(lines))

    def _result(
        self,
        outcome: AssistantOutcome,
        body: str,
        *,
        fallback_reason: str | None = None,
    ) -> CompanyAssistantResult:
        return CompanyAssistantResult(
            route=self.name,
            outcome=outcome,
            messages=(AssistantMessage(body=body),),
            fallback_reason=fallback_reason,
        )
