from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

from boxer_adapter_slack.common import (
    MentionPayload,
    SlackReplyFn,
    _set_request_log_route,
    _set_request_log_status,
)
from boxer_company import settings as cs
from boxer_company.base_access import (
    BaseAccessSettings,
    BaseAccessStore,
    ConfigurationError,
    ConflictError,
    StoreUnavailable,
    ValidationError,
    build_base_access_store,
    slack_ts_to_ordering_key,
)


_BASE_ACCESS_COMMAND_RE = re.compile(
    r"^<@(?P<boxer_user_id>[UW][A-Z0-9]+)>\s+"
    r"<@(?P<target_user_id>[UW][A-Z0-9]+)>\s+박서\s+사용\s+(?P<state>가능|불가)$"
)

BASE_ACCESS_DENIED_REPLY = "박서 사용 권한이 없어. 현에게 요청해줘"
BASE_ACCESS_UNAVAILABLE_REPLY = "박서 사용 권한을 확인할 수 없어. 잠시 후 다시 시도해줘"


@dataclass(slots=True)
class SlackBaseAccessRuntime:
    """Slack 진입점 전체가 공유하는 단일 Boxer 사용 권한 runtime."""

    settings: BaseAccessSettings
    store: BaseAccessStore | None
    logger: logging.Logger

    def is_allowed(self, workspace_id: str | None, user_id: str | None) -> bool:
        normalized_workspace_id = str(workspace_id or "").strip()
        normalized_user_id = str(user_id or "").strip()
        if not normalized_workspace_id or not normalized_user_id or self.store is None:
            self.logger.warning("Boxer 기본 사용 권한을 확인할 식별자나 저장소가 없어")
            return False
        try:
            return self.store.is_allowed(normalized_workspace_id, normalized_user_id)
        except Exception as exc:
            # 권한 저장소 장애 시 요청을 통과시키지 않는 fail-closed 경계다.
            self.logger.warning(
                "Boxer 기본 사용 권한 조회에 실패했어 error_type=%s",
                type(exc).__name__,
            )
            return False


def build_slack_base_access_runtime(
    *,
    logger: logging.Logger | None = None,
) -> SlackBaseAccessRuntime:
    actual_logger = logger or logging.getLogger(__name__)
    settings = BaseAccessSettings(
        state_path=str(cs.BOXER_BASE_ACCESS_STATE_PATH or "").strip(),
    )
    try:
        store = build_base_access_store(settings)
    except (ConfigurationError, StoreUnavailable) as exc:
        # 로컬 상태 파일을 신뢰할 수 없으면 runtime은 유지하되 모든 사람 요청을 차단한다.
        actual_logger.error(
            "Boxer 기본 사용 권한 저장소를 시작하지 못했어 error_type=%s",
            type(exc).__name__,
        )
        store = None
    return SlackBaseAccessRuntime(settings=settings, store=store, logger=actual_logger)


def _parse_base_access_management_command(raw_text: str) -> tuple[str, str, bool] | None:
    # 공개 adapter의 question은 모든 멘션을 제거하므로 두 멘션이 남은 Slack 원문만 파싱한다.
    match = _BASE_ACCESS_COMMAND_RE.fullmatch(str(raw_text or "").strip())
    if match is None:
        return None
    return (
        match.group("boxer_user_id"),
        match.group("target_user_id"),
        match.group("state") == "가능",
    )


def _slack_response_data(response: Any) -> dict[str, Any] | None:
    if isinstance(response, dict):
        return response
    data = getattr(response, "data", None)
    return data if isinstance(data, dict) else None


def _slack_error_code(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return str(response.get("error") or "").strip()
    data = getattr(response, "data", None)
    if isinstance(data, dict):
        return str(data.get("error") or "").strip()
    return ""


def _extract_user_display_name(user: dict[str, Any], user_id: str) -> str:
    profile = user.get("profile") if isinstance(user.get("profile"), dict) else {}
    for candidate in (
        profile.get("display_name_normalized"),
        profile.get("display_name"),
        profile.get("real_name_normalized"),
        profile.get("real_name"),
        user.get("real_name"),
        user.get("name"),
        user_id,
    ):
        value = str(candidate or "").strip()
        if value:
            return value
    return user_id


def _is_active_internal_human(
    user: dict[str, Any],
    *,
    workspace_id: str,
    target_user_id: str,
    boxer_user_id: str,
) -> bool:
    enterprise_user = (
        user.get("enterprise_user")
        if isinstance(user.get("enterprise_user"), dict)
        else {}
    )
    profile = user.get("profile") if isinstance(user.get("profile"), dict) else {}
    return bool(
        str(user.get("id") or "").strip() == target_user_id
        and str(user.get("team_id") or "").strip() == workspace_id
        and target_user_id != boxer_user_id
        and target_user_id not in {"USLACK", "USLACKBOT"}
        and not bool(user.get("deleted"))
        and not bool(user.get("is_bot"))
        and not bool(user.get("is_workflow_bot"))
        and not bool(user.get("is_agentforce_bot"))
        and not bool(user.get("bot_id"))
        and not bool(user.get("app_id"))
        and not bool(profile.get("bot_id"))
        and not bool(profile.get("api_app_id"))
        and not bool(profile.get("app_id"))
        and not bool(user.get("is_restricted"))
        and not bool(user.get("is_ultra_restricted"))
        and not bool(user.get("is_stranger"))
        and not bool(user.get("is_external"))
        and not bool(enterprise_user.get("is_external"))
    )


def _load_boxer_identity(client: Any, workspace_id: str) -> str | None:
    response = _slack_response_data(client.auth_test())
    if response is None:
        return None
    if str(response.get("team_id") or "").strip() != workspace_id:
        return None
    return str(response.get("user_id") or "").strip() or None


def _load_target_for_mutation(
    *,
    runtime: SlackBaseAccessRuntime,
    client: Any,
    workspace_id: str,
    target_user_id: str,
    boxer_user_id: str,
    allowed: bool,
) -> tuple[str | None, str | None]:
    try:
        response = client.users_info(user=target_user_id)
    except Exception as exc:
        # 삭제된 계정은 users_info에서 사라진 뒤에도 기존 membership을 정리할 수 있다.
        if allowed or _slack_error_code(exc) not in {"user_not_found", "users_not_found"}:
            return None, "slack_lookup_failed"
        try:
            member = runtime.store.get_member(workspace_id, target_user_id) if runtime.store else None
        except Exception:
            return None, "store_unavailable"
        return (member.display_name if member is not None else target_user_id), None

    response_data = _slack_response_data(response)
    user = response_data.get("user") if response_data is not None else None
    if not isinstance(user, dict):
        return None, "invalid_user"
    if str(user.get("id") or "").strip() != target_user_id:
        return None, "invalid_user"
    if str(user.get("team_id") or "").strip() != workspace_id:
        return None, "different_workspace"
    display_name = _extract_user_display_name(user, target_user_id)
    if allowed and not _is_active_internal_human(
        user,
        workspace_id=workspace_id,
        target_user_id=target_user_id,
        boxer_user_id=boxer_user_id,
    ):
        return None, "invalid_grant_target"
    return display_name, None


def handle_base_access_management_command(
    payload: MentionPayload,
    reply: SlackReplyFn,
    client: Any,
    logger: logging.Logger,
    *,
    runtime: SlackBaseAccessRuntime,
) -> bool:
    """Hyun의 두 가지 exact 명령만 확인 없이 즉시 적용한다."""

    command = _parse_base_access_management_command(payload.get("raw_text") or "")
    if command is None:
        return False

    _set_request_log_route(payload, "base_access_management")
    actor_user_id = str(payload.get("user_id") or "").strip()
    if (
        cs.HYUN_USER_ID != cs.BOXER_ACCESS_ADMIN_USER_ID
        or actor_user_id != cs.BOXER_ACCESS_ADMIN_USER_ID
    ):
        _set_request_log_status(payload, "denied")
        reply("박서 사용 권한은 현만 변경할 수 있어")
        return True

    mentioned_boxer_user_id, target_user_id, allowed = command
    if not allowed and target_user_id == cs.BOXER_ACCESS_ADMIN_USER_ID:
        _set_request_log_status(payload, "denied")
        reply("현의 박서 사용 권한은 해제할 수 없어")
        return True

    workspace_id = str(payload.get("workspace_id") or "").strip()
    current_ts = str(payload.get("current_ts") or "").strip()
    if not workspace_id or not current_ts or runtime.store is None:
        _set_request_log_status(payload, "error")
        reply(BASE_ACCESS_UNAVAILABLE_REPLY)
        return True

    try:
        boxer_user_id = _load_boxer_identity(client, workspace_id)
    except Exception:
        boxer_user_id = None
    if not boxer_user_id:
        _set_request_log_status(payload, "error")
        reply(BASE_ACCESS_UNAVAILABLE_REPLY)
        return True
    if mentioned_boxer_user_id != boxer_user_id:
        # app_mention의 첫 멘션이 Boxer가 아니면 이 관리 명령 문법으로 취급하지 않는다.
        return False

    display_name, target_error = _load_target_for_mutation(
        runtime=runtime,
        client=client,
        workspace_id=workspace_id,
        target_user_id=target_user_id,
        boxer_user_id=boxer_user_id,
        allowed=allowed,
    )
    if target_error:
        _set_request_log_status(payload, "denied" if target_error.startswith("invalid") else "error")
        if target_error == "different_workspace":
            reply("같은 워크스페이스 사용자만 변경할 수 있어")
        elif target_error == "invalid_grant_target":
            reply("활성 상태인 내부 사람 계정만 박서 사용을 허용할 수 있어")
        else:
            reply(BASE_ACCESS_UNAVAILABLE_REPLY)
        return True

    try:
        ordering_key = slack_ts_to_ordering_key(current_ts)
        result = runtime.store.set_allowed(
            workspace_id,
            target_user_id,
            allowed,
            str(display_name or target_user_id),
            actor_user_id,
            ordering_key,
        )
    except (ConflictError, StoreUnavailable, ValidationError) as exc:
        logger.warning(
            "Boxer 기본 사용 권한 변경에 실패했어 error_type=%s",
            type(exc).__name__,
        )
        _set_request_log_status(payload, "error")
        reply(BASE_ACCESS_UNAVAILABLE_REPLY)
        return True
    except Exception as exc:
        logger.error(
            "Boxer 기본 사용 권한 변경 중 예기치 않은 오류가 발생했어 error_type=%s",
            type(exc).__name__,
        )
        _set_request_log_status(payload, "error")
        reply(BASE_ACCESS_UNAVAILABLE_REPLY)
        return True

    current_state = "가능" if result.allowed else "불가"
    if result.stale:
        reply(f"더 최신 명령이 이미 반영돼 있어. <@{target_user_id}>의 현재 상태는 박서 사용 {current_state}야")
    elif not result.changed:
        reply(f"<@{target_user_id}>은 이미 박서 사용 {current_state} 상태야")
    elif result.allowed:
        reply(f"<@{target_user_id}> 박서 사용을 허용했어")
    else:
        reply(f"<@{target_user_id}> 박서 사용을 막았어")
    return True
