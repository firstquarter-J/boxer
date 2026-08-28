from __future__ import annotations

import logging
import re
import unicodedata
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


_BASE_ACCESS_MENTION_COMMAND_RE = re.compile(
    r"^<@(?P<boxer_user_id>[UW][A-Z0-9]+)>\s+"
    r"<@(?P<target_user_id>[UW][A-Z0-9]+)>\s+박서\s+사용\s+(?P<state>가능|불가)$"
)
_BASE_ACCESS_NAME_COMMAND_RE = re.compile(
    r"^<@(?P<boxer_user_id>[UW][A-Z0-9]+)>\s+"
    r"(?P<target_name>[^<>\r\n]{1,80}?)\s+박서\s+사용\s+(?P<state>가능|불가)$"
)
_SLACK_USER_ID_RE = re.compile(r"^[UW][A-Z0-9]+$")
_MAX_USERS_LIST_PAGES = 100

BASE_ACCESS_DENIED_REPLY = "박서 사용 권한이 없어. 현에게 요청해줘"
BASE_ACCESS_UNAVAILABLE_REPLY = "박서 사용 권한을 확인할 수 없어. 잠시 후 다시 시도해줘"


@dataclass(slots=True)
class SlackBaseAccessRuntime:
    """Slack 진입점 전체가 공유하는 단일 Boxer 사용 권한 runtime."""

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


@dataclass(frozen=True, slots=True)
class _BaseAccessManagementCommand:
    boxer_user_id: str
    target_user_id: str | None
    target_name: str | None
    allowed: bool


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
    return SlackBaseAccessRuntime(store=store, logger=actual_logger)


def _parse_base_access_management_command(
    raw_text: str,
) -> _BaseAccessManagementCommand | None:
    # 공개 adapter의 question은 모든 멘션을 제거하므로 Boxer 멘션이 남은 원문만 파싱한다.
    normalized_text = str(raw_text or "").strip()
    mention_match = _BASE_ACCESS_MENTION_COMMAND_RE.fullmatch(normalized_text)
    if mention_match is not None:
        return _BaseAccessManagementCommand(
            boxer_user_id=mention_match.group("boxer_user_id"),
            target_user_id=mention_match.group("target_user_id"),
            target_name=None,
            allowed=mention_match.group("state") == "가능",
        )

    name_match = _BASE_ACCESS_NAME_COMMAND_RE.fullmatch(normalized_text)
    if name_match is None:
        return None
    target_name = " ".join(name_match.group("target_name").split())
    if not target_name:
        return None
    return _BaseAccessManagementCommand(
        boxer_user_id=name_match.group("boxer_user_id"),
        target_user_id=None,
        target_name=target_name,
        allowed=name_match.group("state") == "가능",
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


def _normalize_user_name(value: object) -> str:
    normalized = unicodedata.normalize("NFKC", str(value or ""))
    return " ".join(normalized.split()).casefold()


def _user_name_aliases(user: dict[str, Any]) -> set[str]:
    profile = user.get("profile") if isinstance(user.get("profile"), dict) else {}
    aliases = {
        _normalize_user_name(candidate)
        for candidate in (
            profile.get("display_name_normalized"),
            profile.get("display_name"),
            profile.get("real_name_normalized"),
            profile.get("real_name"),
            user.get("real_name"),
            user.get("name"),
        )
    }
    aliases.discard("")
    return aliases


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


def _resolve_named_target_user_id(
    *,
    client: Any,
    workspace_id: str,
    boxer_user_id: str,
    target_name: str,
    allowed: bool,
) -> tuple[str | None, str | None]:
    """Slack 전체 사용자에서 정확히 한 명인 활성 내부 사람만 이름으로 확정한다."""

    normalized_target_name = _normalize_user_name(target_name)
    if not normalized_target_name:
        return None, "invalid_name"

    candidates: dict[str, dict[str, Any]] = {}
    cursor = ""
    seen_cursors: set[str] = set()
    for _ in range(_MAX_USERS_LIST_PAGES):
        arguments: dict[str, Any] = {"limit": 200, "team_id": workspace_id}
        if cursor:
            arguments["cursor"] = cursor
        try:
            response = _slack_response_data(client.users_list(**arguments))
        except Exception:
            return None, "slack_lookup_failed"
        if response is None or response.get("ok") is False:
            return None, "slack_lookup_failed"
        members = response.get("members")
        if not isinstance(members, list):
            return None, "slack_lookup_failed"

        # 첫 일치에서 멈추면 뒤 페이지의 동명이인을 놓치므로 끝까지 ID 기준으로 합친다.
        for member in members:
            if not isinstance(member, dict):
                return None, "slack_lookup_failed"
            member_user_id = str(member.get("id") or "").strip()
            if _SLACK_USER_ID_RE.fullmatch(member_user_id) is None:
                return None, "slack_lookup_failed"
            if normalized_target_name in _user_name_aliases(member):
                candidates[member_user_id] = member

        metadata = response.get("response_metadata")
        if not isinstance(metadata, dict):
            return None, "slack_lookup_failed"
        raw_next_cursor = metadata.get("next_cursor", "")
        if not isinstance(raw_next_cursor, str):
            return None, "slack_lookup_failed"
        next_cursor = raw_next_cursor.strip()
        if not next_cursor:
            break
        if next_cursor in seen_cursors:
            return None, "slack_lookup_failed"
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    else:
        return None, "slack_lookup_failed"

    if not candidates:
        return None, "name_not_found"
    if len(candidates) != 1:
        return None, "ambiguous_name"
    target_user_id, target_user = next(iter(candidates.items()))
    if (
        str(target_user.get("team_id") or "").strip() != workspace_id
        or target_user_id == boxer_user_id
        or target_user_id in {"USLACK", "USLACKBOT"}
    ):
        return None, "invalid_name_target"
    if allowed and not _is_active_internal_human(
        target_user,
        workspace_id=workspace_id,
        target_user_id=target_user_id,
        boxer_user_id=boxer_user_id,
    ):
        return None, "invalid_grant_target"
    return target_user_id, None


def _load_target_for_mutation(
    *,
    runtime: SlackBaseAccessRuntime,
    client: Any,
    workspace_id: str,
    target_user_id: str,
    boxer_user_id: str,
    allowed: bool,
    expected_target_name: str | None = None,
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
    if (
        expected_target_name is not None
        and _normalize_user_name(expected_target_name) not in _user_name_aliases(user)
    ):
        # users.list와 users.info 사이에 이름이 바뀌면 다른 사람으로 오인하지 않고 중단한다.
        return None, "name_changed"
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
    """Hyun의 exact 사용 가능/불가 명령을 이름 또는 멘션 대상으로 적용한다."""

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

    mentioned_boxer_user_id = command.boxer_user_id
    target_user_id = command.target_user_id
    target_name = command.target_name
    allowed = command.allowed
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

    if target_user_id is None:
        target_user_id, target_error = _resolve_named_target_user_id(
            client=client,
            workspace_id=workspace_id,
            boxer_user_id=boxer_user_id,
            target_name=str(target_name or ""),
            allowed=allowed,
        )
        if target_error:
            _set_request_log_status(
                payload,
                (
                    "denied"
                    if target_error
                    in {
                        "invalid_name",
                        "invalid_name_target",
                        "invalid_grant_target",
                        "name_not_found",
                        "ambiguous_name",
                    }
                    else "error"
                ),
            )
            if target_error == "ambiguous_name":
                reply("같은 이름의 사용자가 여러 명이야. 대상 사용자를 @멘션해줘")
            elif target_error == "invalid_grant_target":
                reply("활성 상태인 내부 사람 계정만 박서 사용을 허용할 수 있어")
            elif target_error in {
                "invalid_name",
                "invalid_name_target",
                "name_not_found",
            }:
                reply("이름으로 사용자를 찾지 못했어. 대상 사용자를 @멘션해줘")
            else:
                reply(BASE_ACCESS_UNAVAILABLE_REPLY)
            return True

    if not target_user_id:
        _set_request_log_status(payload, "error")
        reply(BASE_ACCESS_UNAVAILABLE_REPLY)
        return True
    if not allowed and target_user_id == cs.BOXER_ACCESS_ADMIN_USER_ID:
        _set_request_log_status(payload, "denied")
        reply("현의 박서 사용 권한은 해제할 수 없어")
        return True

    display_name, target_error = _load_target_for_mutation(
        runtime=runtime,
        client=client,
        workspace_id=workspace_id,
        target_user_id=target_user_id,
        boxer_user_id=boxer_user_id,
        allowed=allowed,
        expected_target_name=target_name,
    )
    if target_error:
        _set_request_log_status(payload, "denied" if target_error.startswith("invalid") else "error")
        if target_error == "different_workspace":
            reply("같은 워크스페이스 사용자만 변경할 수 있어")
        elif target_error == "invalid_grant_target":
            reply("활성 상태인 내부 사람 계정만 박서 사용을 허용할 수 있어")
        elif target_error == "name_changed":
            reply("사용자 이름이 변경됐어. 대상 사용자를 @멘션해줘")
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
