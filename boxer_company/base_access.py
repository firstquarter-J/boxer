from __future__ import annotations

import json
import os
import re
import stat
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Callable, Protocol, Sequence


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATE_PATH = str(_PROJECT_ROOT / "data" / "boxer_base_access.json")
_STATE_VERSION = 1
_ORDERING_KEY_RE = re.compile(r"^\d{20}\.\d{6}$")
_SLACK_TS_RE = re.compile(r"^(?P<seconds>\d{1,20})(?:\.(?P<fraction>\d{1,6}))?$")
# 운영 Slack Socket Mode는 단일 프로세스로 실행한다. 같은 프로세스의 동시 event만
# 직렬화하며, 여러 프로세스가 같은 파일을 공유하는 구성은 지원하지 않는다.
_PROCESS_LOCK = RLock()

# 초기 seed는 실제 Slack 이벤트보다 항상 먼저 정렬되며, 이후 revoke가 이 값을 덮어쓴다.
SEED_ORDERING_KEY = "00000000000000000000.000000"


class ConfigurationError(RuntimeError):
    """기본 권한 저장소를 안전하게 만들 수 없는 설정 오류."""


class StoreUnavailable(RuntimeError):
    """저장소 내용을 신뢰할 수 없어 요청을 차단해야 하는 오류."""


class ConflictError(RuntimeError):
    """같은 순서의 서로 다른 권한 변경 또는 불완전한 seed 충돌."""


class ValidationError(ValueError):
    """기본 권한 API 입력이 계약을 벗어난 오류."""


class _StateFileMissing(StoreUnavailable):
    """seed만 새 파일을 만들 수 있도록 내부에서 구분하는 누락 상태."""


@dataclass(frozen=True, slots=True)
class BaseAccessSettings:
    state_path: str = DEFAULT_STATE_PATH


@dataclass(frozen=True, slots=True)
class BaseAccessMember:
    workspace_id: str
    user_id: str
    display_name: str
    allowed: bool
    ordering_key: str
    updated_at: str
    updated_by: str


@dataclass(frozen=True, slots=True)
class BaseAccessSeedMember:
    user_id: str
    display_name: str


@dataclass(frozen=True, slots=True)
class BaseAccessMutationResult:
    allowed: bool
    changed: bool
    stale: bool
    member: BaseAccessMember


class BaseAccessStore(Protocol):
    def get_member(self, workspace_id: str, user_id: str) -> BaseAccessMember | None: ...

    def is_allowed(self, workspace_id: str, user_id: str) -> bool: ...

    def set_allowed(
        self,
        workspace_id: str,
        user_id: str,
        allowed: bool,
        display_name: str,
        requested_by: str,
        ordering_key: str,
    ) -> BaseAccessMutationResult: ...

    def seed_members(
        self,
        workspace_id: str,
        members: Sequence[BaseAccessSeedMember],
        requested_by: str,
    ) -> bool: ...


def slack_ts_to_ordering_key(slack_ts: str) -> str:
    """Slack ts를 문자열 정렬만으로 비교 가능한 고정 폭 키로 바꾼다."""

    match = _SLACK_TS_RE.fullmatch(str(slack_ts or "").strip())
    if match is None:
        raise ValidationError("Slack ts 형식이 올바르지 않아")
    seconds = match.group("seconds")
    fraction = (match.group("fraction") or "").ljust(6, "0")
    return f"{int(seconds):020d}.{fraction}"


def build_base_access_store(settings: BaseAccessSettings) -> LocalFileBaseAccessStore:
    """Slack 서버의 단일 로컬 JSON 파일을 권한 정본으로 사용한다."""

    if not isinstance(settings, BaseAccessSettings):
        raise ConfigurationError("기본 권한 설정 형식이 올바르지 않아")
    raw_path = str(settings.state_path or "").strip()
    if not raw_path or "\x00" in raw_path:
        raise ConfigurationError("기본 권한 상태 파일 경로가 올바르지 않아")
    try:
        state_path = Path(raw_path).expanduser()
        if not state_path.is_absolute():
            state_path = _PROJECT_ROOT / state_path
        state_path = state_path.resolve(strict=False)
    except (OSError, RuntimeError, ValueError):
        raise ConfigurationError("기본 권한 상태 파일 경로가 올바르지 않아") from None
    if state_path.name in {"", ".", ".."} or (state_path.exists() and state_path.is_dir()):
        raise ConfigurationError("기본 권한 상태 파일 경로가 올바르지 않아")
    return LocalFileBaseAccessStore(state_path=state_path)


class LocalFileBaseAccessStore:
    """Slack 프로세스가 읽고 쓰는 작고 원자적인 membership 저장소."""

    def __init__(
        self,
        *,
        state_path: str | Path,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self._state_path = Path(state_path)
        self._now = now or (lambda: datetime.now(timezone.utc))

    @property
    def state_path(self) -> Path:
        return self._state_path

    def get_member(self, workspace_id: str, user_id: str) -> BaseAccessMember | None:
        workspace_id = _validate_identifier("workspace_id", workspace_id)
        user_id = _validate_identifier("user_id", user_id)
        with _PROCESS_LOCK:
            # 캐시하지 않고 매 요청마다 디스크의 정본을 다시 검증한다.
            state = self._read_state()
            return state.get(workspace_id, {}).get(user_id)

    def is_allowed(self, workspace_id: str, user_id: str) -> bool:
        member = self.get_member(workspace_id, user_id)
        return bool(member is not None and member.allowed)

    def set_allowed(
        self,
        workspace_id: str,
        user_id: str,
        allowed: bool,
        display_name: str,
        requested_by: str,
        ordering_key: str,
    ) -> BaseAccessMutationResult:
        workspace_id = _validate_identifier("workspace_id", workspace_id)
        user_id = _validate_identifier("user_id", user_id)
        display_name = _validate_display_name(display_name)
        requested_by = _validate_identifier("requested_by", requested_by)
        if not isinstance(allowed, bool):
            raise ValidationError("allowed 값은 bool이어야 해")
        ordering_key = _validate_ordering_key(ordering_key)

        with _PROCESS_LOCK:
            state = self._read_state()
            current = state.get(workspace_id, {}).get(user_id)
            if current is not None and current.ordering_key >= ordering_key:
                if current.ordering_key == ordering_key and current.allowed != allowed:
                    raise ConflictError("같은 순서의 권한 변경이 서로 충돌했어")
                return BaseAccessMutationResult(
                    allowed=current.allowed,
                    changed=False,
                    stale=current.ordering_key > ordering_key,
                    member=current,
                )

            member = BaseAccessMember(
                workspace_id=workspace_id,
                user_id=user_id,
                display_name=display_name,
                allowed=allowed,
                ordering_key=ordering_key,
                updated_at=_utc_text(self._now()),
                updated_by=requested_by,
            )
            state.setdefault(workspace_id, {})[user_id] = member
            self._write_state(state)
            return BaseAccessMutationResult(
                allowed=allowed,
                changed=current is None or current.allowed != allowed,
                stale=False,
                member=member,
            )

    def seed_members(
        self,
        workspace_id: str,
        members: Sequence[BaseAccessSeedMember],
        requested_by: str,
    ) -> bool:
        workspace_id = _validate_identifier("workspace_id", workspace_id)
        requested_by = _validate_identifier("requested_by", requested_by)
        normalized = _validate_seed_members(members)

        with _PROCESS_LOCK:
            try:
                state = self._read_state()
            except _StateFileMissing:
                updated_at = _utc_text(self._now())
                seeded = {
                    member.user_id: BaseAccessMember(
                        workspace_id=workspace_id,
                        user_id=member.user_id,
                        display_name=member.display_name,
                        allowed=True,
                        ordering_key=SEED_ORDERING_KEY,
                        updated_at=updated_at,
                        updated_by=requested_by,
                    )
                    for member in normalized
                }
                self._write_state({workspace_id: seeded})
                return True

            # 이미 모두 존재하면 revoke tombstone을 포함해 절대 되살리지 않는다.
            workspace = state.get(workspace_id, {})
            existing_count = sum(member.user_id in workspace for member in normalized)
            if existing_count == len(normalized):
                return False
            raise ConflictError("초기 사용자 전체가 이미 존재하는 상태가 아니야")

    def _read_state(self) -> dict[str, dict[str, BaseAccessMember]]:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self._state_path, flags)
        except FileNotFoundError:
            raise _StateFileMissing("기본 권한 상태 파일이 아직 없어") from None
        except OSError:
            raise StoreUnavailable("기본 권한 상태 파일을 읽을 수 없어") from None

        try:
            metadata = os.fstat(descriptor)
            if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600:
                raise StoreUnavailable("기본 권한 상태 파일 권한을 확인할 수 없어")
            with os.fdopen(descriptor, "r", encoding="utf-8") as state_file:
                descriptor = -1
                document = json.load(state_file)
        except StoreUnavailable:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError):
            raise StoreUnavailable("기본 권한 상태 파일 내용을 확인할 수 없어") from None
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        return _parse_state(document)

    def _write_state(self, state: dict[str, dict[str, BaseAccessMember]]) -> None:
        document = _serialize_state(state)
        parent = self._state_path.parent
        temporary_path: Path | None = None
        descriptor = -1
        try:
            # 임시 파일과 대상 파일을 같은 디렉터리에 둬 os.replace를 원자적으로 유지한다.
            parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            descriptor, raw_temporary_path = tempfile.mkstemp(
                dir=parent,
                prefix=f".{self._state_path.name}.",
                suffix=".tmp",
            )
            temporary_path = Path(raw_temporary_path)
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as state_file:
                descriptor = -1
                json.dump(
                    document,
                    state_file,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
                state_file.write("\n")
                state_file.flush()
                os.fsync(state_file.fileno())
            os.replace(temporary_path, self._state_path)
            temporary_path = None
            os.chmod(self._state_path, 0o600)
            _fsync_directory(parent)
        except OSError:
            raise StoreUnavailable("기본 권한 상태 파일을 저장할 수 없어") from None
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if temporary_path is not None:
                try:
                    temporary_path.unlink(missing_ok=True)
                except OSError:
                    pass


def _parse_state(document: object) -> dict[str, dict[str, BaseAccessMember]]:
    try:
        if not isinstance(document, dict) or set(document) != {"version", "workspaces"}:
            raise TypeError
        if type(document["version"]) is not int or document["version"] != _STATE_VERSION:
            raise ValueError
        raw_workspaces = document["workspaces"]
        if not isinstance(raw_workspaces, dict):
            raise TypeError

        state: dict[str, dict[str, BaseAccessMember]] = {}
        for raw_workspace_id, raw_workspace in raw_workspaces.items():
            workspace_id = _validate_identifier("workspace_id", raw_workspace_id)
            if not isinstance(raw_workspace, dict) or set(raw_workspace) != {"users"}:
                raise TypeError
            raw_users = raw_workspace["users"]
            if not isinstance(raw_users, dict):
                raise TypeError
            users: dict[str, BaseAccessMember] = {}
            for raw_user_id, raw_member in raw_users.items():
                user_id = _validate_identifier("user_id", raw_user_id)
                if not isinstance(raw_member, dict) or set(raw_member) != {
                    "allowed",
                    "displayName",
                    "orderingKey",
                    "updatedAt",
                    "updatedBy",
                }:
                    raise TypeError
                allowed = raw_member["allowed"]
                if not isinstance(allowed, bool):
                    raise TypeError
                users[user_id] = BaseAccessMember(
                    workspace_id=workspace_id,
                    user_id=user_id,
                    display_name=_validate_display_name(raw_member["displayName"]),
                    allowed=allowed,
                    ordering_key=_validate_ordering_key(raw_member["orderingKey"]),
                    updated_at=_require_text(raw_member["updatedAt"]),
                    updated_by=_validate_identifier("updated_by", raw_member["updatedBy"]),
                )
            state[workspace_id] = users
        return state
    except (KeyError, TypeError, ValueError, ValidationError):
        raise StoreUnavailable("기본 권한 상태 파일 내용을 확인할 수 없어") from None


def _serialize_state(
    state: dict[str, dict[str, BaseAccessMember]],
) -> dict[str, object]:
    return {
        "version": _STATE_VERSION,
        "workspaces": {
            workspace_id: {
                "users": {
                    user_id: {
                        "allowed": member.allowed,
                        "displayName": member.display_name,
                        "orderingKey": member.ordering_key,
                        "updatedAt": member.updated_at,
                        "updatedBy": member.updated_by,
                    }
                    for user_id, member in users.items()
                }
            }
            for workspace_id, users in state.items()
        },
    }


def _validate_identifier(label: str, value: object) -> str:
    normalized = _require_text(value)
    if len(normalized) > 128 or "#" in normalized or any(ord(char) < 32 for char in normalized):
        raise ValidationError(f"{label} 값이 올바르지 않아")
    return normalized


def _validate_display_name(value: object) -> str:
    normalized = _require_text(value)
    if len(normalized) > 80 or any(ord(char) < 32 for char in normalized):
        raise ValidationError("display_name 값이 올바르지 않아")
    return normalized


def _validate_ordering_key(value: object) -> str:
    normalized = str(value or "").strip()
    if _ORDERING_KEY_RE.fullmatch(normalized) is None:
        raise ValidationError("ordering_key 형식이 올바르지 않아")
    return normalized


def _validate_seed_members(members: Sequence[BaseAccessSeedMember]) -> list[BaseAccessSeedMember]:
    if not members or len(members) > 100:
        raise ValidationError("초기 사용자 수가 올바르지 않아")
    normalized: list[BaseAccessSeedMember] = []
    seen: set[str] = set()
    for member in members:
        if not isinstance(member, BaseAccessSeedMember):
            raise ValidationError("초기 사용자 형식이 올바르지 않아")
        user_id = _validate_identifier("user_id", member.user_id)
        if user_id in seen:
            raise ValidationError("초기 사용자 ID가 중복됐어")
        seen.add(user_id)
        normalized.append(
            BaseAccessSeedMember(
                user_id=user_id,
                display_name=_validate_display_name(member.display_name),
            )
        )
    return normalized


def _require_text(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError("필수 문자열 값이 비어 있어")
    return value.strip()


def _utc_text(value: datetime) -> str:
    if not isinstance(value, datetime):
        raise StoreUnavailable("현재 시각을 확인할 수 없어")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _fsync_directory(path: Path) -> None:
    descriptor = -1
    try:
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        os.fsync(descriptor)
    except OSError:
        raise StoreUnavailable("기본 권한 상태 파일을 저장할 수 없어") from None
    finally:
        if descriptor >= 0:
            os.close(descriptor)
