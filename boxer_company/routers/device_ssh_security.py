from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import ipaddress
import os
from pathlib import Path
import stat
import threading
from typing import Any, Iterator

from boxer_company import settings as cs


_API_DEVICE_SSH_STRICT: ContextVar[bool] = ContextVar(
    "boxer_company_api_device_ssh_strict",
    default=False,
)
_ENDPOINT_DEVICE_NAMES: dict[tuple[str, int], str] = {}
_ENDPOINT_DEVICE_NAMES_LOCK = threading.Lock()
_MAX_KNOWN_HOSTS_BYTES = 4 * 1024 * 1024


class DeviceSshSecurityError(RuntimeError):
    """SSH credential을 보내기 전에 fail-closed한 안전한 오류 코드다."""


@dataclass(slots=True)
class CompanyApiDeviceSshRequestState:
    """한 API request 안에서 실제 외부 side effect 전송만 기록한다."""

    mutation_attempted: bool = False
    open_attempted: bool = False


_API_DEVICE_SSH_REQUEST_STATE: ContextVar[
    CompanyApiDeviceSshRequestState | None
] = ContextVar(
    "boxer_company_api_device_ssh_request_state",
    default=None,
)


@contextmanager
def company_api_device_ssh_context(
) -> Iterator[CompanyApiDeviceSshRequestState]:
    """공통 API runtime에서만 strict host identity 검증을 강제한다."""

    strict_token = _API_DEVICE_SSH_STRICT.set(True)
    state = CompanyApiDeviceSshRequestState()
    state_token = _API_DEVICE_SSH_REQUEST_STATE.set(state)
    try:
        yield state
    finally:
        _API_DEVICE_SSH_REQUEST_STATE.reset(state_token)
        _API_DEVICE_SSH_STRICT.reset(strict_token)


def _mark_company_api_mutation_attempted() -> None:
    """실제 외부 mutation 전송 직전에 현재 API request state를 표시한다."""

    state = _API_DEVICE_SSH_REQUEST_STATE.get()
    if state is not None:
        state.mutation_attempted = True


def _mark_company_api_device_ssh_open_attempted() -> None:
    """실제 GraphQL 전송 직전에 현재 API request의 mutation 시도를 남긴다."""

    state = _API_DEVICE_SSH_REQUEST_STATE.get()
    if state is not None:
        state.mutation_attempted = True
        state.open_attempted = True


def _register_mda_ssh_endpoint_device(
    device_name: str,
    host: object,
    port: object,
) -> None:
    normalized_device = str(device_name or "").strip()
    normalized_host = str(host or "").strip().casefold()
    try:
        normalized_port = int(port)
    except (TypeError, ValueError):
        return
    if not normalized_device or not normalized_host or normalized_port <= 0:
        return
    with _ENDPOINT_DEVICE_NAMES_LOCK:
        _ENDPOINT_DEVICE_NAMES[(normalized_host, normalized_port)] = normalized_device


def _resolve_mda_ssh_endpoint_device(host: object, port: object) -> str:
    normalized_host = str(host or "").strip().casefold()
    try:
        normalized_port = int(port)
    except (TypeError, ValueError):
        return ""
    with _ENDPOINT_DEVICE_NAMES_LOCK:
        return _ENDPOINT_DEVICE_NAMES.get(
            (normalized_host, normalized_port),
            "",
        )


def _prepare_device_ssh_client(
    client: Any,
    *,
    device_name: str,
    reported_host: str,
    port: int,
    paramiko_module: Any,
) -> str:
    """API는 pinned key/private endpoint만, Slack local은 기존 host를 쓴다."""

    if not _API_DEVICE_SSH_STRICT.get():
        client.set_missing_host_key_policy(paramiko_module.AutoAddPolicy())
        return reported_host

    normalized_device = str(device_name or "").strip()
    normalized_reported_host = str(reported_host or "").strip().casefold()
    allowed_hosts = {
        item.strip().casefold()
        for item in cs.BOXER_COMPANY_API_DEVICE_SSH_ALLOWED_HOSTS
        if item.strip()
    }
    connect_host = str(
        cs.BOXER_COMPANY_API_DEVICE_SSH_CONNECT_HOST or ""
    ).strip()
    known_hosts_path = Path(
        str(cs.BOXER_COMPANY_API_DEVICE_SSH_KNOWN_HOSTS_PATH or "").strip()
    )
    if (
        not normalized_device
        or normalized_reported_host not in allowed_hosts
        or not 1024 <= int(port) <= 65535
        or not connect_host
        or not known_hosts_path.is_absolute()
    ):
        raise DeviceSshSecurityError("device_ssh_policy_unavailable")
    try:
        connect_ip = ipaddress.ip_address(connect_host)
    except ValueError as exc:
        raise DeviceSshSecurityError("device_ssh_connect_host_invalid") from exc
    if not connect_ip.is_private:
        raise DeviceSshSecurityError("device_ssh_connect_host_invalid")

    _validate_known_hosts_path(known_hosts_path)
    entries = _load_known_hosts_entries(known_hosts_path)
    target_entries = [
        entry
        for entry in entries
        if entry[0] == normalized_device
    ]
    if (
        len(target_entries) != 1
        or target_entries[0][1] != "ssh-ed25519"
    ):
        raise DeviceSshSecurityError("device_ssh_host_key_missing")
    # 서로 다른 장비가 같은 골든이미지 host key를 복제한 경우 endpoint가
    # 바뀌어도 검증을 통과하므로 전체 정본에서 public key 중복도 거부한다.
    public_keys = [entry[2] for entry in entries]
    if len(public_keys) != len(set(public_keys)):
        raise DeviceSshSecurityError("device_ssh_host_key_duplicate")

    host_keys = paramiko_module.HostKeys()
    try:
        host_keys.load(str(known_hosts_path))
    except Exception as exc:
        raise DeviceSshSecurityError("device_ssh_known_hosts_invalid") from exc
    target_keys = host_keys.lookup(normalized_device) or {}
    if set(target_keys) != {"ssh-ed25519"}:
        raise DeviceSshSecurityError("device_ssh_host_key_missing")
    endpoint_key = f"[{connect_host}]:{int(port)}"
    client.get_host_keys().add(
        endpoint_key,
        "ssh-ed25519",
        target_keys["ssh-ed25519"],
    )
    client.set_missing_host_key_policy(paramiko_module.RejectPolicy())
    return connect_host


def _validate_known_hosts_path(path: Path) -> None:
    try:
        path_stat = os.lstat(path)
    except OSError as exc:
        raise DeviceSshSecurityError("device_ssh_known_hosts_unavailable") from exc
    if (
        stat.S_ISLNK(path_stat.st_mode)
        or not stat.S_ISREG(path_stat.st_mode)
        or path_stat.st_uid != 0
        or path_stat.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        or path_stat.st_size <= 0
        or path_stat.st_size > _MAX_KNOWN_HOSTS_BYTES
    ):
        raise DeviceSshSecurityError("device_ssh_known_hosts_unsafe")

    # leaf 교체를 막기 위해 루트까지 모든 parent가 root 소유이고
    # group/other writable이 아닌지 같은 syscall 기준으로 검사한다.
    for parent in path.parents:
        try:
            parent_stat = os.lstat(parent)
        except OSError as exc:
            raise DeviceSshSecurityError("device_ssh_known_hosts_unsafe") from exc
        if (
            stat.S_ISLNK(parent_stat.st_mode)
            or not stat.S_ISDIR(parent_stat.st_mode)
            or parent_stat.st_uid != 0
            or parent_stat.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        ):
            raise DeviceSshSecurityError("device_ssh_known_hosts_unsafe")


def _load_known_hosts_entries(path: Path) -> list[tuple[str, str, str]]:
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise DeviceSshSecurityError("device_ssh_known_hosts_invalid") from exc
    entries: list[tuple[str, str, str]] = []
    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        fields = stripped.split()
        if (
            len(fields) != 3
            or not fields[0]
            or "," in fields[0]
            or fields[1] != "ssh-ed25519"
            or not fields[2]
        ):
            raise DeviceSshSecurityError("device_ssh_known_hosts_invalid")
        entries.append((fields[0], fields[1], fields[2]))
    if not entries:
        raise DeviceSshSecurityError("device_ssh_known_hosts_invalid")
    return entries


__all__ = [
    "CompanyApiDeviceSshRequestState",
    "DeviceSshSecurityError",
    "_mark_company_api_device_ssh_open_attempted",
    "_mark_company_api_mutation_attempted",
    "_prepare_device_ssh_client",
    "_register_mda_ssh_endpoint_device",
    "_resolve_mda_ssh_endpoint_device",
    "company_api_device_ssh_context",
]
