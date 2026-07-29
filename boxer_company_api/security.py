from __future__ import annotations

import os
from pathlib import Path
from typing import Mapping

_EC2_HINT_FILES: tuple[tuple[Path, tuple[str, ...]], ...] = (
    (Path("/sys/hypervisor/uuid"), ("ec2",)),
    (Path("/sys/devices/virtual/dmi/id/product_uuid"), ("ec2",)),
    (Path("/sys/devices/virtual/dmi/id/product_name"), ("amazon ec2",)),
    (Path("/sys/devices/virtual/dmi/id/board_vendor"), ("amazon ec2",)),
    (Path("/sys/devices/virtual/dmi/id/sys_vendor"), ("amazon ec2",)),
)

_FORBIDDEN_EC2_AWS_ENV_KEYS: tuple[str, ...] = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
    "AWS_SHARED_CREDENTIALS_FILE",
    "AWS_CONFIG_FILE",
)


def _read_runtime_hint(path: Path) -> str:
    try:
        return path.read_text(
            encoding="utf-8",
            errors="ignore",
        ).strip().lower()
    except OSError:
        return ""


def _looks_like_ec2_runtime() -> bool:
    return any(
        value and any(marker in value for marker in markers)
        for path, markers in _EC2_HINT_FILES
        if (value := _read_runtime_hint(path))
    )


def _find_forbidden_ec2_aws_env_keys(
    env: Mapping[str, str | None] | None = None,
) -> list[str]:
    source = os.environ if env is None else env
    return [
        key
        for key in _FORBIDDEN_EC2_AWS_ENV_KEYS
        if str(source.get(key) or "").strip()
    ]


def validate_company_api_runtime_security(
    *,
    env: Mapping[str, str | None] | None = None,
    is_ec2: bool | None = None,
) -> None:
    """EC2에서는 SDK 기본 체인과 인스턴스 역할만 사용하도록 강제한다."""

    runtime_is_ec2 = _looks_like_ec2_runtime() if is_ec2 is None else bool(is_ec2)
    if not runtime_is_ec2:
        return

    forbidden_keys = _find_forbidden_ec2_aws_env_keys(env)
    if forbidden_keys:
        raise RuntimeError(
            "EC2 runtime contains forbidden static AWS credential settings"
        )
