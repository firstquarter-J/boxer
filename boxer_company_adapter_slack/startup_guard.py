import os
from pathlib import Path
from typing import Mapping

_EC2_HINT_FILES: tuple[tuple[Path, tuple[str, ...]], ...] = (
    (Path("/sys/hypervisor/uuid"), ("ec2",)),
    (Path("/sys/devices/virtual/dmi/id/product_uuid"), ("ec2",)),
    (Path("/sys/devices/virtual/dmi/id/product_name"), ("amazon ec2",)),
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
        return path.read_text(encoding="utf-8", errors="ignore").strip().lower()
    except OSError:
        return ""


def _looks_like_ec2_runtime() -> bool:
    for path, markers in _EC2_HINT_FILES:
        value = _read_runtime_hint(path)
        if value and any(marker in value for marker in markers):
            return True
    return False


def _find_forbidden_ec2_aws_env_keys(
    env: Mapping[str, str | None] | None = None,
) -> list[str]:
    source = os.environ if env is None else env
    return [
        key
        for key in _FORBIDDEN_EC2_AWS_ENV_KEYS
        if str(source.get(key) or "").strip()
    ]


def _validate_ec2_runtime_aws_env(
    *,
    env: Mapping[str, str | None] | None = None,
    is_ec2: bool | None = None,
) -> None:
    runtime_is_ec2 = _looks_like_ec2_runtime() if is_ec2 is None else bool(is_ec2)
    if not runtime_is_ec2:
        return

    forbidden_keys = _find_forbidden_ec2_aws_env_keys(env)
    if not forbidden_keys:
        return

    raise RuntimeError(
        "EC2 운영 런타임에서는 AWS 정적 자격증명 env를 허용하지 않아. "
        f"문제 키: {', '.join(forbidden_keys)}. "
        "EC2 .env에서 제거하고 인스턴스 역할을 사용해."
    )
