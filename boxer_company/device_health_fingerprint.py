from __future__ import annotations

import re
from typing import Any, Mapping


_LEGACY_HOSPITAL_LABEL_PATTERN = re.compile(r"^#(?P<seq>[1-9][0-9]*)\s+(?P<name>.+)$")
_API_HOSPITAL_LABEL_PATTERN = re.compile(r"^(?P<name>.+?)\s+\(#(?P<seq>[1-9][0-9]*)\)$")


def canonical_device_health_alert_fingerprint(
    item: Mapping[str, Any],
) -> str:
    """Slack/API 표시 라벨과 무관한 기존-compatible alert key를 만든다."""

    raw_label = _text(item.get("hospital"))
    parsed_seq, parsed_name = _parse_hospital_label(raw_label)
    hospital_seq = _positive_int(item.get("hospitalSeq")) or parsed_seq
    hospital_name = _text(item.get("hospitalName")) or parsed_name
    if hospital_seq is not None:
        hospital_key = f"#{hospital_seq} {hospital_name}".strip()
    else:
        hospital_key = hospital_name or raw_label
    return "|".join(
        (
            hospital_key,
            _text(item.get("room")),
            _text(item.get("device")),
            _text(item.get("issue")),
        )
    )


def canonicalize_device_health_alert_fingerprint_key(value: Any) -> str:
    """저장된 두 병원 라벨 포맷을 같은 canonical fingerprint로 바꾼다."""

    raw_key = _text(value)
    parts = raw_key.split("|", 3)
    if len(parts) != 4:
        return raw_key
    return canonical_device_health_alert_fingerprint(
        {
            "hospital": parts[0],
            "room": parts[1],
            "device": parts[2],
            "issue": parts[3],
        }
    )


def validate_and_canonicalize_device_health_alert_fingerprint_key(
    value: Any,
) -> str:
    """migration seed는 빈 축 없는 exact 4-part fingerprint만 받는다."""

    if not isinstance(value, str):
        raise ValueError("device health fingerprint key is invalid")
    raw_key = value.strip()
    parts = raw_key.split("|")
    if len(parts) != 4 or any(not part.strip() for part in parts):
        raise ValueError("device health fingerprint key is invalid")
    canonical_key = canonicalize_device_health_alert_fingerprint_key(raw_key)
    if len(canonical_key.split("|")) != 4:
        raise ValueError("device health fingerprint key is invalid")
    return canonical_key


def _parse_hospital_label(value: str) -> tuple[int | None, str]:
    for pattern in (
        _LEGACY_HOSPITAL_LABEL_PATTERN,
        _API_HOSPITAL_LABEL_PATTERN,
    ):
        match = pattern.fullmatch(value)
        if match is not None:
            return int(match.group("seq")), _text(match.group("name"))
    return None, value


def _positive_int(value: Any) -> int | None:
    try:
        number = int(str(value or "").strip())
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _text(value: Any) -> str:
    return str(value or "").strip()


__all__ = [
    "canonical_device_health_alert_fingerprint",
    "canonicalize_device_health_alert_fingerprint_key",
    "validate_and_canonicalize_device_health_alert_fingerprint_key",
]
