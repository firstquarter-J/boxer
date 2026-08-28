from __future__ import annotations

from datetime import datetime
import logging
import re
from typing import Any

from boxer_company import settings as cs


_MDA_RECOVERY_ALERT_TITLE = (
    "업로드 실패한 마미박스 초음파 영상을 모두 낚았습니다!"
)
_MDA_RECOVERY_ALERT_FIELDS = frozenset(
    {"바코드", "촬영일", "병원명", "병실명", "장비명", "파일명"}
)
_MDA_RECOVERY_ALERT_FIELD_LINE_PATTERN = re.compile(
    r"^[ \t]*(바코드|촬영일|병원명|병실명|장비명|파일명)"
    r"[ \t]*:[ \t]*\[([^\]\r\n]+)\][ \t]*$"
)
_MDA_RECOVERY_ALERT_FIELD_PREFIX_PATTERN = re.compile(
    r"^[ \t]*(바코드|촬영일|병원명|병실명|장비명|파일명)[ \t]*:"
)
_MDA_RECOVERY_ALERT_FILE_NAME_PATTERN = re.compile(
    r"^[A-Za-z0-9._-]{1,255}$"
)


def _parse_mda_recovery_alert_text(
    text: str,
) -> dict[str, str] | None:
    """Slack root의 고정 MDA 복구 포맷만 신뢰 가능한 scope로 읽는다."""

    lines = str(text or "").strip().splitlines()
    if not lines:
        return None
    if lines[0].strip().strip("*").strip() != _MDA_RECOVERY_ALERT_TITLE:
        return None

    fields: dict[str, str] = {}
    for line in lines[1:]:
        prefix_match = _MDA_RECOVERY_ALERT_FIELD_PREFIX_PATTERN.match(line)
        if not prefix_match:
            continue
        field_match = _MDA_RECOVERY_ALERT_FIELD_LINE_PATTERN.fullmatch(line)
        if not field_match:
            return None
        field_name = field_match.group(1)
        if field_name in fields:
            return None
        fields[field_name] = field_match.group(2).strip()

    if frozenset(fields) != _MDA_RECOVERY_ALERT_FIELDS:
        return None
    if not re.fullmatch(r"\d{11}", fields["바코드"]):
        return None
    try:
        datetime.strptime(fields["촬영일"], "%Y-%m-%d:%H:%M:%S")
    except ValueError:
        return None
    if not (
        1 <= len(fields["병원명"]) <= 200
        and 1 <= len(fields["병실명"]) <= 200
        and fields["병원명"].isprintable()
        and fields["병실명"].isprintable()
    ):
        return None
    if not (
        len(fields["장비명"]) <= 64
        and cs.S3_DEVICE_NAME_PATTERN.fullmatch(fields["장비명"])
    ):
        return None
    if not _MDA_RECOVERY_ALERT_FILE_NAME_PATTERN.fullmatch(fields["파일명"]):
        return None
    return fields


def _is_mda_recovery_alert_from_current_bot(
    client: Any,
    root_message: dict[str, Any],
    logger: logging.Logger,
) -> bool:
    """root 작성자가 현재 Slack bot identity와 정확히 같은지 검증한다."""

    if root_message.get("type") != "message":
        return False
    subtype = str(root_message.get("subtype") or "").strip()
    if subtype and subtype != "bot_message":
        return False
    try:
        auth = client.auth_test()
    except Exception as exc:
        logger.warning(
            "Failed to validate MDA recovery alert author error_type=%s",
            type(exc).__name__,
        )
        return False
    if not hasattr(auth, "get"):
        return False

    current_user_id = str(auth.get("user_id") or "").strip()
    current_bot_id = str(auth.get("bot_id") or "").strip()
    if not current_user_id or not current_bot_id or auth.get("ok") is False:
        return False
    bot_profile = root_message.get("bot_profile")
    if not isinstance(bot_profile, dict):
        bot_profile = {}
    root_user_ids = {
        str(value).strip()
        for value in (root_message.get("user"), bot_profile.get("user_id"))
        if str(value or "").strip()
    }
    root_bot_ids = {
        str(value).strip()
        for value in (root_message.get("bot_id"), bot_profile.get("id"))
        if str(value or "").strip()
    }
    return root_user_ids == {current_user_id} and root_bot_ids == {
        current_bot_id
    }


def lookup_device_file_scope_from_mda_recovery_thread(
    *,
    client: Any,
    logger: logging.Logger,
    channel_id: str,
    thread_ts: str,
    requested_barcode: str,
    requested_date: str,
) -> list[dict[str, Any]]:
    """API operation에 보낼 MDA 복구 root의 Slack scope만 반환한다."""

    trusted_channel_id = str(
        cs.DEVICE_NOTIFICATION_ALERT_CHANNEL_ID or ""
    ).strip()
    if (
        not client
        or not trusted_channel_id
        or channel_id != trusted_channel_id
        or not thread_ts
    ):
        return []
    try:
        response = client.conversations_replies(
            channel=channel_id,
            ts=thread_ts,
            limit=1,
            inclusive=True,
        )
        messages = response.get("messages") or []
    except Exception as exc:
        logger.warning(
            "Failed to load MDA recovery alert root error_type=%s",
            type(exc).__name__,
        )
        return []

    root_message = next(
        (
            message
            for message in messages
            if isinstance(message, dict)
            and str(message.get("ts") or "") == str(thread_ts)
        ),
        None,
    )
    if not root_message or not _is_mda_recovery_alert_from_current_bot(
        client,
        root_message,
        logger,
    ):
        return []
    fields = _parse_mda_recovery_alert_text(
        str(root_message.get("text") or "")
    )
    if not fields:
        return []
    if (
        fields["바코드"] != requested_barcode
        or fields["촬영일"][:10] != requested_date
    ):
        return []
    return [
        {
            "deviceName": fields["장비명"],
            "hospitalName": fields["병원명"],
            "roomName": fields["병실명"],
        }
    ]


__all__ = ["lookup_device_file_scope_from_mda_recovery_thread"]
