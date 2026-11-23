from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

from boxer_company.deployment import get_timezone_label
from boxer_company_adapter_slack.device_alert_slack import (
    _CATEGORY_TITLES,
    _normalize_status_emoji,
)
from boxer_company_adapter_slack.recording_media_context import _field_label

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
_SLACK_TIMESTAMP = re.compile(r"[0-9]{1,20}(?:\.[0-9]{1,9})?")
_DEVICE_LINK = re.compile(r"<https://[^|>\s]+\|([^|>\r\n]+)>")
_UPLOAD_ALERT_TITLE = _CATEGORY_TITLES["upload"].removesuffix(" 감지")
_RECORDING_ALERT_TITLES = frozenset(
    _CATEGORY_TITLES[category].removesuffix(" 감지")
    for category in ("upload", "recording_video", "recording_audio")
)
_RECORDING_ALERT_FIELDS = {
    "장비": "device",
    "장비명": "device",
    "바코드": "barcode",
    "세션 시작": "session",
    "녹화 시작": "session",
    "세션 시작(추정)": "session",
}


@dataclass(frozen=True, slots=True)
class AlertDownloadScope:
    barcode: str
    log_date: str
    device_name: str


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


def _is_alert_from_current_bot(
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
    except Exception as exc:  # noqa: BLE001 - Slack SDK 예외의 상세 원문은 남기지 않는다.
        logger.warning(
            "Failed to validate device alert author error_type=%s",
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


def _lookup_alert_root(
    *,
    client: Any,
    logger: logging.Logger,
    channel_id: str,
    thread_ts: str,
) -> dict[str, Any] | None:
    trusted_channel_id = str(
        cs.DEVICE_NOTIFICATION_ALERT_CHANNEL_ID or ""
    ).strip()
    if (
        not client
        or not trusted_channel_id
        or channel_id != trusted_channel_id
        or not thread_ts
    ):
        return None
    try:
        response = client.conversations_replies(
            channel=channel_id,
            ts=thread_ts,
            limit=1,
            inclusive=True,
        )
        messages = response.get("messages") or []
    except Exception as exc:  # noqa: BLE001 - Slack SDK 예외의 상세 원문은 남기지 않는다.
        logger.warning(
            "Failed to load device alert root error_type=%s",
            type(exc).__name__,
        )
        return None

    if not isinstance(messages, list):
        return None
    roots = [
        message
        for message in messages
        if isinstance(message, dict)
        and str(message.get("ts") or "") == thread_ts
        and message.get("thread_ts") in (None, "", thread_ts)
    ]
    if len(roots) != 1 or not _is_alert_from_current_bot(client, roots[0], logger):
        return None
    return roots[0]


def _parse_recording_alert_blocks(root: dict[str, Any]) -> AlertDownloadScope | None:
    blocks = root.get("blocks")
    if not isinstance(blocks, list) or not blocks or len(blocks) > 50:
        return None
    header = blocks[0]
    if not isinstance(header, dict) or header.get("type") != "header":
        return None
    title_text = header.get("text")
    if not isinstance(title_text, dict) or not isinstance(title_text.get("text"), str):
        return None
    title = (
        _normalize_status_emoji(title_text["text"])
        .removeprefix(":alert: ")
        .removeprefix("✅ 확인 완료 · ")
        .removesuffix(" 감지")
    )
    if title not in _RECORDING_ALERT_TITLES:
        return None

    identity: dict[str, str] = {}
    for block in blocks[1:]:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "header":
            return None
        fields = block.get("fields")
        if block.get("type") != "section" or not isinstance(fields, list):
            continue
        # 식별값을 여러 카드에서 이어 붙이지 않고 같은 section에서만 읽는다.
        section_identity: dict[str, str] = {}
        for field in fields:
            field_text = field.get("text") if isinstance(field, dict) else None
            if not isinstance(field_text, str):
                continue
            label = _field_label(field_text)
            key = _RECORDING_ALERT_FIELDS.get(label)
            if key is None:
                continue
            if identity or key in section_identity:
                return None
            if label == "세션 시작(추정)" and title != _UPLOAD_ALERT_TITLE:
                return None
            section_identity[key] = field_text.partition("\n")[2].strip()
        if section_identity:
            identity = section_identity

    device_name = identity.get("device", "").strip("`*")
    device_link = _DEVICE_LINK.fullmatch(device_name)
    if device_link:
        device_name = device_link.group(1)
    barcode = identity.get("barcode", "").strip("`*")
    if not (
        re.fullmatch(r"[0-9]{11}", barcode)
        and 3 <= len(device_name) <= 64
        and cs.S3_DEVICE_NAME_PATTERN.fullmatch(device_name)
    ):
        return None
    session = identity.get("session", "").removesuffix(f" {get_timezone_label()}")
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}", session):
        return None
    try:
        log_date = datetime.strptime(session, "%Y-%m-%d %H:%M:%S").date().isoformat()
    except ValueError:
        return None
    return AlertDownloadScope(barcode, log_date, device_name)


def lookup_alert_download_scope(
    *,
    client: Any,
    logger: logging.Logger,
    channel_id: str,
    thread_ts: str | None,
    current_ts: str | None,
) -> AlertDownloadScope | None:
    """검증된 알림에 달린 답글에서 다운로드에 필요한 식별값만 가져온다."""

    if (
        not isinstance(thread_ts, str)
        or not isinstance(current_ts, str)
        or not _SLACK_TIMESTAMP.fullmatch(thread_ts)
        or not _SLACK_TIMESTAMP.fullmatch(current_ts)
        or Decimal(thread_ts) >= Decimal(current_ts)
    ):
        return None
    root = _lookup_alert_root(
        client=client, logger=logger, channel_id=channel_id, thread_ts=thread_ts
    )
    if root is None:
        return None
    fields = _parse_mda_recovery_alert_text(str(root.get("text") or ""))
    if fields is not None:
        return AlertDownloadScope(fields["바코드"], fields["촬영일"][:10], fields["장비명"])
    return _parse_recording_alert_blocks(root)


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

    root_message = _lookup_alert_root(
        client=client, logger=logger, channel_id=channel_id, thread_ts=thread_ts
    )
    if root_message is None:
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


__all__ = [
    "AlertDownloadScope",
    "lookup_alert_download_scope",
    "lookup_device_file_scope_from_mda_recovery_thread",
]
