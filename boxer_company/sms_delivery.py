import hashlib
import hmac
import secrets
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

import requests

from boxer.core.utils import _display_value
from boxer_company import settings as cs

_SMS_DELIVERY_ACCEPTED = "accepted"
_SMS_DELIVERY_DELIVERED = "delivered"
_SMS_DELIVERY_FAILED = "delivery_failed"
_SMS_DELIVERY_NOT_SENT = "not_sent"
_SMS_DELIVERY_REQUEST_FAILED = "request_failed"
_SMS_DELIVERY_CONFIRM_REQUIRED = "confirm_required"
_SOLAPI_PENDING_GROUP_STATUSES = {
    "PENDING",
    "SENDING",
    "PROCESSING",
    "SCHEDULED",
}
_SOLAPI_FAILED_GROUP_STATUSES = {
    "FAILED",
    "DELETED",
    "SYSTEM-ERROR",
}


def _build_solapi_authorization_header() -> str:
    date_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    salt = secrets.token_hex(16)
    signature = hmac.new(
        cs.SOLAPI_API_SECRET.encode("utf-8"),
        f"{date_time}{salt}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return (
        "HMAC-SHA256 "
        f"apiKey={cs.SOLAPI_API_KEY}, date={date_time}, salt={salt}, signature={signature}"
    )


def _load_solapi_group_info(
    group_id: str,
    *,
    timeout: int | None = None,
) -> dict[str, Any]:
    normalized_group_id = _display_value(group_id, default="")
    if not normalized_group_id:
        raise ValueError("Solapi groupId가 비어 있어")
    if not cs.SOLAPI_API_KEY or not cs.SOLAPI_API_SECRET:
        raise RuntimeError("Solapi 조회 자격증명이 설정되지 않았어")

    url = (
        f"{cs.SOLAPI_BASE_URL.rstrip('/')}/messages/v4/groups/"
        f"{quote(normalized_group_id, safe='')}"
    )
    response = requests.get(
        url,
        headers={"Authorization": _build_solapi_authorization_header()},
        timeout=max(
            1,
            int(
                timeout
                if timeout is not None
                else cs.DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_TIMEOUT_SEC
            ),
        ),
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Solapi 그룹 조회 응답이 객체가 아니야")
    return payload


def _resolve_solapi_group_delivery_status(group_info: dict[str, Any]) -> str | None:
    if not isinstance(group_info, dict):
        raise ValueError("Solapi 그룹 정보가 객체가 아니야")

    group_status = _display_value(group_info.get("status"), default="").upper()
    count = group_info.get("count") if isinstance(group_info.get("count"), dict) else {}

    def _count(name: str) -> int:
        try:
            return max(0, int(count.get(name) or 0))
        except (TypeError, ValueError):
            return 0

    sent_success = _count("sentSuccess")
    sent_failed = _count("sentFailed")
    sent_pending = _count("sentPending")
    registered_failed = _count("registeredFailed")

    if group_status in _SOLAPI_FAILED_GROUP_STATUSES:
        return _SMS_DELIVERY_FAILED
    if sent_failed > 0 or registered_failed > 0:
        return _SMS_DELIVERY_FAILED
    if group_status == "COMPLETE":
        if sent_success > 0 and sent_pending == 0:
            return _SMS_DELIVERY_DELIVERED
        # COMPLETE인데 성공·실패 통계가 비어 있으면 실제 결과를 단정하지 않는다.
        return _SMS_DELIVERY_CONFIRM_REQUIRED
    if group_status in _SOLAPI_PENDING_GROUP_STATUSES or sent_pending > 0:
        return None
    # 알 수 없는 신규 상태는 오판하지 않고 다음 poll에서 다시 확인한다.
    return None
