from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import tempfile
import threading
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import UUID, uuid5

from boxer_company import settings as cs
from boxer_company_adapter_slack.automation_api_client import (
    AutomationRemoteReceipt,
    CompanyAutomationApiClient,
)
from boxer_company_adapter_slack.company_api_client import (
    COMPANY_AUTOMATION_CYCLES,
    CompanyApiContractError,
)


_STATE_VERSION = 1
_IDENTIFIER_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$"
)
_STATE_LOCK = threading.RLock()
_DELIVERY_MESSAGE_NAMESPACE = UUID("1fd6588a-f7fb-53cf-9216-463095231edd")


@dataclass(frozen=True, slots=True)
class AutomationSlackDelivery:
    """Slack POST 성공 뒤 API에 돌려줄 channel delivery receipt다."""

    delivery_id: str
    external_message_id: str
    permalink: str
    delivered_at: datetime


def build_automation_request_id(
    *,
    cycle: str,
    cycle_key: str,
    scheduled_at: datetime,
    phase: Literal["run", "ack"] = "run",
    receipt_ids: tuple[str, ...] = (),
) -> str:
    """같은 논리 실행에는 같은 correlation ID가 나오도록 해시한다."""

    if scheduled_at.tzinfo is None:
        raise CompanyApiContractError(
            "company_api_automation_scheduled_at_invalid"
        )
    raw = "\0".join(
        (
            cycle,
            cycle_key,
            scheduled_at.isoformat(),
            phase,
            *sorted(receipt_ids),
        )
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"automation:{cycle}:{phase}:{digest}"


def build_automation_delivery_client_msg_id(
    *,
    cycle: str,
    cycle_key: str,
    delivery_id: str,
    part: str,
) -> str:
    """Slack 재호출도 같은 메시지로 dedupe되도록 결정적 UUID를 만든다."""

    _validate_cycle_identity(cycle, cycle_key)
    if (
        not _IDENTIFIER_PATTERN.fullmatch(str(delivery_id or ""))
        or not _IDENTIFIER_PATTERN.fullmatch(str(part or ""))
    ):
        raise CompanyApiContractError(
            "company_api_automation_delivery_identity_invalid"
        )
    raw = "\0".join((cycle, cycle_key, delivery_id, part))
    return str(uuid5(_DELIVERY_MESSAGE_NAMESPACE, raw))


def remember_automation_delivery(
    *,
    cycle: str,
    cycle_key: str,
    delivery: AutomationSlackDelivery,
    state_path: str | Path | None = None,
) -> None:
    """도메인 payload 없이 Slack 발송 성공 사실만 crash-safe하게 저장한다."""

    _validate_cycle_identity(cycle, cycle_key)
    _validate_delivery(delivery)
    path = _delivery_state_path(state_path)
    with _STATE_LOCK:
        document = _load_document(path)
        cycles = dict(document["cycles"])
        raw_cycle = cycles.get(cycle)
        cycle_state = dict(raw_cycle) if isinstance(raw_cycle, dict) else {}
        stored_cycle_key = str(cycle_state.get("cycleKey") or "").strip()
        receipts = [
            dict(item)
            for item in (cycle_state.get("receipts") or [])
            if isinstance(item, dict)
        ]
        if stored_cycle_key and stored_cycle_key != cycle_key and receipts:
            # 이전 delivery를 잃으면 API pending mutation을 다시 실행할 수 있어
            # 새 주기의 receipt로 덮지 않고 운영 확인을 요구한다.
            raise CompanyApiContractError(
                "company_api_automation_delivery_state_conflict"
            )
        serialized = _serialize_delivery(delivery)
        by_id = {
            str(item.get("deliveryId") or ""): item
            for item in receipts
            if str(item.get("deliveryId") or "").strip()
        }
        previous = by_id.get(delivery.delivery_id)
        if previous is not None and previous != serialized:
            raise CompanyApiContractError(
                "company_api_automation_delivery_state_conflict"
            )
        by_id[delivery.delivery_id] = serialized
        cycles[cycle] = {
            "cycleKey": cycle_key,
            "receipts": list(by_id.values()),
        }
        _write_document(path, {"version": _STATE_VERSION, "cycles": cycles})


def remember_automation_deliveries(
    *,
    cycle: str,
    cycle_key: str,
    deliveries: tuple[AutomationSlackDelivery, ...],
    state_path: str | Path | None = None,
) -> None:
    """한 Slack 메시지가 대표하는 여러 domain delivery를 원자 저장한다."""

    _validate_cycle_identity(cycle, cycle_key)
    if not deliveries:
        return
    for delivery in deliveries:
        _validate_delivery(delivery)
    delivery_ids = [delivery.delivery_id for delivery in deliveries]
    if len(delivery_ids) != len(set(delivery_ids)):
        raise CompanyApiContractError(
            "company_api_automation_delivery_identity_invalid"
        )

    path = _delivery_state_path(state_path)
    with _STATE_LOCK:
        document = _load_document(path)
        cycles = dict(document["cycles"])
        raw_cycle = cycles.get(cycle)
        cycle_state = dict(raw_cycle) if isinstance(raw_cycle, dict) else {}
        stored_cycle_key = str(cycle_state.get("cycleKey") or "").strip()
        receipts = [
            dict(item)
            for item in (cycle_state.get("receipts") or [])
            if isinstance(item, dict)
        ]
        if stored_cycle_key and stored_cycle_key != cycle_key and receipts:
            raise CompanyApiContractError(
                "company_api_automation_delivery_state_conflict"
            )
        by_id = {
            str(item.get("deliveryId") or ""): item
            for item in receipts
            if str(item.get("deliveryId") or "").strip()
        }
        for delivery in deliveries:
            serialized = _serialize_delivery(delivery)
            previous = by_id.get(delivery.delivery_id)
            if previous is not None and previous != serialized:
                raise CompanyApiContractError(
                    "company_api_automation_delivery_state_conflict"
                )
            by_id[delivery.delivery_id] = serialized
        cycles[cycle] = {
            "cycleKey": cycle_key,
            "receipts": list(by_id.values()),
        }
        # 집계 메시지 성공 후 일부 receipt만 남는 crash window가
        # 생기지 않게 한 번의 replace로 전체를 저장한다.
        _write_document(path, {"version": _STATE_VERSION, "cycles": cycles})


def flush_automation_deliveries(
    api_client: CompanyAutomationApiClient,
    *,
    cycle: str,
    cycle_key: str,
    scheduled_at: datetime,
    options: dict[str, Any] | None = None,
    logger: logging.Logger | None = None,
    state_path: str | Path | None = None,
) -> bool:
    """저장된 sent receipt를 API hook에 한 번 전달하고 성공 뒤에만 지운다."""

    _validate_cycle_identity(cycle, cycle_key)
    path = _delivery_state_path(state_path)
    with _STATE_LOCK:
        document = _load_document(path)
        cycle_state = document["cycles"].get(cycle)
        if not isinstance(cycle_state, dict):
            return False
        stored_cycle_key = str(cycle_state.get("cycleKey") or "").strip()
        raw_receipts = cycle_state.get("receipts") or []
        if not raw_receipts:
            return False
        if not _IDENTIFIER_PATTERN.fullmatch(stored_cycle_key):
            raise CompanyApiContractError(
                "company_api_automation_delivery_state_invalid"
            )
        deliveries = tuple(
            _deserialize_delivery(item)
            for item in raw_receipts
            if isinstance(item, dict)
        )

    receipt_ids = tuple(item.delivery_id for item in deliveries)
    request_id = build_automation_request_id(
        cycle=cycle,
        # 장애가 다음 schedule window까지 이어졌다면 새 key가 아니라
        # receipt가 실제로 속한 과거 key를 먼저 ack한다.
        cycle_key=stored_cycle_key,
        scheduled_at=scheduled_at,
        phase="ack",
        receipt_ids=receipt_ids,
    )
    api_client.run(
        request_id=request_id,
        cycle=cycle,
        cycle_key=stored_cycle_key,
        scheduled_at=scheduled_at,
        options=options or {},
        receipts=tuple(
            AutomationRemoteReceipt(
                delivery_id=item.delivery_id,
                status="sent",
                external_message_id=item.external_message_id,
                permalink=item.permalink,
                delivered_at=item.delivered_at,
            )
            for item in deliveries
        ),
        ack_only=True,
    )

    with _STATE_LOCK:
        latest = _load_document(path)
        cycles = dict(latest["cycles"])
        latest_state = cycles.get(cycle)
        if not isinstance(latest_state, dict):
            raise CompanyApiContractError(
                "company_api_automation_delivery_state_changed"
            )
        latest_receipts = [
            dict(item)
            for item in (latest_state.get("receipts") or [])
            if isinstance(item, dict)
        ]
        acknowledged_ids = set(receipt_ids)
        remaining = [
            item
            for item in latest_receipts
            if str(item.get("deliveryId") or "") not in acknowledged_ids
        ]
        cycles[cycle] = {
            "cycleKey": stored_cycle_key,
            "receipts": remaining,
        }
        _write_document(path, {"version": _STATE_VERSION, "cycles": cycles})
    (logger or logging.getLogger(__name__)).info(
        "Acknowledged automation Slack deliveries cycle=%s count=%s",
        cycle,
        len(deliveries),
    )
    return True


def _delivery_state_path(value: str | Path | None) -> Path:
    raw_value = str(
        value
        if value is not None
        else cs.AUTOMATION_DELIVERY_STATE_PATH
    ).strip()
    if not raw_value:
        raise CompanyApiContractError(
            "company_api_automation_delivery_state_path_invalid"
        )
    path = Path(raw_value).expanduser()
    if (
        not path.is_absolute()
        or path == Path("/")
        or (path.exists() and path.is_dir())
    ):
        raise CompanyApiContractError(
            "company_api_automation_delivery_state_path_invalid"
        )
    return path


def _load_document(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": _STATE_VERSION, "cycles": {}}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise CompanyApiContractError(
            "company_api_automation_delivery_state_unreadable"
        ) from exc
    if (
        not isinstance(payload, dict)
        or payload.get("version") != _STATE_VERSION
        or not isinstance(payload.get("cycles"), dict)
    ):
        raise CompanyApiContractError(
            "company_api_automation_delivery_state_invalid"
        )
    return payload


def _write_document(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(
                payload,
                handle,
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            )
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary_path, 0o600)
        os.replace(temporary_path, path)
        directory_fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def _validate_cycle_identity(cycle: str, cycle_key: str) -> None:
    if (
        cycle not in COMPANY_AUTOMATION_CYCLES
        or not _IDENTIFIER_PATTERN.fullmatch(cycle_key)
    ):
        raise CompanyApiContractError(
            "company_api_automation_delivery_identity_invalid"
        )


def _validate_delivery(delivery: AutomationSlackDelivery) -> None:
    parsed_permalink = (
        urlsplit(delivery.permalink) if delivery.permalink else None
    )
    if (
        not _IDENTIFIER_PATTERN.fullmatch(delivery.delivery_id)
        or (
            delivery.external_message_id
            and not _IDENTIFIER_PATTERN.fullmatch(
                delivery.external_message_id
            )
        )
        or delivery.delivered_at.tzinfo is None
        or (
            parsed_permalink is not None
            and not (
                parsed_permalink.scheme == "https"
                and parsed_permalink.hostname
                and parsed_permalink.username is None
                and parsed_permalink.password is None
                and not parsed_permalink.query
                and not parsed_permalink.fragment
            )
        )
    ):
        raise CompanyApiContractError(
            "company_api_automation_delivery_receipt_invalid"
        )


def _serialize_delivery(delivery: AutomationSlackDelivery) -> dict[str, str]:
    return {
        "deliveryId": delivery.delivery_id,
        "externalMessageId": delivery.external_message_id,
        "permalink": delivery.permalink,
        "deliveredAt": delivery.delivered_at.isoformat(),
    }


def _deserialize_delivery(value: dict[str, Any]) -> AutomationSlackDelivery:
    try:
        delivered_at = datetime.fromisoformat(
            str(value.get("deliveredAt") or "")
        )
    except ValueError as exc:
        raise CompanyApiContractError(
            "company_api_automation_delivery_state_invalid"
        ) from exc
    delivery = AutomationSlackDelivery(
        delivery_id=str(value.get("deliveryId") or ""),
        external_message_id=str(value.get("externalMessageId") or ""),
        permalink=str(value.get("permalink") or ""),
        delivered_at=delivered_at,
    )
    _validate_delivery(delivery)
    return delivery


__all__ = [
    "AutomationSlackDelivery",
    "build_automation_request_id",
    "build_automation_delivery_client_msg_id",
    "flush_automation_deliveries",
    "remember_automation_delivery",
]
