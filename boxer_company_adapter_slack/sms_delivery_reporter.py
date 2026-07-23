import fcntl
import json
import logging
import os
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from boxer.core.utils import _display_value
from boxer_company import settings as cs
from boxer_company.device_health_sheet import (
    _SMS_SHEET_ACCEPTED,
    _SMS_SHEET_CONFIRM_REQUIRED,
    _SMS_SHEET_DELIVERED,
    _SMS_SHEET_DELIVERY_FAILED,
    _append_device_health_sheet_alerts,
    _has_device_health_sheet_sms_tracking_group_id,
    _load_device_health_sheet_sms_delivery_matches,
    _load_device_health_sheet_pending_sms_deliveries,
    _update_device_health_sheet_sms_status_by_group_id,
)
from boxer_company.sms_delivery import (
    _SMS_DELIVERY_ACCEPTED,
    _SMS_DELIVERY_CONFIRM_REQUIRED,
    _SMS_DELIVERY_DELIVERED,
    _SMS_DELIVERY_FAILED,
    _load_solapi_group_info,
    _resolve_solapi_group_delivery_status,
)

_SMS_DELIVERY_REPORTER_THREAD: threading.Thread | None = None
_SMS_DELIVERY_REPORTER_THREAD_LOCK = threading.Lock()
_SMS_DELIVERY_OUTBOX_THREAD_LOCK = threading.RLock()
_SMS_DELIVERY_RECONCILE_THREAD_LOCK = threading.Lock()
_SMS_DELIVERY_OUTBOX_VERSION = 1
_SMS_DELIVERY_OUTBOX_ALLOWED_KEYS = {
    "device",
    "hospital",
    "room",
    "components",
    "issue",
    "smsDeliveryStatus",
    "smsGroupId",
    "detectedAt",
    "smsAcceptedAt",
    "storedAt",
    "permalink",
}
_SMS_DELIVERY_OUTBOX_STATUSES = {
    _SMS_DELIVERY_ACCEPTED,
    _SMS_DELIVERY_DELIVERED,
    _SMS_DELIVERY_FAILED,
    _SMS_DELIVERY_CONFIRM_REQUIRED,
}
_SMS_DELIVERY_FINAL_STATUSES = {
    _SMS_DELIVERY_DELIVERED,
    _SMS_DELIVERY_FAILED,
    _SMS_DELIVERY_CONFIRM_REQUIRED,
}
_SMS_DELIVERY_FINAL_SHEET_STATUSES = {
    _SMS_SHEET_DELIVERED,
    _SMS_SHEET_DELIVERY_FAILED,
    _SMS_SHEET_CONFIRM_REQUIRED,
}
_SMS_DELIVERY_SHEET_STATUS_BY_RESULT = {
    _SMS_DELIVERY_ACCEPTED: _SMS_SHEET_ACCEPTED,
    _SMS_DELIVERY_DELIVERED: _SMS_SHEET_DELIVERED,
    _SMS_DELIVERY_FAILED: _SMS_SHEET_DELIVERY_FAILED,
    _SMS_DELIVERY_CONFIRM_REQUIRED: _SMS_SHEET_CONFIRM_REQUIRED,
}
_KST = ZoneInfo("Asia/Seoul")


def _sms_delivery_outbox_path(
    outbox_path: str | Path | None = None,
) -> Path:
    return Path(outbox_path or cs.SMS_DELIVERY_OUTBOX_PATH).expanduser()


def _coerce_sms_delivery_datetime(
    value: datetime | str | None,
    *,
    fallback: datetime | None = None,
) -> datetime:
    parsed: datetime | None = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        raw_value = value.strip()
        try:
            parsed = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
    if parsed is None:
        parsed = fallback
    if parsed is None:
        raise ValueError("문자 발송 outbox 감지 시각이 올바르지 않아")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_KST)
    return parsed.astimezone(timezone.utc)


def _sms_delivery_datetime_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _normalize_sms_delivery_outbox_item(
    value: Any,
    *,
    detected_at: datetime | str | None = None,
    sms_accepted_at: datetime | str | None = None,
    stored_at: datetime | str | None = None,
    permalink: str | None = None,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("문자 발송 outbox 항목이 객체가 아니야")

    group_id = _display_value(value.get("smsGroupId"), default="")
    if not group_id:
        raise ValueError("문자 발송 outbox 항목의 groupId가 비어 있어")
    delivery_status = _display_value(
        value.get("smsDeliveryStatus"),
        default=_SMS_DELIVERY_ACCEPTED,
    )
    if delivery_status not in _SMS_DELIVERY_OUTBOX_STATUSES:
        raise ValueError("문자 발송 outbox 상태가 올바르지 않아")

    raw_components = value.get("components")
    if not isinstance(raw_components, list):
        raw_components = value.get("problemComponents")
    components: list[str] = []
    if isinstance(raw_components, list):
        for component in raw_components:
            normalized_component = _display_value(component, default="")
            if normalized_component and normalized_component not in components:
                components.append(normalized_component)

    detected_value = value.get("detectedAt")
    normalized_detected_at = _coerce_sms_delivery_datetime(
        detected_value
        if detected_value is not None and detected_value != ""
        else detected_at
    )
    accepted_value = value.get("smsAcceptedAt")
    normalized_accepted_at = _coerce_sms_delivery_datetime(
        sms_accepted_at
        if sms_accepted_at is not None and sms_accepted_at != ""
        else accepted_value,
        # 기존 outbox에는 별도 접수 시각이 없으므로 감지 시각을 최초 접수 시각으로 승격한다.
        fallback=normalized_detected_at,
    )
    stored_value = value.get("storedAt")
    normalized_stored_at = _coerce_sms_delivery_datetime(
        stored_at
        if stored_at is not None and stored_at != ""
        else stored_value,
        # 기존 outbox의 repair grace도 무한히 연장하지 않도록 감지 시각으로 호환한다.
        fallback=normalized_detected_at,
    )
    normalized_permalink = _display_value(
        permalink
        if permalink is not None
        else value.get("permalink") or value.get("slackPermalink"),
        default="",
    )
    # 공급자 호출 payload 전체를 받아도 전화번호·문자본문 등은 이 allowlist 밖이라 저장하지 않는다.
    return {
        "device": _display_value(value.get("device"), default="장비명 미확인"),
        "hospital": _display_value(
            value.get("hospitalName") or value.get("hospital"),
            default="병원 미확인",
        ),
        "room": _display_value(value.get("room"), default="병실 미확인"),
        "components": components,
        "issue": _display_value(value.get("issue"), default="상세 확인 필요"),
        "smsDeliveryStatus": delivery_status,
        "smsGroupId": group_id,
        "detectedAt": _sms_delivery_datetime_text(normalized_detected_at),
        "smsAcceptedAt": _sms_delivery_datetime_text(normalized_accepted_at),
        "storedAt": _sms_delivery_datetime_text(normalized_stored_at),
        "permalink": normalized_permalink,
    }


def _merge_sms_delivery_outbox_items(
    current: dict[str, Any],
    incoming: dict[str, Any],
) -> dict[str, Any]:
    current_status = _display_value(current.get("smsDeliveryStatus"), default="")
    incoming_status = _display_value(incoming.get("smsDeliveryStatus"), default="")
    delivery_status = (
        current_status
        if current_status in _SMS_DELIVERY_FINAL_STATUSES
        else incoming_status
    )
    current_detected_at = _coerce_sms_delivery_datetime(current.get("detectedAt"))
    incoming_detected_at = _coerce_sms_delivery_datetime(incoming.get("detectedAt"))
    current_accepted_at = _coerce_sms_delivery_datetime(
        current.get("smsAcceptedAt"),
        fallback=current_detected_at,
    )
    incoming_accepted_at = _coerce_sms_delivery_datetime(
        incoming.get("smsAcceptedAt"),
        fallback=incoming_detected_at,
    )
    current_stored_at = _coerce_sms_delivery_datetime(
        current.get("storedAt"),
        fallback=current_detected_at,
    )
    incoming_stored_at = _coerce_sms_delivery_datetime(
        incoming.get("storedAt"),
        fallback=incoming_detected_at,
    )
    merged = dict(current)
    for key in ("device", "hospital", "room", "issue", "permalink"):
        incoming_value = _display_value(incoming.get(key), default="")
        if incoming_value:
            merged[key] = incoming_value
    incoming_components = incoming.get("components")
    if isinstance(incoming_components, list) and incoming_components:
        merged["components"] = list(incoming_components)
    merged["smsDeliveryStatus"] = delivery_status
    merged["smsGroupId"] = incoming["smsGroupId"]
    merged["detectedAt"] = _sms_delivery_datetime_text(
        min(current_detected_at, incoming_detected_at)
    )
    # 최초 접수 시각은 고정하고, 최신 context 저장 시각부터 repair grace를 다시 시작한다.
    merged["smsAcceptedAt"] = _sms_delivery_datetime_text(
        min(current_accepted_at, incoming_accepted_at)
    )
    merged["storedAt"] = _sms_delivery_datetime_text(
        max(current_stored_at, incoming_stored_at)
    )
    return {
        key: merged[key]
        for key in _SMS_DELIVERY_OUTBOX_ALLOWED_KEYS
    }


@contextmanager
def _locked_sms_delivery_outbox_file(
    path: Path,
) -> Iterator[None]:
    lock_path = path.with_name(f"{path.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


def _read_sms_delivery_outbox_unlocked(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        # 손상 파일을 빈 outbox로 덮으면 발송 추적 정보가 영구 유실되므로 복구 전까지 중단한다.
        raise RuntimeError(f"문자 발송 outbox를 읽지 못했어: {path}") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("items"), list):
        raise RuntimeError(f"문자 발송 outbox 형식이 올바르지 않아: {path}")

    normalized_by_group_id: dict[str, dict[str, Any]] = {}
    for raw_item in payload["items"]:
        item = _normalize_sms_delivery_outbox_item(raw_item)
        group_id = item["smsGroupId"]
        current = normalized_by_group_id.get(group_id)
        normalized_by_group_id[group_id] = (
            _merge_sms_delivery_outbox_items(current, item)
            if current is not None
            else item
        )
    return sorted(
        normalized_by_group_id.values(),
        key=lambda item: (item["detectedAt"], item["smsGroupId"]),
    )


def _write_sms_delivery_outbox_unlocked(
    path: Path,
    items: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": _SMS_DELIVERY_OUTBOX_VERSION,
        "items": sorted(
            items,
            key=lambda item: (item["detectedAt"], item["smsGroupId"]),
        ),
    }
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
            json.dump(
                payload,
                temp_file,
                ensure_ascii=True,
                indent=2,
                sort_keys=True,
            )
            temp_file.write("\n")
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, path)
        temp_path = None
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            # 일부 파일시스템은 디렉터리 fsync를 지원하지 않아도 파일 원자 교체 자체는 유지된다.
            pass
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()


def _mutate_sms_delivery_outbox(
    mutator: Any,
    *,
    outbox_path: str | Path | None = None,
) -> Any:
    path = _sms_delivery_outbox_path(outbox_path)
    with _SMS_DELIVERY_OUTBOX_THREAD_LOCK:
        with _locked_sms_delivery_outbox_file(path):
            items = _read_sms_delivery_outbox_unlocked(path)
            result = mutator(items)
            _write_sms_delivery_outbox_unlocked(path, items)
            return result


def _load_sms_delivery_outbox_items(
    *,
    outbox_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    path = _sms_delivery_outbox_path(outbox_path)
    # 빈 outbox 조회만으로 data 디렉터리와 lock 파일을 만들지는 않는다.
    if not path.exists():
        return []
    with _SMS_DELIVERY_OUTBOX_THREAD_LOCK:
        with _locked_sms_delivery_outbox_file(path):
            return _read_sms_delivery_outbox_unlocked(path)


def remember_sms_delivery_sheet_record(
    alert_item: dict[str, Any],
    *,
    detected_at: datetime | str,
    sms_accepted_at: datetime | str | None = None,
    permalink: str | None = None,
    outbox_path: str | Path | None = None,
) -> bool:
    if not isinstance(alert_item, dict):
        return False
    delivery_status = _display_value(
        alert_item.get("smsDeliveryStatus"),
        default="",
    )
    if (
        delivery_status not in _SMS_DELIVERY_OUTBOX_STATUSES
        or not _display_value(alert_item.get("smsGroupId"), default="")
    ):
        return False
    stored_at = datetime.now(timezone.utc)
    actual_sms_accepted_at = sms_accepted_at
    if actual_sms_accepted_at is None or (
        isinstance(actual_sms_accepted_at, str)
        and not actual_sms_accepted_at.strip()
    ):
        actual_sms_accepted_at = alert_item.get("smsAcceptedAt") or stored_at
    incoming = _normalize_sms_delivery_outbox_item(
        alert_item,
        detected_at=detected_at,
        sms_accepted_at=actual_sms_accepted_at,
        stored_at=stored_at,
        permalink=permalink,
    )

    def _upsert(items: list[dict[str, Any]]) -> None:
        for index, current in enumerate(items):
            if current["smsGroupId"] != incoming["smsGroupId"]:
                continue
            items[index] = _merge_sms_delivery_outbox_items(current, incoming)
            return
        items.append(incoming)

    _mutate_sms_delivery_outbox(_upsert, outbox_path=outbox_path)
    return True


def remember_accepted_sms_delivery(
    alert_item: dict[str, Any],
    *,
    detected_at: datetime | str,
    permalink: str | None = None,
    outbox_path: str | Path | None = None,
) -> bool:
    # 기존 호출자 호환용 API는 이름대로 접수 상태만 보존한다.
    if (
        not isinstance(alert_item, dict)
        or _display_value(alert_item.get("smsDeliveryStatus"), default="")
        != _SMS_DELIVERY_ACCEPTED
    ):
        return False
    return remember_sms_delivery_sheet_record(
        alert_item,
        detected_at=detected_at,
        permalink=permalink,
        outbox_path=outbox_path,
    )


def _set_sms_delivery_outbox_status(
    group_id: str,
    delivery_status: str,
    *,
    outbox_path: str | Path | None = None,
) -> bool:
    if delivery_status not in _SMS_DELIVERY_FINAL_STATUSES:
        raise ValueError("문자 발송 outbox 최종 상태가 올바르지 않아")

    def _set_status(items: list[dict[str, Any]]) -> bool:
        for item in items:
            if item["smsGroupId"] == group_id:
                item["smsDeliveryStatus"] = delivery_status
                return True
        return False

    return bool(
        _mutate_sms_delivery_outbox(
            _set_status,
            outbox_path=outbox_path,
        )
    )


def _remove_sms_delivery_outbox_item(
    group_id: str,
    *,
    outbox_path: str | Path | None = None,
) -> bool:
    def _remove(items: list[dict[str, Any]]) -> bool:
        original_count = len(items)
        items[:] = [
            item
            for item in items
            if item["smsGroupId"] != group_id
        ]
        return len(items) != original_count

    return bool(
        _mutate_sms_delivery_outbox(
            _remove,
            outbox_path=outbox_path,
        )
    )


def _load_device_health_sheet_sms_delivery_rows(
) -> dict[str, dict[str, Any]]:
    rows = _load_device_health_sheet_sms_delivery_matches() or []
    rows_by_group_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("장비 장애 시트 문자 추적 항목 형식이 올바르지 않아")
        group_id = _display_value(row.get("groupId"), default="")
        if not group_id:
            continue
        # Sheet 모듈이 R metadata와 B/F/Q identity hash로 고유성을 검증한 결과만 사용한다.
        rows_by_group_id[group_id] = dict(row)
    return rows_by_group_id


def _append_sms_delivery_outbox_item_to_sheet(
    item: dict[str, Any],
) -> bool:
    detected_at = _coerce_sms_delivery_datetime(item.get("detectedAt"))
    row_count = _append_device_health_sheet_alerts(
        [
            {
                "device": item["device"],
                "hospitalName": item["hospital"],
                "room": item["room"],
                "problemComponents": list(item.get("components") or []),
                "issue": item["issue"],
                "smsDeliveryStatus": item["smsDeliveryStatus"],
                "smsGroupId": item["smsGroupId"],
                "smsAcceptedAt": item["smsAcceptedAt"],
            }
        ],
        detected_at=detected_at,
        slack_permalink=_display_value(item.get("permalink"), default=""),
    )
    return row_count == 1


def _is_sms_delivery_outbox_item_expired(
    item: dict[str, Any],
    *,
    now: datetime,
) -> bool:
    return _is_sms_delivery_tracking_expired(
        item.get("smsAcceptedAt") or item.get("detectedAt"),
        now=now,
    )


def _is_sms_delivery_outbox_repair_ready(
    item: dict[str, Any],
    *,
    now: datetime,
) -> bool:
    stored_at = _coerce_sms_delivery_datetime(
        item.get("storedAt") or item.get("detectedAt")
    )
    grace = timedelta(
        seconds=max(0, int(cs.SMS_DELIVERY_OUTBOX_REPAIR_GRACE_SEC))
    )
    return now - stored_at >= grace


def _is_sms_delivery_tracking_expired(
    accepted_at: datetime | str | None,
    *,
    now: datetime,
) -> bool:
    detected_at = _coerce_sms_delivery_datetime(accepted_at)
    max_age = timedelta(
        hours=max(1, int(cs.SOLAPI_DELIVERY_REPORT_MAX_AGE_HOURS))
    )
    return now - detected_at >= max_age


def _is_solapi_group_permanently_missing(exc: Exception) -> bool:
    response = getattr(exc, "response", None)
    try:
        status_code = int(getattr(response, "status_code", 0) or 0)
    except (TypeError, ValueError):
        return False
    # 인증·rate limit·provider 장애는 재시도하고, 보관 내역 자체가 없는 경우만 확정 불가로 닫는다.
    return status_code in {404, 410}


@contextmanager
def _try_sms_delivery_reconcile_lock(
    *,
    outbox_path: str | Path | None = None,
) -> Iterator[bool]:
    if not _SMS_DELIVERY_RECONCILE_THREAD_LOCK.acquire(blocking=False):
        yield False
        return

    lock_file: Any | None = None
    process_locked = False
    try:
        path = _sms_delivery_outbox_path(outbox_path)
        lock_path = path.with_name(f"{path.name}.reconcile.lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock_file = lock_path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(
                lock_file.fileno(),
                fcntl.LOCK_EX | fcntl.LOCK_NB,
            )
            process_locked = True
        except BlockingIOError:
            yield False
            return
        yield True
    finally:
        if process_locked and lock_file is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
        if lock_file is not None:
            lock_file.close()
        _SMS_DELIVERY_RECONCILE_THREAD_LOCK.release()


def _reconcile_sms_delivery_outbox_once(
    logger: logging.Logger,
    *,
    now: datetime | None = None,
    outbox_path: str | Path | None = None,
) -> int:
    actual_now = _coerce_sms_delivery_datetime(
        now,
        fallback=datetime.now(timezone.utc),
    )
    if not _load_sms_delivery_outbox_items(outbox_path=outbox_path):
        return 0
    with _try_sms_delivery_reconcile_lock(
        outbox_path=outbox_path
    ) as acquired:
        if not acquired:
            return 0

        items = _load_sms_delivery_outbox_items(outbox_path=outbox_path)
        if not items:
            return 0
        sheet_rows = _load_device_health_sheet_sms_delivery_rows()
        changed_count = 0

        for snapshot_item in items:
            item = dict(snapshot_item)
            group_id = item["smsGroupId"]
            sheet_row = sheet_rows.get(group_id)
            if (
                sheet_row is not None
                and sheet_row.get("smsStatus") in _SMS_DELIVERY_FINAL_SHEET_STATUSES
            ):
                # 재시작 직전에 이미 최종 H열까지 반영된 경우 provider를 다시 조회하지 않는다.
                if _remove_sms_delivery_outbox_item(
                    group_id,
                    outbox_path=outbox_path,
                ):
                    changed_count += 1
                continue

            delivery_status = item["smsDeliveryStatus"]
            if delivery_status == _SMS_DELIVERY_ACCEPTED:
                if _is_sms_delivery_outbox_item_expired(item, now=actual_now):
                    delivery_status = _SMS_DELIVERY_CONFIRM_REQUIRED
                else:
                    try:
                        resolved_status = _resolve_solapi_group_delivery_status(
                            _load_solapi_group_info(group_id)
                        )
                    except Exception as exc:
                        if _is_solapi_group_permanently_missing(exc):
                            resolved_status = _SMS_DELIVERY_CONFIRM_REQUIRED
                        else:
                            # rate limit·5xx·네트워크 장애는 실패로 오판하지 않고 다음 poll에서 재시도한다.
                            logger.warning(
                                "Solapi 문자 최종 결과를 조회하지 못했어 "
                                "group_id=%s error_type=%s",
                                group_id,
                                type(exc).__name__,
                            )
                            resolved_status = None
                    if resolved_status in _SMS_DELIVERY_FINAL_STATUSES:
                        delivery_status = resolved_status

                if delivery_status in _SMS_DELIVERY_FINAL_STATUSES:
                    # provider 확인과 Sheet 쓰기 사이에 재시작해도 결과를 잃지 않게 먼저 outbox에 확정한다.
                    _set_sms_delivery_outbox_status(
                        group_id,
                        delivery_status,
                        outbox_path=outbox_path,
                    )
                    item["smsDeliveryStatus"] = delivery_status

            desired_sheet_status = _SMS_DELIVERY_SHEET_STATUS_BY_RESULT[
                item["smsDeliveryStatus"]
            ]
            if sheet_row is None:
                if not _is_sms_delivery_outbox_repair_ready(
                    item,
                    now=actual_now,
                ):
                    # producer가 Slack permalink를 병합하고 원본 append를 마칠 시간을 먼저 보장한다.
                    continue
                try:
                    group_id_exists = (
                        _has_device_health_sheet_sms_tracking_group_id(group_id)
                    )
                except Exception as exc:
                    # R 존재 확인이 실패하면 중복 위험이 있으므로 재append하지 않는다.
                    logger.warning(
                        "문자 발송 추적 groupId를 Google Sheets에서 확인하지 못했어 "
                        "group_id=%s error_type=%s",
                        group_id,
                        type(exc).__name__,
                    )
                    continue
                if group_id_exists:
                    # identity match가 모호해도 R에 같은 groupId가 하나라도 있으면 중복 행을 만들지 않는다.
                    logger.warning(
                        "문자 발송 추적 행의 identity match가 모호해 재기록하지 않았어 "
                        "group_id=%s",
                        group_id,
                    )
                    continue
                try:
                    appended = _append_sms_delivery_outbox_item_to_sheet(item)
                except Exception as exc:
                    # append timeout 뒤 실제 반영됐을 수도 있으므로 outbox를 지우지 않고 다음 poll에서 재스캔한다.
                    logger.warning(
                        "문자 발송 장애 행을 Google Sheets에 재기록하지 못했어 "
                        "group_id=%s error_type=%s",
                        group_id,
                        type(exc).__name__,
                    )
                    continue
                if not appended:
                    continue
                changed_count += 1
                # 최종 상태로 직접 append한 경우에도 실제 H/R 반영 확인 뒤에만 outbox를 제거한다.
                try:
                    refreshed_rows = _load_device_health_sheet_sms_delivery_rows()
                except Exception as exc:
                    logger.warning(
                        "재기록한 문자 발송 장애 행을 확인하지 못했어 "
                        "group_id=%s error_type=%s",
                        group_id,
                        type(exc).__name__,
                    )
                    continue
                sheet_rows.update(refreshed_rows)
                sheet_row = sheet_rows.get(group_id)
                if sheet_row is None:
                    continue

            if item["smsDeliveryStatus"] == _SMS_DELIVERY_ACCEPTED:
                # 접수 행은 최종 provider 결과가 나올 때까지 outbox에 남긴다.
                continue
            if sheet_row.get("smsStatus") == desired_sheet_status:
                if _remove_sms_delivery_outbox_item(
                    group_id,
                    outbox_path=outbox_path,
                ):
                    changed_count += 1
                continue
            if sheet_row.get("smsStatus") != _SMS_SHEET_ACCEPTED:
                logger.warning(
                    "문자 최종 결과와 시트 상태가 달라 자동 갱신하지 않았어 "
                    "group_id=%s sheet_status=%s desired_status=%s",
                    group_id,
                    sheet_row.get("smsStatus"),
                    desired_sheet_status,
                )
                continue

            try:
                updated = _update_device_health_sheet_sms_status_by_group_id(
                    row_number=int(sheet_row["rowNumber"]),
                    group_id=group_id,
                    sms_status=desired_sheet_status,
                )
            except Exception as exc:
                logger.warning(
                    "문자 최종 결과를 Google Sheets에 갱신하지 못했어 "
                    "sheet_row=%s error_type=%s",
                    sheet_row["rowNumber"],
                    type(exc).__name__,
                )
                continue
            if not updated:
                continue
            sheet_row["smsStatus"] = desired_sheet_status
            changed_count += 1
            # 최종 H열 쓰기 성공 뒤에만 outbox에서 제거한다.
            _remove_sms_delivery_outbox_item(
                group_id,
                outbox_path=outbox_path,
            )
            logger.info(
                "Updated SMS delivery result sheet_row=%s status=%s",
                sheet_row["rowNumber"],
                desired_sheet_status,
            )
        return changed_count


def _run_sms_delivery_reporter_once(
    logger: logging.Logger,
    *,
    now: datetime | None = None,
) -> int:
    actual_now = _coerce_sms_delivery_datetime(
        now,
        fallback=datetime.now(timezone.utc),
    )
    updated_count = _reconcile_sms_delivery_outbox_once(
        logger,
        now=actual_now,
    )
    active_outbox_group_ids = {
        item["smsGroupId"]
        for item in _load_sms_delivery_outbox_items()
    }
    pending_deliveries = _load_device_health_sheet_pending_sms_deliveries()
    if not pending_deliveries:
        return updated_count

    group_results: dict[str, str | None] = {}
    failed_group_ids: set[str] = set()
    for pending_delivery in pending_deliveries:
        if not isinstance(pending_delivery, dict):
            continue
        group_id = str(pending_delivery.get("groupId") or "").strip()
        row_number = int(pending_delivery.get("rowNumber") or 0)
        if not group_id or row_number < 2:
            continue
        # outbox 항목은 재append·max-age까지 포함한 경로에서 이미 처리했으므로 중복 조회하지 않는다.
        if group_id in active_outbox_group_ids:
            continue

        accepted_at = pending_delivery.get("acceptedAt")
        try:
            if accepted_at and _is_sms_delivery_tracking_expired(
                accepted_at,
                now=actual_now,
            ):
                delivery_status = _SMS_DELIVERY_CONFIRM_REQUIRED
            else:
                if group_id in failed_group_ids:
                    continue
                if group_id not in group_results:
                    group_results[group_id] = _resolve_solapi_group_delivery_status(
                        _load_solapi_group_info(group_id)
                    )
                delivery_status = group_results[group_id]
        except Exception as exc:
            if _is_solapi_group_permanently_missing(exc):
                delivery_status = _SMS_DELIVERY_CONFIRM_REQUIRED
            else:
                # 조회 일시 실패는 수신 실패로 오판하지 않고 접수 상태를 유지해 다음 poll에서 재시도한다.
                failed_group_ids.add(group_id)
                logger.warning(
                    "Solapi 문자 최종 결과를 조회하지 못했어 "
                    "sheet_row=%s error_type=%s",
                    row_number,
                    type(exc).__name__,
                )
                continue

        sheet_status = _SMS_DELIVERY_SHEET_STATUS_BY_RESULT.get(delivery_status)
        if not sheet_status:
            continue
        try:
            updated = _update_device_health_sheet_sms_status_by_group_id(
                row_number=row_number,
                group_id=group_id,
                sms_status=sheet_status,
            )
        except Exception as exc:
            # 시트 갱신 실패 시 H열이 접수됨으로 남아 다음 poll에서 자연스럽게 재시도된다.
            logger.warning(
                "문자 최종 결과를 Google Sheets에 갱신하지 못했어 "
                "sheet_row=%s error_type=%s",
                row_number,
                type(exc).__name__,
            )
            continue
        if updated:
            updated_count += 1
            logger.info(
                "Updated SMS delivery result sheet_row=%s status=%s",
                row_number,
                sheet_status,
            )
    return updated_count


def _sms_delivery_reporter_loop(logger: logging.Logger) -> None:
    poll_interval_sec = max(
        10,
        int(cs.SOLAPI_DELIVERY_REPORT_POLL_INTERVAL_SEC),
    )
    while True:
        try:
            _run_sms_delivery_reporter_once(logger)
        except Exception as exc:
            logger.warning(
                "문자 최종 결과 확인 중 오류가 발생했어 error_type=%s",
                type(exc).__name__,
            )
        time.sleep(poll_interval_sec)


def attach_sms_delivery_reporter(*, logger: logging.Logger | None = None) -> None:
    if (
        not cs.DEVICE_HEALTH_SHEET_ENABLED
        or str(cs.DEVICE_HEALTH_MONITOR_SMS_PROVIDER or "").strip().lower() != "solapi"
        or not cs.SOLAPI_API_KEY
        or not cs.SOLAPI_API_SECRET
    ):
        return

    actual_logger = logger or logging.getLogger(__name__)
    global _SMS_DELIVERY_REPORTER_THREAD
    with _SMS_DELIVERY_REPORTER_THREAD_LOCK:
        if (
            _SMS_DELIVERY_REPORTER_THREAD is not None
            and _SMS_DELIVERY_REPORTER_THREAD.is_alive()
        ):
            return
        _SMS_DELIVERY_REPORTER_THREAD = threading.Thread(
            target=_sms_delivery_reporter_loop,
            args=(actual_logger,),
            name="boxer-sms-delivery-reporter",
            daemon=True,
        )
        _SMS_DELIVERY_REPORTER_THREAD.start()
    actual_logger.info(
        "Started SMS delivery reporter interval=%ss",
        max(10, int(cs.SOLAPI_DELIVERY_REPORT_POLL_INTERVAL_SEC)),
    )
