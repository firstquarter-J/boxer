"""Solapi 최종 상태와 장비 장애 Sheet를 조정하는 채널 중립 cycle."""

import fcntl
import hashlib
import json
import logging
import os
import re
import stat
import tempfile
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from boxer_company.utils import _display_value
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

_SMS_DELIVERY_OUTBOX_THREAD_LOCK = threading.RLock()
_SMS_DELIVERY_RECONCILE_THREAD_LOCK = threading.Lock()
# 자동 alert producer 둘은 legacy Slack reporter와 같은 process-memory
# claim을 공유한다. durable claim 파일은 cutover/recovery 호환용으로만 남긴다.
_SMS_AUTOMATION_RUNTIME_CLAIMS_LOCK = threading.Lock()
_SMS_AUTOMATION_RUNTIME_CLAIMS: dict[str, dict[str, Any]] = {}
_SMS_DELIVERY_OUTBOX_VERSION = 1
_SMS_AUTOMATION_CLAIM_VERSION = 2
_SMS_AUTOMATION_CLAIM_WINDOW_SEC = 60
_SMS_AUTOMATION_CLAIM_STATES = {
    "pending",
    "accepted",
    "uncertain",
    "settled",
}
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
_SMS_RECOVERY_MAX_FILE_BYTES = 16 * 1024 * 1024
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


def _automatic_sms_runtime_incident_family(
    item: Mapping[str, Any],
) -> str:
    """legacy health reporter와 같은 장애군으로 두 producer를 묶는다."""

    alert_category = _display_value(
        item.get("alertCategory"),
        default="",
    )
    issue = _display_value(item.get("issue"), default="").lower()
    problem_components = {
        _display_value(component, default="").replace(" ", "")
        for component in (
            item.get("problemComponents")
            if isinstance(item.get("problemComponents"), (list, tuple))
            else []
        )
        if _display_value(component, default="")
    }
    if alert_category == "recording_processing" or any(
        marker in issue for marker in ("병합", "ffmpeg", "merge")
    ):
        return "recording_processing"
    if (
        alert_category in {"video_signal", "recording"}
        or "캡처보드" in problem_components
        or any(
            marker in issue
            for marker in (
                "캡처보드",
                "캡쳐보드",
                "비디오 장치",
                "영상 입력",
                "녹화 파일 증가 정지",
            )
        )
    ):
        return "captureboard_recording"
    if alert_category:
        return alert_category
    if problem_components:
        return "+".join(sorted(problem_components))
    return issue[:120]


def build_automatic_sms_runtime_claim_key(
    item: Mapping[str, Any],
) -> str:
    """병원·장비·장애군을 legacy process claim key로 만든다."""

    device_name = _display_value(item.get("device"), default="")
    incident_family = _automatic_sms_runtime_incident_family(item)
    if not device_name or not incident_family:
        return ""
    hospital_key = _display_value(
        item.get("hospitalSeq"),
        default=_display_value(
            item.get("hospitalName"),
            default=_display_value(item.get("hospital"), default=""),
        ),
    )
    return "|".join((hospital_key, device_name, incident_family))


def acquire_automatic_sms_runtime_claim(
    claim_key: str,
) -> tuple[bool, dict[str, Any]]:
    """legacy와 같이 monotonic 60초 TTL의 process claim을 잡는다."""

    if not claim_key:
        return True, {}
    claimed_at = time.monotonic()
    with _SMS_AUTOMATION_RUNTIME_CLAIMS_LOCK:
        expired_keys = [
            key
            for key, claim in _SMS_AUTOMATION_RUNTIME_CLAIMS.items()
            if claimed_at - float(claim.get("claimedAt") or 0.0)
            >= _SMS_AUTOMATION_CLAIM_WINDOW_SEC
        ]
        for key in expired_keys:
            _SMS_AUTOMATION_RUNTIME_CLAIMS.pop(key, None)

        existing = _SMS_AUTOMATION_RUNTIME_CLAIMS.get(claim_key)
        if existing is not None:
            return False, existing
        claim = {
            "claimedAt": claimed_at,
            "done": threading.Event(),
            "result": None,
        }
        _SMS_AUTOMATION_RUNTIME_CLAIMS[claim_key] = claim
        return True, claim


def publish_automatic_sms_runtime_claim_result(
    claim: dict[str, Any],
    result: Mapping[str, Any],
) -> None:
    """owner 결과를 기다리는 같은 프로세스의 중복 cycle에 알린다."""

    if not claim:
        return
    with _SMS_AUTOMATION_RUNTIME_CLAIMS_LOCK:
        claim["result"] = dict(result)
        done = claim.get("done")
        if isinstance(done, threading.Event):
            done.set()


def wait_for_automatic_sms_runtime_claim(
    claim_key: str,
    claim: Mapping[str, Any],
    *,
    logger: logging.Logger,
) -> None:
    """동시 중복은 첫 provider 호출 완료까지만 legacy timeout으로 기다린다."""

    done = claim.get("done")
    if isinstance(done, threading.Event):
        done.wait(
            timeout=min(
                float(_SMS_AUTOMATION_CLAIM_WINDOW_SEC),
                max(
                    3.0,
                    float(
                        cs.DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_TIMEOUT_SEC
                    )
                    + 2.0,
                ),
            )
        )
    with _SMS_AUTOMATION_RUNTIME_CLAIMS_LOCK:
        if isinstance(claim.get("result"), Mapping):
            logger.info(
                "Reused device alert auto SMS result claim=%s",
                claim_key,
            )


def _sms_delivery_outbox_path(
    outbox_path: str | Path | None = None,
) -> Path:
    return Path(outbox_path or cs.SMS_DELIVERY_OUTBOX_PATH).expanduser()


def _sms_automation_claim_path(
    outbox_path: str | Path | None = None,
) -> Path:
    path = _sms_delivery_outbox_path(outbox_path)
    return path.with_name(f"{path.name}.automatic-claims.json")


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


def _read_sms_automation_claims_unlocked(
    path: Path,
) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        # 손상된 claim을 비어 있다고 취급하면 provider mutation이 중복될 수 있다.
        raise RuntimeError(f"자동 문자 claim을 읽지 못했어: {path}") from exc
    raw_claims = payload.get("claims") if isinstance(payload, dict) else None
    if (
        not isinstance(payload, dict)
        or payload.get("version") not in {1, _SMS_AUTOMATION_CLAIM_VERSION}
        or not isinstance(raw_claims, dict)
    ):
        raise RuntimeError(f"자동 문자 claim 형식이 올바르지 않아: {path}")

    claims: dict[str, dict[str, Any]] = {}
    for claim_key, raw_claim in raw_claims.items():
        if not isinstance(claim_key, str) or not claim_key:
            raise RuntimeError(f"자동 문자 claim key가 올바르지 않아: {path}")
        if payload.get("version") == 1:
            raw_claim = {
                "claimedAt": raw_claim,
                # 구 형식의 TTL을 다시 열면 배포 직후 중복 발송할 수 있어
                # 결과 불명 pending으로 안전하게 승격한다.
                "state": "pending",
                "groupHash": "",
            }
        if not isinstance(raw_claim, dict):
            raise RuntimeError(f"자동 문자 claim 값이 올바르지 않아: {path}")
        state = str(raw_claim.get("state") or "").strip()
        group_hash = str(raw_claim.get("groupHash") or "").strip()
        if state not in _SMS_AUTOMATION_CLAIM_STATES or (
            group_hash
            and not re.fullmatch(r"[0-9a-f]{64}", group_hash)
        ):
            raise RuntimeError(f"자동 문자 claim 상태가 올바르지 않아: {path}")
        try:
            claimed_at = _coerce_sms_delivery_datetime(
                raw_claim.get("claimedAt")
            )
        except ValueError as exc:
            raise RuntimeError(
                f"자동 문자 claim 시각이 올바르지 않아: {path}"
            ) from exc
        claims[claim_key] = {
            "claimedAt": claimed_at,
            "state": state,
            "groupHash": group_hash,
        }
    return claims


def _write_sms_automation_claims_unlocked(
    path: Path,
    claims: dict[str, dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": _SMS_AUTOMATION_CLAIM_VERSION,
        # 원문 장비명과 alert category는 파일에 남기지 않고 해시 key만 보존한다.
        "claims": {
            key: {
                "claimedAt": _sms_delivery_datetime_text(
                    value["claimedAt"]
                ),
                "state": value["state"],
                "groupHash": value.get("groupHash") or "",
            }
            for key, value in sorted(claims.items())
        },
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
        os.chmod(path, 0o600)
        try:
            directory_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        except OSError:
            pass
    finally:
        if temp_path is not None and temp_path.exists():
            temp_path.unlink()


def _automatic_sms_claim_key(device_name: str, alert_category: str) -> str:
    normalized_device = _display_value(device_name, default="").casefold()
    normalized_category = _display_value(alert_category, default="").casefold()
    if normalized_category in {
        "video_signal",
        "recording",
        "recording_processing",
    }:
        normalized_category = "captureboard_recording"
    if not normalized_device or not normalized_category:
        raise ValueError("자동 문자 claim 대상 장비나 분류가 비어 있어")
    raw_key = f"{normalized_device}\0{normalized_category}"
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def claim_automatic_sms_delivery(
    device_name: str,
    alert_category: str,
    *,
    claimed_at: datetime,
    window_seconds: int = _SMS_AUTOMATION_CLAIM_WINDOW_SEC,
    outbox_path: str | Path | None = None,
) -> bool:
    """두 automation cycle이 공유하는 provider 직전 crash-safe claim을 잡는다."""

    actual_claimed_at = _coerce_sms_delivery_datetime(claimed_at)
    actual_window_seconds = max(1, int(window_seconds))
    claim_key = _automatic_sms_claim_key(device_name, alert_category)
    outbox = _sms_delivery_outbox_path(outbox_path)
    claim_path = _sms_automation_claim_path(outbox_path)
    cutoff = actual_claimed_at - timedelta(seconds=actual_window_seconds)
    with _SMS_DELIVERY_OUTBOX_THREAD_LOCK:
        # receipt outbox와 같은 process/file lock을 써서 여러 cycle·worker thread의
        # read-check-write를 provider 호출 전에 하나의 원자 구간으로 만든다.
        with _locked_sms_delivery_outbox_file(outbox):
            claims = _read_sms_automation_claims_unlocked(claim_path)
            current = claims.get(claim_key)
            if current is not None and not (
                current["state"] == "settled"
                and current["claimedAt"] <= cutoff
            ):
                return False
            # provider 호출 직전부터 결과를 durable하게 확정할 때까지 pending은
            # TTL 없이 유지한다. 프로세스 crash나 timeout 뒤 자동 재발송하지 않는다.
            claims[claim_key] = {
                "claimedAt": actual_claimed_at,
                "state": "pending",
                "groupHash": "",
            }
            _write_sms_automation_claims_unlocked(claim_path, claims)
            return True


def hold_automatic_sms_delivery_claim(
    device_name: str,
    alert_category: str,
    *,
    held_at: datetime,
    state: str,
    group_id: str | None = None,
    outbox_path: str | Path | None = None,
) -> bool:
    """provider 결과를 accepted/uncertain 또는 settled claim으로 승격한다."""

    if state not in {"accepted", "uncertain", "settled"}:
        raise ValueError("자동 문자 claim hold 상태가 올바르지 않아")
    actual_held_at = _coerce_sms_delivery_datetime(held_at)
    claim_key = _automatic_sms_claim_key(device_name, alert_category)
    group_hash = (
        hashlib.sha256(group_id.strip().encode("utf-8")).hexdigest()
        if isinstance(group_id, str) and group_id.strip()
        else ""
    )
    claim_path = _sms_automation_claim_path(outbox_path)
    outbox = _sms_delivery_outbox_path(outbox_path)
    with _SMS_DELIVERY_OUTBOX_THREAD_LOCK:
        with _locked_sms_delivery_outbox_file(outbox):
            claims = _read_sms_automation_claims_unlocked(claim_path)
            current = claims.get(claim_key)
            if current is None or current["state"] == "settled":
                return False
            claims[claim_key] = {
                "claimedAt": actual_held_at,
                "state": state,
                "groupHash": group_hash if state == "accepted" else "",
            }
            _write_sms_automation_claims_unlocked(claim_path, claims)
            return True


def _settle_automatic_sms_delivery_claim_by_group_id(
    group_id: str,
    *,
    settled_at: datetime,
    outbox_path: str | Path | None = None,
) -> bool:
    """최종 provider/Sheet 반영 뒤에만 sticky accepted claim을 cooldown으로 바꾼다."""

    normalized_group_id = str(group_id or "").strip()
    if not normalized_group_id:
        return False
    group_hash = hashlib.sha256(
        normalized_group_id.encode("utf-8")
    ).hexdigest()
    actual_settled_at = _coerce_sms_delivery_datetime(settled_at)
    claim_path = _sms_automation_claim_path(outbox_path)
    outbox = _sms_delivery_outbox_path(outbox_path)
    with _SMS_DELIVERY_OUTBOX_THREAD_LOCK:
        with _locked_sms_delivery_outbox_file(outbox):
            claims = _read_sms_automation_claims_unlocked(claim_path)
            changed = False
            for claim in claims.values():
                if (
                    claim["state"] == "accepted"
                    and claim.get("groupHash") == group_hash
                ):
                    claim.update(
                        {
                            "claimedAt": actual_settled_at,
                            "state": "settled",
                            "groupHash": group_hash,
                        }
                    )
                    changed = True
            if changed:
                _write_sms_automation_claims_unlocked(claim_path, claims)
            return changed


def settle_automatic_sms_delivery_claim_for_recovery(
    device_name: str,
    alert_category: str,
    *,
    settled_at: datetime,
    allow_accepted: bool = False,
    outbox_path: str | Path | None = None,
) -> dict[str, str]:
    """서비스 중지·provider 확인 뒤 exact sticky claim 하나만 cooldown 처리한다."""

    actual_settled_at = _coerce_sms_delivery_datetime(settled_at)
    claim_key = _automatic_sms_claim_key(device_name, alert_category)
    claim_path = _sms_automation_claim_path(outbox_path)
    outbox = _sms_delivery_outbox_path(outbox_path)
    with _SMS_DELIVERY_OUTBOX_THREAD_LOCK:
        with _locked_sms_delivery_outbox_file(outbox):
            claims = _read_sms_automation_claims_unlocked(claim_path)
            current = claims.get(claim_key)
            if current is None:
                raise ValueError("자동 문자 claim을 찾지 못했어")
            previous_state = str(current["state"])
            if previous_state == "accepted" and not allow_accepted:
                raise ValueError(
                    "접수된 자동 문자는 outbox reconcile 확인이 먼저 필요해"
                )
            if previous_state not in {
                "pending",
                "uncertain",
                "accepted",
                "settled",
            }:
                raise ValueError("자동 문자 claim 상태가 올바르지 않아")
            current.update(
                {
                    "claimedAt": actual_settled_at,
                    "state": "settled",
                    "groupHash": current.get("groupHash") or "",
                }
            )
            _write_sms_automation_claims_unlocked(claim_path, claims)
            return {
                "claimHash": claim_key[:24],
                "previousState": previous_state,
                "state": "settled",
            }


def inspect_automatic_sms_recovery_state(
    *,
    outbox_path: str | Path,
    expected_outbox_path: str | Path | None = None,
    require_initialized: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """offline rollback CAS용 outbox/claim drain 상태를 원문 없이 반환한다."""

    outbox = _canonical_sms_recovery_path(
        _sms_delivery_outbox_path(outbox_path),
        label="문자 발송 outbox",
    )
    if expected_outbox_path is not None:
        expected_outbox = _canonical_sms_recovery_path(
            expected_outbox_path,
            label="설정된 문자 발송 outbox",
        )
        if outbox != expected_outbox:
            raise ValueError("문자 발송 outbox가 설정된 경로와 달라")
    elif require_initialized:
        # strict recovery는 서비스가 실제 쓰던 경로와 비교해야
        # 다른 빈 파일을 drain 상태로 오인하지 않는다.
        raise ValueError("설정된 문자 발송 outbox 경로가 필요해")

    if require_initialized:
        return _inspect_initialized_sms_recovery_state(outbox, now=now)

    claim_path = _sms_automation_claim_path(outbox_path)
    with _SMS_DELIVERY_OUTBOX_THREAD_LOCK:
        with _locked_sms_delivery_outbox_file(outbox):
            items = _read_sms_delivery_outbox_unlocked(outbox)
            claims = _read_sms_automation_claims_unlocked(claim_path)
    serialized_claims = {
        key: {
            "claimedAt": _sms_delivery_datetime_text(value["claimedAt"]),
            "state": value["state"],
            "groupHash": value.get("groupHash") or "",
        }
        for key, value in sorted(claims.items())
    }
    state_digest = hashlib.sha256(
        json.dumps(
            {"items": items, "claims": serialized_claims},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "stateDigest": state_digest,
        "outboxItemCount": len(items),
        "unresolvedClaimCount": sum(
            1 for value in claims.values() if value["state"] != "settled"
        ),
        "settledClaimCount": sum(
            1 for value in claims.values() if value["state"] == "settled"
        ),
        "activeSettledClaimCount": _active_settled_sms_claim_count(
            claims,
            now=now,
        ),
    }


def initialize_automatic_sms_recovery_state(
    *,
    outbox_path: str | Path,
    expected_outbox_path: str | Path,
    now: datetime | None = None,
) -> dict[str, Any]:
    """중지된 owner service의 canonical SMS 정본 두 파일을 한 번만 만든다."""

    outbox = _canonical_sms_recovery_path(
        _sms_delivery_outbox_path(outbox_path),
        label="문자 발송 outbox",
    )
    expected_outbox = _canonical_sms_recovery_path(
        expected_outbox_path,
        label="설정된 문자 발송 outbox",
    )
    if outbox != expected_outbox:
        raise ValueError("문자 발송 outbox가 설정된 경로와 달라")
    claim_path = _sms_automation_claim_path(outbox)
    _validate_sms_recovery_parent_chain(outbox)

    created = False
    with _SMS_DELIVERY_OUTBOX_THREAD_LOCK:
        # runtime writer와 동일한 host lock 아래서 두 leaf의 존재 여부를
        # 한 번에 확정해 clean host에서만 create-only 초기화한다.
        with _locked_strict_sms_recovery_file(outbox):
            outbox_exists = _sms_recovery_leaf_exists(outbox)
            claim_exists = _sms_recovery_leaf_exists(claim_path)
            if outbox_exists != claim_exists:
                raise ValueError(
                    "문자 발송 recovery 상태가 부분 초기화되어 있어"
                )
            if not outbox_exists:
                outbox_revision: tuple[str, int, int] | None = None
                try:
                    outbox_revision = _create_sms_recovery_json_file(
                        outbox,
                        {"version": _SMS_DELIVERY_OUTBOX_VERSION, "items": []},
                    )
                    _create_sms_recovery_json_file(
                        claim_path,
                        {
                            "version": _SMS_AUTOMATION_CLAIM_VERSION,
                            "claims": {},
                        },
                    )
                    created = True
                except Exception as exc:
                    # 두 번째 create가 실패하면 이 실행이 만든 exact empty
                    # outbox만 제거한다. revision이 달라졌으면 삭제하지
                    # 않고 부분 상태로 fail-closed한다.
                    if outbox_revision is not None:
                        _remove_exact_initialized_sms_leaf(
                            outbox,
                            expected_digest=outbox_revision[0],
                            expected_device=outbox_revision[1],
                            expected_inode=outbox_revision[2],
                        )
                    raise ValueError(
                        "문자 발송 recovery 상태를 초기화하지 못했어"
                    ) from exc

    state = _inspect_initialized_sms_recovery_state(outbox, now=now)
    return {
        "kind": "sms_recovery_state_initializer",
        "initialized": True,
        "created": created,
        **state,
    }


def _inspect_initialized_sms_recovery_state(
    outbox: Path,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """중지된 서비스의 initialized SMS 두 파일을 exact revision으로 검사한다."""

    claim_path = _sms_automation_claim_path(outbox)
    _validate_sms_recovery_parent_chain(outbox)
    # missing file로 빈 상태를 만들거나 lock leaf만 남기지 않고,
    # 두 정본이 이미 initialized된 절차에서만 recovery를 연다.
    _validate_initialized_sms_recovery_leaf(
        outbox,
        label="문자 발송 outbox",
    )
    _validate_initialized_sms_recovery_leaf(
        claim_path,
        label="자동 문자 claim",
    )
    with _SMS_DELIVERY_OUTBOX_THREAD_LOCK:
        # runtime writer와 같은 lock 파일을 잡아 두 파일을 하나의
        # recovery snapshot으로 읽고, 비협조 writer도 raw digest 재검증으로 감지한다.
        with _locked_strict_sms_recovery_file(outbox):
            raw_outbox, outbox_digest = _load_strict_sms_recovery_json(
                outbox,
                label="문자 발송 outbox",
            )
            raw_claims, claims_digest = _load_strict_sms_recovery_json(
                claim_path,
                label="자동 문자 claim",
            )
            items = _parse_strict_sms_recovery_outbox(raw_outbox, outbox)
            claims = _parse_strict_sms_recovery_claims(raw_claims, claim_path)
            _, final_outbox_digest = _load_strict_sms_recovery_json(
                outbox,
                label="문자 발송 outbox",
            )
            _, final_claims_digest = _load_strict_sms_recovery_json(
                claim_path,
                label="자동 문자 claim",
            )
    if (
        final_outbox_digest != outbox_digest
        or final_claims_digest != claims_digest
    ):
        raise ValueError("문자 발송 recovery 상태가 변경됐어")
    state_digest = hashlib.sha256(
        f"{outbox_digest}:{claims_digest}".encode("ascii")
    ).hexdigest()
    return {
        "stateDigest": state_digest,
        "outboxItemCount": len(items),
        "unresolvedClaimCount": sum(
            1 for value in claims.values() if value["state"] != "settled"
        ),
        "settledClaimCount": sum(
            1 for value in claims.values() if value["state"] == "settled"
        ),
        "activeSettledClaimCount": _active_settled_sms_claim_count(
            claims,
            now=now,
        ),
    }


def _active_settled_sms_claim_count(
    claims: dict[str, dict[str, Any]],
    *,
    now: datetime | None,
) -> int:
    """재claim cooldown 60초가 끝나지 않은 settled claim만 세어 이관을 막는다."""

    actual_now = _coerce_sms_delivery_datetime(
        now or datetime.now(timezone.utc)
    )
    cutoff = actual_now - timedelta(seconds=_SMS_AUTOMATION_CLAIM_WINDOW_SEC)
    return sum(
        1
        for value in claims.values()
        if value["state"] == "settled" and value["claimedAt"] > cutoff
    )


def _canonical_sms_recovery_path(
    path_value: str | Path,
    *,
    label: str,
) -> Path:
    path = Path(path_value).expanduser()
    if not path.is_absolute() or path == Path("/"):
        raise ValueError(f"{label} 경로가 올바르지 않아")
    try:
        resolved = path.resolve(strict=False)
    except OSError as exc:
        raise ValueError(f"{label} 경로가 올바르지 않아") from exc
    if path != resolved:
        raise ValueError(f"{label} 경로가 canonical 경로가 아니야")
    return path


def _validate_sms_recovery_parent_chain(path: Path) -> None:
    current = path.parent
    while True:
        try:
            current_stat = current.lstat()
        except OSError as exc:
            raise ValueError("문자 발송 recovery parent가 올바르지 않아") from exc
        if (
            stat.S_ISLNK(current_stat.st_mode)
            or not stat.S_ISDIR(current_stat.st_mode)
            or current_stat.st_uid not in {0, os.geteuid()}
            or current_stat.st_mode & 0o022
        ):
            raise ValueError("문자 발송 recovery parent가 보호되지 않았어")
        if current.parent == current:
            break
        current = current.parent


def _validate_initialized_sms_recovery_leaf(path: Path, *, label: str) -> None:
    try:
        leaf_stat = path.lstat()
    except OSError as exc:
        raise ValueError(f"{label}가 초기화되지 않았어") from exc
    if (
        stat.S_ISLNK(leaf_stat.st_mode)
        or not stat.S_ISREG(leaf_stat.st_mode)
        or leaf_stat.st_uid not in {0, os.geteuid()}
        or stat.S_IMODE(leaf_stat.st_mode) != 0o600
    ):
        raise ValueError(f"{label}가 보호되지 않았어")


def _sms_recovery_leaf_exists(path: Path) -> bool:
    """symlink를 missing으로 숨기지 않고 leaf 존재를 확정한다."""

    try:
        leaf_stat = path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise ValueError("문자 발송 recovery leaf를 확인할 수 없어") from exc
    if stat.S_ISLNK(leaf_stat.st_mode) or not stat.S_ISREG(leaf_stat.st_mode):
        raise ValueError("문자 발송 recovery leaf가 보호되지 않았어")
    return True


def _create_sms_recovery_json_file(
    path: Path,
    payload: dict[str, Any],
) -> tuple[str, int, int]:
    """공통 protected JSON create-only/CAS 계약으로 strict leaf를 만든다."""

    # module import 시에는 device health cycle이 이 모듈을 사용하므로,
    # initializer 실행 시점에만 공통 protected JSON primitive를 가져온다.
    from boxer_company.protected_json import (
        create_protected_json_file,
        ProtectedJsonFileError,
    )

    try:
        digest = create_protected_json_file(
            path,
            payload,
            label="SMS recovery state",
        )
        created_stat = path.lstat()
    except (ProtectedJsonFileError, OSError) as exc:
        raise ValueError("문자 발송 recovery leaf를 생성할 수 없어") from exc
    return (
        digest,
        created_stat.st_dev,
        created_stat.st_ino,
    )


def _remove_exact_initialized_sms_leaf(
    path: Path,
    *,
    expected_digest: str,
    expected_device: int,
    expected_inode: int,
) -> None:
    """두 파일 생성 중 실패한 이 실행의 exact empty leaf만 롤백한다."""

    try:
        _, actual_digest = _load_strict_sms_recovery_json(
            path,
            label="문자 발송 outbox",
        )
        leaf_stat = path.lstat()
    except (OSError, ValueError) as exc:
        raise ValueError(
            "문자 발송 recovery 부분 상태를 직접 확인해야 해"
        ) from exc
    if (
        actual_digest != expected_digest
        or leaf_stat.st_dev != expected_device
        or leaf_stat.st_ino != expected_inode
    ):
        raise ValueError(
            "문자 발송 recovery 부분 상태를 직접 확인해야 해"
        )
    _unlink_sms_recovery_leaf_if_same_inode(
        path,
        expected_device=expected_device,
        expected_inode=expected_inode,
    )


def _unlink_sms_recovery_leaf_if_same_inode(
    path: Path,
    *,
    expected_device: int,
    expected_inode: int,
) -> None:
    try:
        leaf_stat = path.lstat()
    except OSError as exc:
        raise ValueError("문자 발송 recovery leaf를 정리하지 못했어") from exc
    if (
        stat.S_ISLNK(leaf_stat.st_mode)
        or not stat.S_ISREG(leaf_stat.st_mode)
        or leaf_stat.st_dev != expected_device
        or leaf_stat.st_ino != expected_inode
    ):
        raise ValueError(
            "문자 발송 recovery 부분 상태를 직접 확인해야 해"
        )
    path.unlink()
    _fsync_sms_recovery_directory(path.parent)


def _fsync_sms_recovery_directory(path: Path) -> None:
    try:
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError:
        # 일부 파일시스템의 directory fsync 미지원은 leaf 원자성을 바꾸지 않는다.
        pass


@contextmanager
def _locked_strict_sms_recovery_file(outbox: Path) -> Iterator[None]:
    lock_path = outbox.with_name(f"{outbox.name}.lock")
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise ValueError("문자 발송 recovery lock을 열 수 없어") from exc
    try:
        lock_stat = os.fstat(descriptor)
        if (
            not stat.S_ISREG(lock_stat.st_mode)
            or lock_stat.st_uid not in {0, os.geteuid()}
            or lock_stat.st_mode & 0o022
        ):
            raise ValueError("문자 발송 recovery lock이 보호되지 않았어")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _load_strict_sms_recovery_json(
    path: Path,
    *,
    label: str,
) -> tuple[Any, str]:
    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ValueError(f"{label}가 초기화되지 않았어") from exc
    chunks: list[bytes] = []
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid not in {0, os.geteuid()}
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_size > _SMS_RECOVERY_MAX_FILE_BYTES
        ):
            raise ValueError(f"{label}가 보호되지 않았어")
        size = 0
        while True:
            chunk = os.read(
                descriptor,
                min(64 * 1024, _SMS_RECOVERY_MAX_FILE_BYTES + 1 - size),
            )
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            if size > _SMS_RECOVERY_MAX_FILE_BYTES:
                raise ValueError(f"{label}가 너무 커")
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise ValueError(f"{label}가 읽는 중 변경됐어")
    finally:
        os.close(descriptor)
    raw = b"".join(chunks)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise ValueError(f"{label}를 읽지 못했어") from exc
    return payload, hashlib.sha256(raw).hexdigest()


def _parse_strict_sms_recovery_outbox(
    payload: Any,
    path: Path,
) -> list[dict[str, Any]]:
    if (
        not isinstance(payload, dict)
        or set(payload) != {"version", "items"}
        or type(payload.get("version")) is not int
        or payload.get("version") != _SMS_DELIVERY_OUTBOX_VERSION
        or not isinstance(payload.get("items"), list)
    ):
        raise ValueError(f"문자 발송 outbox 형식이 올바르지 않아: {path}")
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


def _parse_strict_sms_recovery_claims(
    payload: Any,
    path: Path,
) -> dict[str, dict[str, Any]]:
    if (
        not isinstance(payload, dict)
        or set(payload) != {"version", "claims"}
        or type(payload.get("version")) is not int
        or payload.get("version") != _SMS_AUTOMATION_CLAIM_VERSION
        or not isinstance(payload.get("claims"), dict)
    ):
        raise ValueError(f"자동 문자 claim 형식이 올바르지 않아: {path}")
    claims: dict[str, dict[str, Any]] = {}
    for claim_key, raw_claim in payload["claims"].items():
        if (
            not isinstance(claim_key, str)
            or re.fullmatch(r"[0-9a-f]{64}", claim_key) is None
            or not isinstance(raw_claim, dict)
            or set(raw_claim) != {"claimedAt", "state", "groupHash"}
        ):
            raise ValueError(f"자동 문자 claim 값이 올바르지 않아: {path}")
        state = str(raw_claim.get("state") or "").strip()
        group_hash = str(raw_claim.get("groupHash") or "").strip()
        if state not in _SMS_AUTOMATION_CLAIM_STATES or (
            group_hash and not re.fullmatch(r"[0-9a-f]{64}", group_hash)
        ):
            raise ValueError(f"자동 문자 claim 상태가 올바르지 않아: {path}")
        try:
            claimed_at = _coerce_sms_delivery_datetime(raw_claim.get("claimedAt"))
        except ValueError as exc:
            raise ValueError(f"자동 문자 claim 시각이 올바르지 않아: {path}") from exc
        claims[claim_key] = {
            "claimedAt": claimed_at,
            "state": state,
            "groupHash": group_hash,
        }
    return claims


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
        # Sheet 모듈이 T metadata와 B/F/S identity hash로 고유성을 검증한 결과만 사용한다.
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
        try:
            sheet_rows = _load_device_health_sheet_sms_delivery_rows()
        except Exception as exc:
            # outbox 처리의 첫 외부 호출은 변경 전 Sheets GET이다. 이
            # 순수 읽기 실패를 coordinator까지 올리면 inFlight가 남아
            # 전체 SMS 후처리가 잠기므로 outbox를 보존하고 다음 poll로 넘긴다.
            logger.warning(
                "문자 발송 결과 시트를 조회하지 못했어 "
                "phase=outbox_reconcile error_type=%s",
                type(exc).__name__,
            )
            return 0
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
                    _settle_automatic_sms_delivery_claim_by_group_id(
                        group_id,
                        settled_at=actual_now,
                        outbox_path=outbox_path,
                    )
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
                    # T 존재 확인이 실패하면 중복 위험이 있으므로 재append하지 않는다.
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
                # 최종 상태로 직접 append한 경우에도 실제 H/T 반영 확인 뒤에만 outbox를 제거한다.
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
                    _settle_automatic_sms_delivery_claim_by_group_id(
                        group_id,
                        settled_at=actual_now,
                        outbox_path=outbox_path,
                    )
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
            removed = _remove_sms_delivery_outbox_item(
                group_id,
                outbox_path=outbox_path,
            )
            if removed:
                _settle_automatic_sms_delivery_claim_by_group_id(
                    group_id,
                    settled_at=actual_now,
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
    try:
        pending_deliveries = _load_device_health_sheet_pending_sms_deliveries()
    except Exception as exc:
        # outbox reconcile이 끝난 뒤의 B2:T scan도 외부 변경 전 읽기다.
        # 완료된 reconcile 수는 보존하고 조회만 다음 fixed-delay에 재시도한다.
        logger.warning(
            "문자 발송 후처리 대상을 조회하지 못했어 "
            "phase=pending_scan error_type=%s",
            type(exc).__name__,
        )
        return updated_count
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


def run_sms_delivery_cycle_once(
    logger: logging.Logger,
    *,
    now: datetime | None = None,
) -> int:
    """Slack과 무관한 문자 최종 상태 조정 cycle을 한 번 실행한다."""

    return _run_sms_delivery_reporter_once(logger, now=now)
