from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

from boxer.core.utils import _display_value
from boxer.core import settings as s
from boxer.retrieval.connectors.db import _create_db_connection
from boxer_company.routers.mda_graphql import _lookup_mda_special_barcodes_by_barcode

_BARCODE_VALIDATION_CONTEXT_HINTS = (
    "유효성 검사",
    "유효성 검증",
    "바코드 검증",
    "걸리는 바코드",
    "막히는 바코드",
    "차단 대상",
    "제한 대상",
    "special_barcodes",
    "special barcode",
)
_BARCODE_VALIDATION_STATUS_HINTS = (
    "걸리",
    "막히",
    "차단",
    "제한",
    "통과",
    "허용",
    "무료 바코드",
    "무료바코드",
    "핑크 바코드",
    "핑크바코드",
    "pink barcode",
    "FREE",
    "환불 바코드",
    "환불바코드",
    "환불",
    "REFUND",
    "refund",
)
_BLOCKING_SPECIAL_TYPE_LABELS = {
    "FREE": "무료 바코드(핑크 바코드)",
    "REFUND": "환불 처리 바코드",
}
_PINK_CLASSIFICATION_REASON_HINTS = (
    "왜",
    "이유",
    "분류되지",
    "분류 안",
    "기록되지",
    "기록 안",
    "등록되지",
    "등록 안",
    "안됐",
    "안 됐",
    "누락",
    "대조",
    "첫 녹화",
    "첫녹화",
)
_PINK_CLASSIFICATION_SUBJECT_HINTS = (
    "핑크",
    "무료",
    "FREE",
    "special_barcodes",
    "special barcode",
)


def _contains_any_hint(question: str, hints: tuple[str, ...]) -> bool:
    normalized = str(question or "").strip()
    lowered = normalized.lower()
    return any(hint in normalized or hint.lower() in lowered for hint in hints)


def _is_barcode_validation_status_request(question: str, barcode: str | None) -> bool:
    if not str(barcode or "").strip():
        return False
    normalized = str(question or "").strip()
    if not normalized:
        return False
    if _contains_any_hint(normalized, _BARCODE_VALIDATION_CONTEXT_HINTS):
        return True
    return _contains_any_hint(normalized, _BARCODE_VALIDATION_STATUS_HINTS)


def _is_barcode_pink_classification_reason_request(question: str, barcode: str | None) -> bool:
    if not str(barcode or "").strip():
        return False
    normalized = str(question or "").strip()
    if not normalized:
        return False
    return _contains_any_hint(
        normalized,
        _PINK_CLASSIFICATION_SUBJECT_HINTS,
    ) and _contains_any_hint(normalized, _PINK_CLASSIFICATION_REASON_HINTS)


def _normalize_special_barcode_type(value: Any) -> str:
    return _display_value(value, default="").strip().upper()


def _local_zone() -> ZoneInfo:
    try:
        return ZoneInfo("Asia/Seoul")
    except Exception:
        return ZoneInfo("UTC")


def _parse_db_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        if value.year <= 1:
            return None
        return value
    text = _display_value(value, default="").strip()
    if not text or text.startswith("0000-00-00") or text.lower() in {"none", "null"}:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed


def _format_kst(value: Any) -> str:
    parsed = _parse_db_datetime(value)
    if parsed is None:
        return "없음"
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(_local_zone()).strftime("%Y-%m-%d %H:%M:%S")


def _format_special_barcode_rows(rows: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for row in rows:
        special_type = _normalize_special_barcode_type(row.get("type"))
        type_label = _BLOCKING_SPECIAL_TYPE_LABELS.get(special_type, special_type or "미확인")
        reason = _display_value(row.get("reason"), default="")
        suffix = f" / 사유: `{reason}`" if reason else ""
        lines.append(
            f"• 제한 목록: `{type_label}`(`{special_type or '미확인'}`){suffix}"
        )
    return lines


def _load_barcode_pink_classification_context(barcode: str) -> dict[str, Any]:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            # 첫 녹화 시점의 병원/장비 매핑과 현재 병원 핑크 적용일을 함께 확인한다.
            cursor.execute(
                "SELECT "
                "r.seq, "
                "r.fullBarcode, "
                "r.hospitalSeq, "
                "h.hospitalName AS hospitalName, "
                "h.isPinkBarcode AS hospitalPinkBarcodeAt, "
                "r.hospitalRoomSeq, "
                "hr.roomName AS roomName, "
                "r.deviceSeq, "
                "d.deviceName AS deviceName, "
                "r.recordedAt, "
                "r.createdAt "
                "FROM recordings r "
                "LEFT JOIN hospitals h ON h.seq = r.hospitalSeq "
                "LEFT JOIN hospital_rooms hr ON hr.seq = r.hospitalRoomSeq "
                "LEFT JOIN devices d ON d.seq = r.deviceSeq "
                "WHERE r.fullBarcode = %s "
                "ORDER BY COALESCE(r.recordedAt, r.createdAt) ASC, r.seq ASC "
                "LIMIT 1",
                (barcode,),
            )
            first_recording = cursor.fetchone() or None

            # 앱 표시 기준과 장비 동기화 기준은 최초 병원 이력 생성시각을 사용한다.
            cursor.execute(
                "SELECT "
                "bhh.barcode, "
                "bhh.hospitalSeq, "
                "h.hospitalName AS hospitalName, "
                "h.isPinkBarcode AS hospitalPinkBarcodeAt, "
                "bhh.createdAt, "
                "bhh.updatedAt "
                "FROM barcode_hospital_histories bhh "
                "LEFT JOIN hospitals h ON h.seq = bhh.hospitalSeq "
                "WHERE bhh.barcode = %s "
                "ORDER BY bhh.createdAt ASC, bhh.hospitalSeq ASC "
                "LIMIT 5",
                (barcode,),
            )
            history_rows = cursor.fetchall() or []

            hospital_seq = None
            if first_recording:
                hospital_seq = first_recording.get("hospitalSeq")
            if hospital_seq is None and history_rows:
                hospital_seq = history_rows[0].get("hospitalSeq")

            pink_activity_rows: list[dict[str, Any]] = []
            if hospital_seq is not None:
                # 병원 핑크 적용일이 나중에 소급 입력됐는지 activity_log로 확인한다.
                cursor.execute(
                    "SELECT "
                    "seq, "
                    "hospitalSeq, "
                    "activityType, "
                    "actorIdentity, "
                    "description, "
                    "createdAt "
                    "FROM activity_log "
                    "WHERE hospitalSeq = %s "
                    "AND targetEntityType = 'Hospital' "
                    "AND description LIKE %s "
                    "ORDER BY createdAt ASC, seq ASC "
                    "LIMIT 5",
                    (hospital_seq, "%핑크바코드 적용일%"),
                )
                pink_activity_rows = cursor.fetchall() or []
    finally:
        connection.close()

    return {
        "barcode": barcode,
        "firstRecording": first_recording,
        "historyRows": history_rows,
        "pinkActivityRows": pink_activity_rows,
    }


def _query_barcode_validation_status(barcode: str) -> str:
    normalized_barcode = str(barcode or "").strip()
    if not normalized_barcode:
        raise ValueError("바코드가 필요해")

    matches = _lookup_mda_special_barcodes_by_barcode(normalized_barcode)
    if not matches:
        return "\n".join(
            (
                "*바코드 유효성 검사 확인*",
                f"• 바코드: `{normalized_barcode}`",
                "• 결론: 현재 운영 제한 목록 기준으로는 유효성 검사에 걸리는 바코드로 확인되지 않았어",
                "• 확인: 무료/핑크 바코드나 환불 처리 바코드 목록에는 없어",
                "• 조치: 다만 일반 바코드 만료 여부까지는 여기서 바로 단정 못 해서, 꼭 확정이 필요하면 실제 장비에서 확인해",
            )
        )

    target = matches[0]
    special_type = _normalize_special_barcode_type(target.get("type"))
    reason = _display_value(target.get("reason"), default="")
    type_label = _BLOCKING_SPECIAL_TYPE_LABELS.get(special_type, special_type or "미확인")

    if special_type in _BLOCKING_SPECIAL_TYPE_LABELS:
        confirm = f"운영 제한 목록에서 `{type_label}`로 등록돼 있어"
        if reason:
            confirm = f"{confirm} / 사유: `{reason}`"
        return "\n".join(
            (
                "*바코드 유효성 검사 확인*",
                f"• 바코드: `{normalized_barcode}`",
                "• 결론: 이 바코드는 유효성 검사에 걸리는 바코드야",
                f"• 확인: {confirm}",
                "• 조치: 유효성 검사가 켜진 장비에서는 촬영 전에 막혀야 해",
            )
        )

    confirm = f"운영 제한 목록에서 `{type_label}` 유형으로 보이지만 현재 기준으로는 차단형 타입인지 바로 단정하긴 어려워"
    if reason:
        confirm = f"{confirm} / 사유: `{reason}`"
    return "\n".join(
        (
            "*바코드 유효성 검사 확인*",
            f"• 바코드: `{normalized_barcode}`",
            "• 결론: 이 바코드는 운영 제한 목록에 등록돼 있어",
            f"• 확인: {confirm}",
            "• 조치: 실제 차단 여부가 꼭 필요하면 유효성 검사가 켜진 장비에서 다시 확인해",
        )
    )


def _query_barcode_pink_classification_reason(barcode: str) -> str:
    normalized_barcode = str(barcode or "").strip()
    if not normalized_barcode:
        raise ValueError("바코드가 필요해")

    special_rows = _lookup_mda_special_barcodes_by_barcode(normalized_barcode)
    context = _load_barcode_pink_classification_context(normalized_barcode)
    first_recording = context.get("firstRecording") or {}
    history_rows = context.get("historyRows") or []
    first_history = history_rows[0] if history_rows else {}
    pink_activity_rows = context.get("pinkActivityRows") or []
    first_pink_activity = pink_activity_rows[0] if pink_activity_rows else {}

    special_types = {
        _normalize_special_barcode_type(row.get("type"))
        for row in special_rows
        if _normalize_special_barcode_type(row.get("type"))
    }
    is_blocked_special = bool(special_types & set(_BLOCKING_SPECIAL_TYPE_LABELS))

    lines = [
        "*핑크/환불 바코드 분류 근거*",
        f"• 바코드: `{normalized_barcode}`",
    ]
    if is_blocked_special:
        lines.append("• 결론: 이미 운영 제한 목록에 등록돼 있어")
        lines.extend(_format_special_barcode_rows(special_rows))
    else:
        lines.append("• 결론: 현재 `special_barcodes`에는 `FREE`/`REFUND`로 등록돼 있지 않아")

    if not first_recording:
        lines.extend(
            (
                "• 첫 녹화: recordings 기록 없음",
                "• 판단: 첫 녹화 병원 기준 핑크 분류 여부를 계산할 근거가 부족해",
            )
        )
        return "\n".join(lines)

    recorded_at = _parse_db_datetime(first_recording.get("recordedAt"))
    created_at = _parse_db_datetime(first_recording.get("createdAt"))
    hospital_pink_at = _parse_db_datetime(first_recording.get("hospitalPinkBarcodeAt"))
    history_created_at = _parse_db_datetime(first_history.get("createdAt"))
    pink_activity_created_at = _parse_db_datetime(first_pink_activity.get("createdAt"))

    lines.extend(
        (
            f"• 첫 녹화: recordedAt(KST) `{_format_kst(recorded_at)}` / createdAt(KST) `{_format_kst(created_at)}`",
            f"• 첫 녹화 병원: `{_display_value(first_recording.get('hospitalName'), default='미확인')}`"
            f" / 장비 `{_display_value(first_recording.get('deviceName'), default='미확인')}`",
            f"• 병원 현재 핑크 적용일: `{_format_kst(hospital_pink_at)}`",
        )
    )
    if first_history:
        lines.append(
            f"• 최초 병원 매핑 이력: `{_display_value(first_history.get('hospitalName'), default='미확인')}`"
            f" / createdAt(KST) `{_format_kst(history_created_at)}`"
        )
    else:
        lines.append("• 최초 병원 매핑 이력: 없음")

    if first_pink_activity:
        lines.append(
            f"• 핑크 설정 변경 로그: `{_format_kst(pink_activity_created_at)}`"
            f" / {_display_value(first_pink_activity.get('description'), default='내용 없음')}"
        )
    else:
        lines.append("• 핑크 설정 변경 로그: 확인 안 됨")

    if is_blocked_special:
        lines.append("• 판단: 이미 제한 목록에 있으니 미분류 케이스는 아니야")
    elif (
        pink_activity_created_at is not None
        and created_at is not None
        and pink_activity_created_at > created_at
    ):
        lines.append(
            "• 판단: 현재 적용일은 첫 녹화보다 앞서 보이지만, 실제 핑크 설정 변경은 첫 녹화 이후에 들어갔어. "
            "업로드 당시 병원 값이 없었고 기존 바코드 backfill이 없어 `special_barcodes`에 생성되지 않은 케이스로 봐"
        )
    elif hospital_pink_at is None:
        lines.append("• 판단: 첫 녹화 병원의 핑크 적용일이 없어서 FREE로 분류될 조건이 아니야")
    elif recorded_at is not None and hospital_pink_at is not None and recorded_at < hospital_pink_at:
        lines.append("• 판단: 첫 녹화 시각이 병원 핑크 적용일보다 빨라서 FREE로 분류될 조건이 아니야")
    elif history_created_at is not None and hospital_pink_at is not None and history_created_at < hospital_pink_at:
        lines.append("• 판단: 최초 병원 매핑 이력이 병원 핑크 적용일보다 빨라서 앱 표시 기준으로도 핑크가 아니야")
    else:
        lines.append(
            "• 판단: 현재 값만 보면 핑크 조건에 가까워 보여. 그래도 제한 목록에 없다면 설정이 나중에 소급 입력됐거나 "
            "업로드 경로가 핑크 등록 분기를 타지 않았는지 확인이 필요해"
        )

    return "\n".join(lines)
