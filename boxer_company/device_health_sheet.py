import hashlib
import json
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from boxer.core.utils import _display_value
from boxer_company import settings as cs
from boxer_company.sms_delivery import (
    _SMS_DELIVERY_ACCEPTED,
    _SMS_DELIVERY_CONFIRM_REQUIRED,
    _SMS_DELIVERY_DELIVERED,
    _SMS_DELIVERY_FAILED,
    _SMS_DELIVERY_NOT_SENT,
    _SMS_DELIVERY_REQUEST_FAILED,
)

_GOOGLE_SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
_GOOGLE_SHEETS_API_BASE_URL = "https://sheets.googleapis.com/v4/spreadsheets"
_GOOGLE_SHEETS_SERIAL_EPOCH = datetime(1899, 12, 30)
_KST = ZoneInfo("Asia/Seoul")
_NON_DEVICE_PROBLEM_COMPONENTS = {"용량", "storage", "디스크", "저장공간", "trashcan"}
_CAPTUREBOARD_INCIDENT_ISSUE_MARKERS = (
    "녹화 파일 증가 정지",
    "캡처보드",
    "비디오 장치",
)
_CAPTUREBOARD_INCIDENT_EXCLUDED_MARKERS = (
    "병합",
    "업로드",
    "merge",
    "upload",
    "ffmpeg",
)
# 시트 반복 계산(최대 1회)과 함께 사용해 완료 선택 순간을 셀 자체에 고정한다.
_COMPLETED_AT_FORMULA = (
    '=IF(INDIRECT("J"&ROW())="완료",'
    'IF(OR(INDIRECT("L"&ROW())="",INDIRECT("L"&ROW())=0),'
    'NOW()+9/24,INDIRECT("L"&ROW())),"")'
)
_WORK_DURATION_FORMULA = (
    '=IF(OR(INDIRECT("A"&ROW())="",INDIRECT("L"&ROW())=""),"",'
    'LET(total,ROUND((INDIRECT("L"&ROW())-INDIRECT("A"&ROW()))*86400),'
    'hours,INT(total/3600),minutes,INT(MOD(total,3600)/60),seconds,MOD(total,60),'
    'TRIM(IF(hours>0,hours&"시간 ","")&IF(minutes>0,minutes&"분 ","")&'
    'IF(OR(seconds>0,AND(hours=0,minutes=0)),seconds&"초",""))))'
)
_AUTO_SMS_ACCEPTED_TEXT = "문자 발송 접수"
_AUTO_SMS_LEGACY_SENT_TEXT = "문자 자동발송 완료"
_AUTO_SMS_FAILED_TEXT = "문자 자동발송 실패 - 수동 발송 가능"
_AUTO_SMS_CONFIRM_REQUIRED_MARKER = "발송 여부 확인 필요"
_SMS_SHEET_ACCEPTED = "접수됨"
_SMS_SHEET_DELIVERED = "수신 완료"
_SMS_SHEET_DELIVERY_FAILED = "수신 실패"
_SMS_SHEET_NOT_SENT = "미발송"
_SMS_SHEET_REQUEST_FAILED = "발송 실패"
_SMS_SHEET_CONFIRM_REQUIRED = "확인 필요"
_SMS_TRACKING_METADATA_VERSION = 1


def _build_device_health_sheet_authorized_session() -> Any:
    # ADC를 사용해 배포 환경이 제공하는 서비스 계정 키 또는 WIF 자격증명을 공통으로 읽는다.
    import google.auth
    from google.auth.transport.requests import AuthorizedSession

    credentials, _project_id = google.auth.default(scopes=[_GOOGLE_SHEETS_SCOPE])
    return AuthorizedSession(credentials)


def _device_health_sheet_serial_datetime(value: datetime) -> float:
    local_value = value.astimezone(_KST) if value.tzinfo is not None else value.replace(tzinfo=_KST)
    local_naive = local_value.replace(tzinfo=None)
    return (local_naive - _GOOGLE_SHEETS_SERIAL_EPOCH).total_seconds() / 86400


def _device_health_sheet_sms_status(item: dict[str, Any]) -> str:
    delivery_status = _display_value(item.get("smsDeliveryStatus"), default="")
    if (
        delivery_status == _SMS_DELIVERY_ACCEPTED
        and not _display_value(item.get("smsGroupId"), default="")
    ):
        return _SMS_SHEET_CONFIRM_REQUIRED
    delivery_status_mapping = {
        _SMS_DELIVERY_ACCEPTED: _SMS_SHEET_ACCEPTED,
        _SMS_DELIVERY_DELIVERED: _SMS_SHEET_DELIVERED,
        _SMS_DELIVERY_FAILED: _SMS_SHEET_DELIVERY_FAILED,
        _SMS_DELIVERY_NOT_SENT: _SMS_SHEET_NOT_SENT,
        _SMS_DELIVERY_REQUEST_FAILED: _SMS_SHEET_REQUEST_FAILED,
        _SMS_DELIVERY_CONFIRM_REQUIRED: _SMS_SHEET_CONFIRM_REQUIRED,
    }
    if delivery_status in delivery_status_mapping:
        return delivery_status_mapping[delivery_status]

    sms_status_text = _display_value(item.get("smsStatusText"), default="")
    # 구조화 결과가 없는 과거 호출은 Slack 표시 문구로만 보수적으로 해석한다.
    # 기존 "완료" 문구는 실제 수신 완료가 아니라 provider 접수 성공을 뜻한다.
    if sms_status_text in {
        _AUTO_SMS_ACCEPTED_TEXT,
        _AUTO_SMS_LEGACY_SENT_TEXT,
    }:
        return _SMS_SHEET_CONFIRM_REQUIRED
    if sms_status_text == _AUTO_SMS_FAILED_TEXT:
        return _SMS_SHEET_REQUEST_FAILED
    if _AUTO_SMS_CONFIRM_REQUIRED_MARKER in sms_status_text:
        return _SMS_SHEET_CONFIRM_REQUIRED
    return _SMS_SHEET_NOT_SENT


def _device_health_sheet_sms_tracking_key(
    device_name: str,
    issue: str,
    slack_permalink: str,
) -> str:
    # 표시 셀 세 개를 JSON 배열로 직렬화해 단순 문자열 연결의 경계 충돌을 피한다.
    canonical_target = json.dumps(
        [
            _display_value(device_name, default=""),
            _display_value(issue, default=""),
            _display_value(slack_permalink, default=""),
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical_target.encode("utf-8")).hexdigest()


def _device_health_sheet_sms_tracking_time(
    item: dict[str, Any],
    *,
    detected_at: datetime,
) -> str:
    raw_accepted_at = item.get("smsAcceptedAt")
    if isinstance(raw_accepted_at, datetime):
        accepted_at = raw_accepted_at
    else:
        accepted_at_text = _display_value(raw_accepted_at, default="")
        try:
            accepted_at = datetime.fromisoformat(
                accepted_at_text.replace("Z", "+00:00")
            )
        except ValueError:
            accepted_at = detected_at
    if accepted_at.tzinfo is None:
        accepted_at = accepted_at.replace(tzinfo=_KST)
    # 숨김 메타데이터를 작게 유지하면서도 만료 판단에 필요한 초 단위 시각을 보존한다.
    return (
        accepted_at.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def _build_device_health_sheet_sms_tracking_metadata(
    item: dict[str, Any],
    *,
    device_name: str,
    issue: str,
    slack_permalink: str,
    detected_at: datetime,
) -> str:
    group_id = _display_value(item.get("smsGroupId"), default="")
    if not group_id:
        return ""
    metadata: dict[str, Any] = {
        "v": _SMS_TRACKING_METADATA_VERSION,
        "g": group_id,
        "k": _device_health_sheet_sms_tracking_key(
            device_name,
            issue,
            slack_permalink,
        ),
        "t": _device_health_sheet_sms_tracking_time(
            item,
            detected_at=detected_at,
        ),
    }
    message_id = _display_value(item.get("smsMessageId"), default="")
    if message_id:
        metadata["m"] = message_id
    # 숨김 R열에는 원문 식별값 대신 버전이 있는 compact JSON만 남긴다.
    return json.dumps(metadata, ensure_ascii=False, separators=(",", ":"))


def _parse_device_health_sheet_sms_tracking_metadata(
    value: Any,
) -> dict[str, Any] | None:
    raw_value = _display_value(value, default="")
    if not raw_value:
        return None
    try:
        payload = json.loads(raw_value)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict) or payload.get("v") != _SMS_TRACKING_METADATA_VERSION:
        return None
    group_id = _display_value(payload.get("g"), default="")
    tracking_key = _display_value(payload.get("k"), default="")
    accepted_at = _display_value(payload.get("t"), default="")
    if (
        not group_id
        or len(tracking_key) != 64
        or any(character not in "0123456789abcdef" for character in tracking_key)
        or not accepted_at
    ):
        return None
    try:
        parsed_accepted_at = datetime.fromisoformat(
            accepted_at.replace("Z", "+00:00")
        )
    except ValueError:
        return None
    if parsed_accepted_at.tzinfo is None:
        return None
    return {
        "groupId": group_id,
        "trackingKey": tracking_key,
        "acceptedAt": accepted_at,
        "messageId": _display_value(payload.get("m"), default=""),
    }


def _build_device_health_sheet_rows(
    alert_items: list[dict[str, Any]],
    *,
    detected_at: datetime,
    slack_permalink: str,
) -> list[list[Any]]:
    detected_at_serial = _device_health_sheet_serial_datetime(detected_at)
    permalink = _display_value(slack_permalink, default="")
    rows: list[list[Any]] = []
    for item in alert_items:
        if not isinstance(item, dict):
            continue
        problem_components = item.get("problemComponents")
        problem_device = (
            " ".join(
                _display_value(component, default="")
                for component in problem_components
                if _display_value(component, default="")
                and _display_value(component, default="").lower()
                not in _NON_DEVICE_PROBLEM_COMPONENTS
            )
            if isinstance(problem_components, list)
            else ""
        )
        device_name = _display_value(item.get("device"), default="장비명 미확인")
        issue = _display_value(item.get("issue"), default="상세 확인 필요")
        # 문자 상태를 Action 왼쪽 H에 두고, 신규 장애는 J 대기와 L/M 계산식으로 시작한다.
        rows.append(
            [
                detected_at_serial,
                device_name,
                _display_value(item.get("hospitalName"), default="병원 미확인"),
                _display_value(item.get("room"), default="병실 미확인"),
                problem_device,
                issue,
                "",
                _device_health_sheet_sms_status(item),
                "",
                "대기",
                "",
                _COMPLETED_AT_FORMULA,
                _WORK_DURATION_FORMULA,
                "",
                "",
                "",
                permalink,
                _build_device_health_sheet_sms_tracking_metadata(
                    item,
                    device_name=device_name,
                    issue=issue,
                    slack_permalink=permalink,
                    detected_at=detected_at,
                ),
            ]
        )
    return rows


def _load_device_health_sheet_captureboard_incidents(
    *,
    authorized_session: Any | None = None,
) -> dict[str, dict[str, Any]] | None:
    if not cs.DEVICE_HEALTH_SHEET_ENABLED:
        return None
    spreadsheet_id = str(cs.DEVICE_HEALTH_SHEET_SPREADSHEET_ID or "").strip()
    tab_name = str(cs.DEVICE_HEALTH_SHEET_TAB_NAME or "").strip()
    if not spreadsheet_id or not tab_name:
        raise ValueError("장비 장애 시트 ID 또는 탭 이름이 비어 있어")

    # 장비·문제장치·감지내용·상태·permalink가 포함된 B:Q를 한 번에 읽어
    # TA가 갱신한 현재 상태와 원본 Slack 알림을 같은 물리 행 기준으로 판단한다.
    quoted_tab_name = tab_name.replace("'", "''")
    read_range = quote(f"'{quoted_tab_name}'!B2:Q", safe="")
    url = (
        f"{_GOOGLE_SHEETS_API_BASE_URL}/{quote(spreadsheet_id, safe='')}"
        f"/values/{read_range}"
    )
    session = authorized_session or _build_device_health_sheet_authorized_session()
    response = session.get(
        url,
        params={
            "majorDimension": "ROWS",
            "valueRenderOption": "FORMATTED_VALUE",
        },
        timeout=max(1, int(cs.DEVICE_HEALTH_SHEET_TIMEOUT_SEC)),
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("장비 장애 시트 조회 응답이 객체가 아니야")
    rows = payload.get("values", [])
    if not isinstance(rows, list):
        raise ValueError("장비 장애 시트 조회 행 목록이 올바르지 않아")

    incidents: dict[str, dict[str, Any]] = {}
    for row_number, row in enumerate(rows, start=2):
        if not isinstance(row, list):
            raise ValueError("장비 장애 시트 조회 행 형식이 올바르지 않아")
        device_name = _display_value(row[0] if row else None, default="")
        if not device_name:
            continue
        problem_device = _display_value(row[3] if len(row) > 3 else None, default="")
        issue = _display_value(row[4] if len(row) > 4 else None, default="")
        normalized_details = f"{problem_device}\n{issue}".lower()

        # 녹화 증가 정지와 캡처보드 미감지만 같은 장애로 묶고,
        # 병합·업로드 같은 후처리 장애가 알림을 가리지 않도록 제외한다.
        if any(
            marker in normalized_details
            for marker in _CAPTUREBOARD_INCIDENT_EXCLUDED_MARKERS
        ):
            continue
        is_captureboard_incident = "캡처보드" in problem_device or any(
            marker in issue for marker in _CAPTUREBOARD_INCIDENT_ISSUE_MARKERS
        )
        if not is_captureboard_incident:
            continue

        # 응답은 시트의 물리 행 순서이므로 같은 장비를 덮어써 가장 아래 행을 최신으로 남긴다.
        incidents[device_name] = {
            "deviceName": device_name,
            "status": _display_value(row[8] if len(row) > 8 else None, default=""),
            "slackPermalink": _display_value(
                row[15] if len(row) > 15 else None,
                default="",
            ),
            "rowNumber": row_number,
        }
    return incidents


def _load_device_health_sheet_sms_tracking_rows(
    *,
    spreadsheet_id: str,
    tab_name: str,
    authorized_session: Any,
) -> list[list[Any]]:
    quoted_tab_name = tab_name.replace("'", "''")
    read_range = quote(f"'{quoted_tab_name}'!B2:R", safe="")
    url = (
        f"{_GOOGLE_SHEETS_API_BASE_URL}/{quote(spreadsheet_id, safe='')}"
        f"/values/{read_range}"
    )
    response = authorized_session.get(
        url,
        params={
            "majorDimension": "ROWS",
            "valueRenderOption": "FORMATTED_VALUE",
        },
        timeout=max(1, int(cs.DEVICE_HEALTH_SHEET_TIMEOUT_SEC)),
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("장비 장애 시트 조회 응답이 객체가 아니야")
    rows = payload.get("values", [])
    if not isinstance(rows, list):
        raise ValueError("장비 장애 시트 조회 행 목록이 올바르지 않아")
    for row in rows:
        if not isinstance(row, list):
            raise ValueError("장비 장애 시트 조회 행 형식이 올바르지 않아")
    return rows


def _resolve_device_health_sheet_sms_tracking_rows(
    rows: list[list[Any]],
) -> list[dict[str, Any]]:
    target_rows_by_key: dict[str, list[dict[str, Any]]] = {}
    tracking_records_by_key: dict[str, list[dict[str, Any]]] = {}
    for row_number, row in enumerate(rows, start=2):
        if not isinstance(row, list):
            raise ValueError("장비 장애 시트 조회 행 형식이 올바르지 않아")

        # B/F/Q는 사용자가 보는 대상 식별자이고 R은 정렬 범위에서 빠질 수 있으므로 독립 수집한다.
        device_name = _display_value(row[0] if row else None, default="")
        issue = _display_value(row[4] if len(row) > 4 else None, default="")
        slack_permalink = _display_value(
            row[15] if len(row) > 15 else None,
            default="",
        )
        if device_name and issue:
            tracking_key = _device_health_sheet_sms_tracking_key(
                device_name,
                issue,
                slack_permalink,
            )
            target_rows_by_key.setdefault(tracking_key, []).append(
                {
                    "rowNumber": row_number,
                    "smsStatus": _display_value(
                        row[6] if len(row) > 6 else None,
                        default="",
                    ),
                }
            )

        tracking_metadata = _parse_device_health_sheet_sms_tracking_metadata(
            row[16] if len(row) > 16 else None
        )
        if tracking_metadata is not None:
            tracking_records_by_key.setdefault(
                tracking_metadata["trackingKey"],
                [],
            ).append(
                {
                    **tracking_metadata,
                    "trackingRowNumber": row_number,
                }
            )

    resolved_matches: list[dict[str, Any]] = []
    for tracking_key, tracking_records in tracking_records_by_key.items():
        target_rows = target_rows_by_key.get(tracking_key, [])
        group_ids = {
            _display_value(record.get("groupId"), default="")
            for record in tracking_records
            if _display_value(record.get("groupId"), default="")
        }
        # 동일 키가 여러 대상 행 또는 여러 Solapi 그룹을 가리키면 어느 H셀도 수정하지 않는다.
        if len(target_rows) != 1 or len(group_ids) != 1:
            continue
        tracking_record = min(
            tracking_records,
            key=lambda record: (
                _display_value(record.get("acceptedAt"), default=""),
                int(record.get("trackingRowNumber") or 0),
            ),
        )
        target_row = target_rows[0]
        resolved_matches.append(
            {
                "rowNumber": target_row["rowNumber"],
                "groupId": next(iter(group_ids)),
                "trackingKey": tracking_key,
                "acceptedAt": tracking_record["acceptedAt"],
                "messageId": tracking_record["messageId"],
                "smsStatus": target_row["smsStatus"],
            }
        )

    # Solapi 한 그룹에 여러 메시지가 포함될 수 있으므로 identity가 고유한 행은 각각 반환한다.
    return resolved_matches


def _load_device_health_sheet_sms_delivery_matches(
    *,
    authorized_session: Any | None = None,
) -> list[dict[str, Any]] | None:
    if not cs.DEVICE_HEALTH_SHEET_ENABLED:
        return None
    spreadsheet_id = str(cs.DEVICE_HEALTH_SHEET_SPREADSHEET_ID or "").strip()
    tab_name = str(cs.DEVICE_HEALTH_SHEET_TAB_NAME or "").strip()
    if not spreadsheet_id or not tab_name:
        raise ValueError("장비 장애 시트 ID 또는 탭 이름이 비어 있어")

    session = authorized_session or _build_device_health_sheet_authorized_session()
    rows = _load_device_health_sheet_sms_tracking_rows(
        spreadsheet_id=spreadsheet_id,
        tab_name=tab_name,
        authorized_session=session,
    )
    # outbox 복구도 최종 상태 행을 볼 수 있게 H 상태와 무관한 모든 고유 매칭을 반환한다.
    return sorted(
        _resolve_device_health_sheet_sms_tracking_rows(rows),
        key=lambda item: (int(item["rowNumber"]), str(item["groupId"])),
    )


def _has_device_health_sheet_sms_tracking_group_id(
    group_id: str,
    authorized_session: Any | None = None,
) -> bool:
    if not cs.DEVICE_HEALTH_SHEET_ENABLED:
        return False
    normalized_group_id = _display_value(group_id, default="")
    if not normalized_group_id:
        return False
    spreadsheet_id = str(cs.DEVICE_HEALTH_SHEET_SPREADSHEET_ID or "").strip()
    tab_name = str(cs.DEVICE_HEALTH_SHEET_TAB_NAME or "").strip()
    if not spreadsheet_id or not tab_name:
        raise ValueError("장비 장애 시트 ID 또는 탭 이름이 비어 있어")

    session = authorized_session or _build_device_health_sheet_authorized_session()
    rows = _load_device_health_sheet_sms_tracking_rows(
        spreadsheet_id=spreadsheet_id,
        tab_name=tab_name,
        authorized_session=session,
    )
    # outbox 중복 방지는 B/F/Q 결합 성공 여부와 무관하게 R의 버전된 메타데이터만 본다.
    for row in rows:
        tracking_metadata = _parse_device_health_sheet_sms_tracking_metadata(
            row[16] if len(row) > 16 else None
        )
        if (
            tracking_metadata is not None
            and tracking_metadata["groupId"] == normalized_group_id
        ):
            return True
    return False


def _load_device_health_sheet_pending_sms_deliveries(
    *,
    authorized_session: Any | None = None,
) -> list[dict[str, Any]] | None:
    matches = _load_device_health_sheet_sms_delivery_matches(
        authorized_session=authorized_session
    )
    if matches is None:
        return None
    pending_deliveries: list[dict[str, Any]] = []
    for match in matches:
        if match["smsStatus"] != _SMS_SHEET_ACCEPTED:
            continue
        pending_delivery = {
            "rowNumber": match["rowNumber"],
            "groupId": match["groupId"],
            "acceptedAt": match["acceptedAt"],
        }
        if match["messageId"]:
            pending_delivery["messageId"] = match["messageId"]
        pending_deliveries.append(pending_delivery)
    return sorted(
        pending_deliveries,
        key=lambda item: (int(item["rowNumber"]), str(item["groupId"])),
    )


def _find_device_health_sheet_sms_tracking_match(
    rows: list[list[Any]],
    *,
    group_id: str,
    row_number_hint: int | None = None,
    tracking_key: str = "",
) -> dict[str, Any] | None:
    matches = [
        match
        for match in _resolve_device_health_sheet_sms_tracking_rows(rows)
        if match["groupId"] == group_id
    ]
    if tracking_key:
        tracking_matches = [
            match for match in matches if match["trackingKey"] == tracking_key
        ]
        return tracking_matches[0] if len(tracking_matches) == 1 else None
    if row_number_hint is not None:
        row_matches = [
            match
            for match in matches
            if int(match["rowNumber"]) == int(row_number_hint)
        ]
        if len(row_matches) == 1:
            return row_matches[0]
    return matches[0] if len(matches) == 1 else None


def _update_device_health_sheet_sms_status_by_group_id(
    *,
    row_number: int,
    group_id: str,
    sms_status: str,
    authorized_session: Any | None = None,
) -> bool:
    if not cs.DEVICE_HEALTH_SHEET_ENABLED:
        return False
    spreadsheet_id = str(cs.DEVICE_HEALTH_SHEET_SPREADSHEET_ID or "").strip()
    tab_name = str(cs.DEVICE_HEALTH_SHEET_TAB_NAME or "").strip()
    if not spreadsheet_id or not tab_name:
        raise ValueError("장비 장애 시트 ID 또는 탭 이름이 비어 있어")

    # 기존 호출 API의 행 번호는 힌트로만 받고, 실제 대상은 매번 R 메타데이터와 B/F/Q 해시로 다시 찾는다.
    # 호환용 행 번호 인자를 검증하되 실제 대상은 아래 identity 조회로 결정한다.
    normalized_row_number = int(row_number)
    if normalized_row_number < 2:
        return False
    normalized_group_id = _display_value(group_id, default="")
    normalized_sms_status = _display_value(sms_status, default="")
    if not normalized_group_id or not normalized_sms_status:
        raise ValueError("문자 상태 갱신용 groupId 또는 상태값이 비어 있어")

    quoted_tab_name = tab_name.replace("'", "''")
    base_url = f"{_GOOGLE_SHEETS_API_BASE_URL}/{quote(spreadsheet_id, safe='')}"
    session = authorized_session or _build_device_health_sheet_authorized_session()
    initial_rows = _load_device_health_sheet_sms_tracking_rows(
        spreadsheet_id=spreadsheet_id,
        tab_name=tab_name,
        authorized_session=session,
    )
    initial_match = _find_device_health_sheet_sms_tracking_match(
        initial_rows,
        group_id=normalized_group_id,
        row_number_hint=normalized_row_number,
    )
    if initial_match is None:
        return False
    if initial_match["smsStatus"] == normalized_sms_status:
        return True
    if initial_match["smsStatus"] != _SMS_SHEET_ACCEPTED:
        return False

    # 실제 쓰기 직전 전체 R 레코드를 다시 스캔해 행 이동과 식별 셀 수정을 확인한다.
    preflight_rows = _load_device_health_sheet_sms_tracking_rows(
        spreadsheet_id=spreadsheet_id,
        tab_name=tab_name,
        authorized_session=session,
    )
    preflight_match = _find_device_health_sheet_sms_tracking_match(
        preflight_rows,
        group_id=normalized_group_id,
        tracking_key=initial_match["trackingKey"],
    )
    if (
        preflight_match is None
        or preflight_match["trackingKey"] != initial_match["trackingKey"]
    ):
        return False
    if preflight_match["smsStatus"] == normalized_sms_status:
        return True
    if preflight_match["smsStatus"] != _SMS_SHEET_ACCEPTED:
        return False

    target_row_number = int(preflight_match["rowNumber"])
    update_range = quote(f"'{quoted_tab_name}'!H{target_row_number}", safe="")
    update_response = session.put(
        f"{base_url}/values/{update_range}",
        params={"valueInputOption": "RAW"},
        json={
            "majorDimension": "ROWS",
            "values": [[normalized_sms_status]],
        },
        timeout=max(1, int(cs.DEVICE_HEALTH_SHEET_TIMEOUT_SEC)),
    )
    update_response.raise_for_status()

    # HTTP 성공만 믿지 않고 다시 해시로 대상을 찾아 H 최종값까지 일치해야 성공으로 본다.
    verified_rows = _load_device_health_sheet_sms_tracking_rows(
        spreadsheet_id=spreadsheet_id,
        tab_name=tab_name,
        authorized_session=session,
    )
    verified_match = _find_device_health_sheet_sms_tracking_match(
        verified_rows,
        group_id=normalized_group_id,
        tracking_key=preflight_match["trackingKey"],
    )
    return bool(
        verified_match is not None
        and verified_match["trackingKey"] == preflight_match["trackingKey"]
        and verified_match["smsStatus"] == normalized_sms_status
    )


def _append_device_health_sheet_alerts(
    alert_items: list[dict[str, Any]],
    *,
    detected_at: datetime,
    slack_permalink: str,
    authorized_session: Any | None = None,
) -> int | None:
    if not cs.DEVICE_HEALTH_SHEET_ENABLED:
        return None
    spreadsheet_id = str(cs.DEVICE_HEALTH_SHEET_SPREADSHEET_ID or "").strip()
    tab_name = str(cs.DEVICE_HEALTH_SHEET_TAB_NAME or "").strip()
    if not spreadsheet_id or not tab_name:
        raise ValueError("장비 장애 시트 ID 또는 탭 이름이 비어 있어")

    rows = _build_device_health_sheet_rows(
        alert_items,
        detected_at=detected_at,
        slack_permalink=slack_permalink,
    )
    if not rows:
        return 0

    # 탭 이름의 작은따옴표를 Sheets A1 규칙에 맞게 이스케이프하고 URL path도 별도로 인코딩한다.
    quoted_tab_name = tab_name.replace("'", "''")
    append_range = quote(f"'{quoted_tab_name}'!A:R", safe="")
    url = (
        f"{_GOOGLE_SHEETS_API_BASE_URL}/{quote(spreadsheet_id, safe='')}"
        f"/values/{append_range}:append"
    )
    session = authorized_session or _build_device_health_sheet_authorized_session()
    response = session.post(
        url,
        params={
            "valueInputOption": "USER_ENTERED",
            # 미리 준비한 날짜 형식·드롭다운·행 색상을 유지하고 헤더 서식 복제를 막는다.
            "insertDataOption": "OVERWRITE",
        },
        json={"majorDimension": "ROWS", "values": rows},
        timeout=max(1, int(cs.DEVICE_HEALTH_SHEET_TIMEOUT_SEC)),
    )
    response.raise_for_status()
    return len(rows)
