from datetime import datetime
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from boxer.core.utils import _display_value
from boxer_company import settings as cs

_GOOGLE_SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
_GOOGLE_SHEETS_API_BASE_URL = "https://sheets.googleapis.com/v4/spreadsheets"
_GOOGLE_SHEETS_SERIAL_EPOCH = datetime(1899, 12, 30)
_KST = ZoneInfo("Asia/Seoul")
_NON_DEVICE_PROBLEM_COMPONENTS = {"용량", "storage", "디스크", "저장공간", "trashcan"}


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


def _format_device_health_sheet_duration(duration_minutes: float) -> str:
    total_seconds = max(0, int(round(duration_minutes * 60)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    parts: list[str] = []
    if hours:
        parts.append(f"{hours}시간")
    if minutes:
        parts.append(f"{minutes}분")
    if seconds or not parts:
        parts.append(f"{seconds}초")
    return " ".join(parts)


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
        # G~N은 TA 처리 영역이며 신규 장애의 처리상태는 담당자가 선택하도록 비워 둔다.
        rows.append(
            [
                detected_at_serial,
                _display_value(item.get("device"), default="장비명 미확인"),
                _display_value(item.get("hospitalName"), default="병원 미확인"),
                _display_value(item.get("room"), default="병실 미확인"),
                problem_device,
                _display_value(item.get("issue"), default="상세 확인 필요"),
                "",
                "",
                "",
                "",
                "",
                "",
                permalink,
                "",
            ]
        )
    return rows


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
    append_range = quote(f"'{quoted_tab_name}'!A:N", safe="")
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


def _stamp_device_health_sheet_status_times(
    *,
    now: datetime,
    authorized_session: Any | None = None,
) -> int | None:
    if not cs.DEVICE_HEALTH_SHEET_ENABLED:
        return None
    spreadsheet_id = str(cs.DEVICE_HEALTH_SHEET_SPREADSHEET_ID or "").strip()
    tab_name = str(cs.DEVICE_HEALTH_SHEET_TAB_NAME or "").strip()
    if not spreadsheet_id or not tab_name:
        raise ValueError("장비 장애 시트 ID 또는 탭 이름이 비어 있어")

    quoted_tab_name = tab_name.replace("'", "''")
    status_range = quote(f"'{quoted_tab_name}'!A2:N", safe="")
    base_url = f"{_GOOGLE_SHEETS_API_BASE_URL}/{quote(spreadsheet_id, safe='')}"
    session = authorized_session or _build_device_health_sheet_authorized_session()
    response = session.get(
        f"{base_url}/values/{status_range}",
        params={"majorDimension": "ROWS", "valueRenderOption": "UNFORMATTED_VALUE"},
        timeout=max(1, int(cs.DEVICE_HEALTH_SHEET_TIMEOUT_SEC)),
    )
    response.raise_for_status()

    timestamp = _device_health_sheet_serial_datetime(now)
    updates: list[dict[str, Any]] = []
    updated_row_count = 0
    for row_number, row in enumerate(response.json().get("values", []), start=2):
        if not isinstance(row, list) or len(row) <= 7:
            continue
        status = _display_value(row[7], default="")
        if status != "완료":
            continue

        detected_at = row[0] if isinstance(row[0], (int, float)) else None
        completed_at = row[8] if len(row) > 8 and isinstance(row[8], (int, float)) else None
        work_duration = row[9] if len(row) > 9 else ""
        work_duration_minutes = row[13] if len(row) > 13 else ""
        effective_completed_at = completed_at if completed_at is not None else timestamp
        row_updated = False
        if completed_at is None:
            updates.append(
                {"range": f"'{quoted_tab_name}'!I{row_number}", "values": [[effective_completed_at]]}
            )
            row_updated = True

        if detected_at is not None and effective_completed_at >= detected_at:
            duration_minutes = (effective_completed_at - detected_at) * 1440
            if work_duration in {"", None}:
                updates.append(
                    {
                        "range": f"'{quoted_tab_name}'!J{row_number}",
                        "values": [[_format_device_health_sheet_duration(duration_minutes)]],
                    }
                )
                row_updated = True
            if work_duration_minutes in {"", None}:
                updates.append(
                    {
                        "range": f"'{quoted_tab_name}'!N{row_number}",
                        "values": [[round(duration_minutes, 1)]],
                    }
                )
                row_updated = True
        if row_updated:
            updated_row_count += 1

    if not updates:
        return 0
    update_response = session.post(
        f"{base_url}/values:batchUpdate",
        json={"valueInputOption": "USER_ENTERED", "data": updates},
        timeout=max(1, int(cs.DEVICE_HEALTH_SHEET_TIMEOUT_SEC)),
    )
    update_response.raise_for_status()
    return updated_row_count
