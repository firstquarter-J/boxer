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
            )
            if isinstance(problem_components, list)
            else ""
        )
        # G~N은 TA 수동 처리 영역이고 처리상태만 신규 장애의 기본값인 대기로 채운다.
        rows.append(
            [
                detected_at_serial,
                _display_value(item.get("device"), default="장비명 미확인"),
                _display_value(item.get("hospitalName"), default="병원 미확인"),
                _display_value(item.get("room"), default="병실 미확인"),
                problem_device,
                _display_value(item.get("issue"), default="상세 확인 필요"),
                "",
                "대기",
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
            "insertDataOption": "INSERT_ROWS",
        },
        json={"majorDimension": "ROWS", "values": rows},
        timeout=max(1, int(cs.DEVICE_HEALTH_SHEET_TIMEOUT_SEC)),
    )
    response.raise_for_status()
    return len(rows)
