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
    '=IF(INDIRECT("I"&ROW())="완료",'
    'IF(OR(INDIRECT("K"&ROW())="",INDIRECT("K"&ROW())=0),'
    'NOW()+9/24,INDIRECT("K"&ROW())),"")'
)
_WORK_DURATION_FORMULA = (
    '=IF(OR(INDIRECT("A"&ROW())="",INDIRECT("K"&ROW())=""),"",'
    'LET(total,ROUND((INDIRECT("K"&ROW())-INDIRECT("A"&ROW()))*86400),'
    'hours,INT(total/3600),minutes,INT(MOD(total,3600)/60),seconds,MOD(total,60),'
    'TRIM(IF(hours>0,hours&"시간 ","")&IF(minutes>0,minutes&"분 ","")&'
    'IF(OR(seconds>0,AND(hours=0,minutes=0)),seconds&"초",""))))'
)


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
                and _display_value(component, default="").lower()
                not in _NON_DEVICE_PROBLEM_COMPONENTS
            )
            if isinstance(problem_components, list)
            else ""
        )
        # G~N은 TA 처리 영역이다. 신규 장애는 I열 대기로 시작하고 K/L열은 시트가 계산한다.
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
                "대기",
                "",
                _COMPLETED_AT_FORMULA,
                _WORK_DURATION_FORMULA,
                "",
                "",
                permalink,
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

    # 장비·문제장치·감지내용·상태·permalink가 포함된 B:O를 한 번에 읽어
    # TA가 갱신한 현재 상태와 원본 Slack 알림을 같은 물리 행 기준으로 판단한다.
    quoted_tab_name = tab_name.replace("'", "''")
    read_range = quote(f"'{quoted_tab_name}'!B2:O", safe="")
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
            "status": _display_value(row[7] if len(row) > 7 else None, default=""),
            "slackPermalink": _display_value(
                row[13] if len(row) > 13 else None,
                default="",
            ),
            "rowNumber": row_number,
        }
    return incidents


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
    append_range = quote(f"'{quoted_tab_name}'!A:O", safe="")
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
