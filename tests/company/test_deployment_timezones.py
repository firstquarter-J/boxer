"""국가별 달력과 DST 경계가 조회, 원격 수집과 답변까지 같은 범위를 유지한다."""

import json
import os
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.request_reference import build_request_reference_context
from boxer_company.deployment import localize_wall_datetime
from boxer_company.recordings_trend_query import parse_recordings_trend_query
from boxer_company.routers import (
    barcode_log,
    box_db,
    device_network_probe,
    device_network_window,
)
from boxer_company.routers.device_file_probe import _build_uploader_recorded_at_ms
from boxer_company.weekly_recordings_report import (
    _weekly_recordings_report_date_range_to_utc_range,
)
from boxer_company_api.automation_scheduler import load_automation_scheduler_settings


@pytest.fixture
def us_profile(monkeypatch):
    monkeypatch.setenv("BOXER_COMPANY_COUNTRY", "us")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    monkeypatch.setenv("BOXER_COMPANY_TIMEZONE", "America/New_York")
    # 서버의 이전 TZ가 남아 있어도 새 명시 설정이 조회의 정본이다.
    monkeypatch.setenv("TZ", "Asia/Seoul")


@pytest.mark.parametrize("zone", ("America/New_York", "America/Los_Angeles"))
@pytest.mark.parametrize("day,hours", (("2026-03-08", 23), ("2026-11-01", 25)))
def test_db_weekly_and_network_share_dst_calendar_day(us_profile, monkeypatch, zone, day, hours):
    monkeypatch.setenv("BOXER_COMPANY_TIMEZONE", zone)
    start, end = box_db._local_date_to_utc_range(day)
    assert end - start == timedelta(hours=hours)
    local_date = date.fromisoformat(day)
    assert _weekly_recordings_report_date_range_to_utc_range(local_date, local_date) == (start, end)
    reference = datetime.combine(local_date + timedelta(days=1), datetime.min.time(), tzinfo=ZoneInfo(zone))
    window = device_network_window.resolve_network_window(day, now=reference)
    assert window["timezone"] == zone
    assert window["endEpoch"] - window["startEpoch"] == hours * 3600
    assert device_network_window.validate_network_window(window) == window
    with pytest.raises(device_network_window.NetworkDateWindowError):
        device_network_window.validate_network_window({**window, "endEpoch": window["endEpoch"] + 1})


def test_question_and_trend_keep_us_day_when_utc_and_kr_are_already_tomorrow(us_profile):
    instant = datetime(2026, 10, 19, 1, 2, 3, tzinfo=UTC)
    stamp = f"{int(instant.timestamp())}.000000"
    request = CompanyAssistantRequest(request_id="us-date", tenant_id="T1", actor_id="U1", channel="slack",
        conversation_id=stamp, question="오늘 녹화가 몇 건이야?", locale="ko", metadata={"channel_id": "C1"},
        memory_context={"workspaceId": "T1", "actorId": "U1", "channelId": "C1", "threadTs": stamp,
            "currentMessageTs": stamp, "triggerKind": "app_mention"})
    rendered = build_request_reference_context(request, now=instant + timedelta(days=1))
    facts = json.loads(rendered.splitlines()[1])
    assert facts["referenceDate"] == "2026-10-18"
    assert facts["referenceTime"] == "2026-10-18T21:02:03.000000-04:00"
    assert facts["timeZone"] == "America/New_York"
    assert "requestedPeriod" not in facts and "KST" not in rendered
    period = parse_recordings_trend_query("이번 주 녹화 추이", now=instant)
    assert period.start == date(2026, 10, 12) and period.end == date(2026, 10, 18)
    assert box_db._format_recorded_at_local(instant.replace(tzinfo=None)) == "2026-10-18 21:02:03"


@pytest.mark.parametrize("wall", ("2026-03-08 02:30:00", "2026-11-01 01:30:00"))
def test_missing_or_duplicate_dst_session_time_never_selects_an_upload(us_profile, wall):
    # offset 없는 세션 시각은 실제 UTC를 하나로 정할 수 있을 때만 복구에 쓴다.
    with pytest.raises(ValueError):
        localize_wall_datetime(datetime.fromisoformat(wall))
    day, clock = wall.split(" ")
    assert _build_uploader_recorded_at_ms(day, clock) == ""


def test_explicit_session_offset_and_unambiguous_wall_time_are_preserved(us_profile):
    aware = datetime.fromisoformat("2026-11-01T01:30:00-05:00")
    assert localize_wall_datetime(aware).astimezone(UTC) == datetime(2026, 11, 1, 6, 30, tzinfo=UTC)
    assert _build_uploader_recorded_at_ms("2026-03-08", "03:30:00") == str(
        int(datetime(2026, 3, 8, 7, 30, tzinfo=UTC).timestamp() * 1000))


def test_bad_timezone_never_falls_back_to_korea(us_profile, monkeypatch):
    monkeypatch.setenv("BOXER_COMPANY_TIMEZONE", "America/Not_A_Zone")
    with pytest.raises(ValueError):
        box_db._local_date_to_utc_range("2026-03-08")


def test_scheduler_uses_injected_profile_and_local_clock_keys():
    # 외부 process 환경이 한국이어도 loader에 주입한 미국 설정으로 일정을 만든다.
    env = {"BOXER_COMPANY_COUNTRY": "us", "AWS_REGION": "us-east-1",
        "BOXER_COMPANY_TIMEZONE": "America/Los_Angeles",
        "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED": "true",
        "BOXER_COMPANY_API_AUTOMATION_TENANT_ID": "T1", "WEEKLY_RECORDINGS_REPORT_ENABLED": "true",
        "WEEKLY_RECORDINGS_REPORT_CHANNEL_ID": "C123456", "WEEKLY_RECORDINGS_REPORT_HOUR_LOCAL": "10"}
    settings = load_automation_scheduler_settings(env)
    assert settings.schedule.timezone == ZoneInfo("America/Los_Angeles")
    assert settings.schedule.weekly_hour == 10
    with pytest.raises(ValueError):
        load_automation_scheduler_settings({**env, "DAILY_DEVICE_ROUND_HOUR_KST": "22"})


def test_remote_network_uses_us_process_timezone_without_zoneinfo_dependency(us_profile):
    window = device_network_window.resolve_network_window("2026-11-01", now=datetime(2026, 11, 2, 12, tzinfo=UTC))
    script = (f"_FIXED_START={window['startEpoch']!r}\n_FIXED_END={window['endEpoch']!r}\n"
        "_TZ_NAME='America/New_York'\n_TZ_LABEL='America/New_York'\n" + device_network_probe._REMOTE_COMMON +
        "\nemit({'status':'ok','ambiguous':local_wall_stamp(datetime.datetime(2026,11,1,1,30)) is None})")
    completed = subprocess.run([sys.executable, "-c", script], env={**os.environ, "TZ": "America/New_York"},
        capture_output=True, text=True, check=True)
    result = json.loads(completed.stdout)
    assert result["ambiguous"] is True
    assert result["networkWindow"]["date"] == "2026-11-01"
    assert result["networkWindow"]["timezone"] == "America/New_York"
    command = device_network_probe._remote_command("def main(): emit({'status':'ok'})", window)
    assert "TZ=America/New_York" in command and "import zoneinfo" not in command


def test_remote_iso_offsets_preserve_both_folds_and_order_by_actual_instant(us_profile, tmp_path):
    # 시계가 뒤로 가도 명시 offset을 가진 두 사건은 버리지 않고 UTC 순서로 유지한다.
    logs = tmp_path / "AppData" / "logs"
    logs.mkdir(parents=True)
    (logs / "log-2026-11-01.log").write_text(
        "2026-11-01T01:59:50-04:00 Socket reconnection error: websocket error\n"
        "2026-11-01T01:00:10-05:00 Socket reconnection error: websocket error\n"
        "2026-11-01T07:00:00Z No Internet Connection\n"
    )
    window = device_network_window.resolve_network_window("2026-11-01", now=datetime(2026, 11, 2, 12, tzinfo=UTC))
    script = (f"_FIXED_START={window['startEpoch']!r}\n_FIXED_END={window['endEpoch']!r}\n"
        "_TZ_NAME='America/New_York'\n_TZ_LABEL='America/New_York'\n" + device_network_probe._REMOTE_COMMON +
        device_network_probe._REMOTE_APP_HISTORY +
        f"\nos.path.expanduser=lambda path:path.replace('~',{str(tmp_path)!r},1)\nmain()")
    completed = subprocess.run([sys.executable, "-c", script], env={**os.environ, "TZ": "America/New_York"},
        capture_output=True, text=True, check=True)
    result = json.loads(completed.stdout)
    assert result["status"] == "ok" and result["counts"]["socket_error"] == 2
    assert [item["timestamp"] for item in result["events"]] == [
        "2026-11-01T01:59:50-04:00", "2026-11-01T01:00:10-05:00", "2026-11-01T02:00:00-05:00",
    ]
    assert all("_epoch" not in item for item in result["events"])
    assert device_network_probe._time_distance("2026-11-01T01:00:10-05:00", "2026-11-01T01:59:50-04:00") == 20


def test_dst_barcode_rows_without_file_id_are_not_assigned_to_first_repeated_session(us_profile, monkeypatch):
    monkeypatch.setenv("BOXER_COMPANY_TIMEZONE", "America/Los_Angeles")
    sessions = [dict(start_line_no=start, end_line_no=start + 1, stop_line_no=start + 1,
        start_time_label="01:10:00", stop_time_label="01:20:00", stop_token="C_STOPSESS") for start in (1, 3)]
    rows = [{"recordedAt": datetime(2026, 11, 1, hour, 15, tzinfo=UTC), "videoLength": 60} for hour in (8, 9)]
    matches = barcode_log._match_recordings_rows_to_sessions(["noop"] * 4, sessions, rows)
    assert matches == {1: [], 2: []}
    assert all(session["recordings_time_ambiguous"] for session in sessions)
    message, _, _ = barcode_log._build_session_recording_result_text(
        ["noop"] * 4, sessions[0], [], [], recordings_on_date_count=2, session_recordings_rows=[])
    assert "연결 확인 불가" in message and "영상 기록 없음" not in message
    card = []
    barcode_log._append_session_card(card, index=1, source_lines=["noop"] * 4, session=sessions[0],
        session_scan_events=[], session_motion_events=[], session_restart_events=[], session_error_lines=[],
        diagnostic_scan_events=[], recordings_on_date_count=2, session_recordings_rows=[])
    assert "DB 영상 기록(세션 기준): 확인 불가" in "\n".join(card)


def test_exact_file_id_keeps_both_dst_barcode_sessions_separate(us_profile, monkeypatch):
    monkeypatch.setenv("BOXER_COMPANY_TIMEZONE", "America/Los_Angeles")
    sessions = [dict(start_line_no=start, end_line_no=start + 1, start_time_label="01:10:00",
        stop_time_label="01:20:00") for start in (1, 3)]
    rows = [{"fileId": f"file{index}", "recordedAt": datetime(2026, 11, 1, hour, 15, tzinfo=UTC)}
        for index, hour in enumerate((8, 9), 1)]
    with patch.object(barcode_log, "_find_recording_recovery_context",
        side_effect=lambda _lines, session: {"fileId": "file1" if session["start_line_no"] == 1 else "file2"}):
        matches = barcode_log._match_recordings_rows_to_sessions(["noop"] * 4, sessions, rows)
    assert matches == {1: [rows[0]], 2: [rows[1]]}
    assert all(not session.get("recordings_time_ambiguous") for session in sessions)


def test_us_remote_app_timezone_mismatch_never_reads_logs_or_reports_pass(us_profile):
    # OS의 로그 날짜 기준을 모르면 배포 zone으로 파일과 원문 시각을 추측하지 않는다.
    script = ("_TZ_NAME='America/New_York'\n_TZ_LABEL='America/New_York'\n_REQUIRE_DEVICE_LOG_TIMEZONE=True\n" +
        device_network_probe._REMOTE_COMMON + device_network_probe._REMOTE_APP_HISTORY +
        "\nfrom unittest.mock import patch\n"
        "with patch('builtins.open',side_effect=FileNotFoundError), patch('os.path.realpath',return_value='/usr/share/zoneinfo/Asia/Seoul'):\n    main()")
    completed = subprocess.run([sys.executable, "-c", script], env={**os.environ, "TZ": "America/New_York"},
        capture_output=True, text=True, check=True)
    result = json.loads(completed.stdout)
    assert result["status"] == "timezone_mismatch" and result["filesRead"] == 0 and result["events"] == []
    window = device_network_window.resolve_network_window("2026-11-01", now=datetime(2026, 11, 2, 12, tzinfo=UTC))
    summary = device_network_probe.build_network_summary({
        "network_state": {"ok": True, "output": json.dumps({"status": "ok", "interface": "enp1s0",
            "connectionType": "wired", "carrier": True, "hasDefaultRoute": True})},
        "network_link_history": {"ok": True, "output": json.dumps({"status": "ok", "events": []})},
        "network_app_history": {"ok": True, "output": json.dumps(result)},
    }, window)
    assert summary["status"] != "pass" and "장비 로그 시간대 확인 필요" in summary["summary"]
