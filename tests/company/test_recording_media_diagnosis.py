"""실제 환자 파일 없이 중앙 영상 진단의 대상, 자원 한계와 판정을 검증한다."""

from __future__ import annotations

import io
import json
import shutil
import stat
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

from boxer_company import recording_media_diagnosis as media

_BARCODE = "81000000000"
_FILE_ID = "SYNTHETIC260001"
_DEVICE = "MB2-X00001"
_BUCKET = "ultrasound-prod-kr"
_OWNER = "123456789012"


@pytest.mark.parametrize(
    ("message", "expected"),
    [
        ("[h264 @ 0x00] error while decoding MB 81 34", "h264_corrupt_frame"),
        ("[hevc @ 0x00] corrupt decoded frame", "video_corrupt_frame"),
        ("corrupt decoded frame", "video_corrupt_frame"),
        ("Permission denied\n[h264 @ 0x00] corrupt decoded frame", "permission_denied"),
    ],
)
def test_corrupt_frame_only_names_h264_when_decoder_identifies_codec(
    message, expected
) -> None:
    # 공통 decoder 오류 문구만으로 다른 codec까지 H.264 손상으로 설명하지 않는다.
    assert media._diagnostic_code(message) == expected


def _recording(**overrides: object) -> dict:
    return {
        "seq": 901,
        "deviceSeq": 90,
        "fileId": _FILE_ID,
        "s3Bucket": _BUCKET,
        "s3FileKey": f"{_BARCODE}/{_FILE_ID}.mp4",
        "streamingStatus": "AVAILABLE",
        "deviceName": _DEVICE,
        **overrides,
    }


def _notification(**overrides: object) -> dict:
    return {
        "id": 71,
        "deviceSeq": 90,
        "deviceName": _DEVICE,
        "barcode": _BARCODE,
        "fileId": _FILE_ID,
        "details": {"issues": ["video_decode_failed"], "durationSec": 2},
        **overrides,
    }


def _load_target(**overrides: object) -> dict:
    return media._load_media_target(
        **{
            "notification_id": None,
            "barcode": None,
            "device_name": None,
            "log_date": None,
            "file_id": None,
            **overrides,
        }
    )


@pytest.fixture
def database(monkeypatch: pytest.MonkeyPatch) -> tuple[MagicMock, MagicMock]:
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = _notification()
    cursor.fetchall.return_value = [_recording()]
    monkeypatch.setattr(media.core_settings, "DB_QUERY_ENABLED", True)
    monkeypatch.setattr(media.settings, "S3_ULTRASOUND_BUCKET", _BUCKET)
    monkeypatch.setattr(media.settings, "S3_ULTRASOUND_BUCKET_OWNER_ID", _OWNER)
    monkeypatch.setattr(media, "_create_db_connection", Mock(return_value=connection))
    return connection, cursor


def test_notification_pk_selects_exact_recording_and_reuses_barcode_index(
    database,
) -> None:
    connection, cursor = database
    result = _load_target(notification_id=71)

    assert result["status"] == "ready"
    assert result["notificationId"] == 71
    assert result["_key"] == f"{_BARCODE}/{_FILE_ID}.mp4"
    first_sql, first_params = cursor.execute.call_args_list[0].args
    assert "WHERE id = %s AND code = %s" in first_sql
    assert first_params == (71, "recording_media_quality_issue")
    recording_sql, recording_params = cursor.execute.call_args_list[1].args
    assert "r.fullBarcode" in recording_sql and "r.barcode" in recording_sql
    assert "r.fileId = %s" in recording_sql
    assert "LIMIT 2" in recording_sql
    assert _FILE_ID in recording_params
    assert all(value != 71 for value in recording_params)
    connection.close.assert_called_once_with()


@pytest.mark.parametrize(
    "requested",
    [
        {"barcode": "81000000001"},
        {"file_id": "OTHERFILE"},
        {"device_name": "MB2-X00002"},
    ],
)
def test_notification_conflicting_scope_stops_before_recording_query(
    database, requested
) -> None:
    _, cursor = database
    assert _load_target(notification_id=71, **requested) == {
        "status": "needs_input",
        "reason": "target_conflict",
    }
    assert cursor.execute.call_count == 1


@pytest.mark.parametrize(
    ("notification", "reason"),
    [
        ({}, "notification_not_found"),
        (_notification(fileId="../private-file"), "invalid_notification_target"),
    ],
)
def test_missing_or_invalid_notification_never_queries_recordings(
    database, notification, reason
) -> None:
    _, cursor = database
    cursor.fetchone.return_value = notification
    assert _load_target(notification_id=71)["reason"] == reason
    assert cursor.execute.call_count == 1


@pytest.mark.parametrize(
    "requested",
    [
        {},
        {"notification_id": 0},
        {"notification_id": True},
        {"barcode": "8100"},
        {"barcode": _BARCODE, "file_id": "../wrong"},
        {"barcode": _BARCODE, "device_name": "../device"},
        {"barcode": _BARCODE, "log_date": "2026-02-30"},
        {"barcode": _BARCODE, "recorded_at": "2026-02-30 17:19:43"},
        {"barcode": _BARCODE, "recorded_at": "2026-11-02"},
    ],
)
def test_invalid_target_does_not_open_database(database, requested) -> None:
    connection, cursor = database
    result = _load_target(**requested)
    assert result["status"] == "needs_input"
    cursor.execute.assert_not_called()
    connection.close.assert_not_called()


def test_barcode_multiple_recordings_requires_disambiguation(database) -> None:
    _, cursor = database
    cursor.fetchall.return_value = [_recording(), _recording(seq=902, fileId="SECOND")]
    assert _load_target(barcode=_BARCODE)["reason"] == "ambiguous_recording"


def test_korean_date_uses_half_open_utc_recorded_at_bounds(database) -> None:
    _, cursor = database
    assert _load_target(barcode=_BARCODE, log_date="2026-11-02")["status"] == "ready"
    sql, params = cursor.execute.call_args_list[0].args
    assert "r.recordedAt >= %s" in sql and "r.recordedAt < %s" in sql
    assert params[-2:] == (datetime(2026, 11, 1, 15), datetime(2026, 11, 2, 15))


@pytest.mark.parametrize(
    ("recorded_at", "start", "end"),
    [
        (
            "2026-11-02 17:19:43",
            datetime(2026, 11, 2, 8, 19, 43),
            datetime(2026, 11, 2, 8, 19, 44),
        ),
        (
            "2026-11-02 00:00:00",
            datetime(2026, 11, 1, 15),
            datetime(2026, 11, 1, 15, 0, 1),
        ),
    ],
)
def test_card_session_start_selects_its_one_second_utc_window(
    database, recorded_at, start, end
) -> None:
    _, cursor = database
    result = _load_target(
        barcode=_BARCODE, device_name=_DEVICE, recorded_at=recorded_at
    )
    assert result["status"] == "ready"
    sql, params = cursor.execute.call_args_list[0].args
    assert "d.deviceName = %s" in sql
    assert "r.recordedAt >= %s" in sql and "r.recordedAt < %s" in sql
    assert params[-3:] == (_DEVICE, start, end)


def test_date_and_card_session_are_intersected_without_falling_back_to_another_video(
    database,
) -> None:
    _, cursor = database
    cursor.fetchall.return_value = []
    result = _load_target(
        barcode=_BARCODE,
        log_date="2026-11-01",
        recorded_at="2026-11-02 17:19:43",
    )
    assert result["reason"] == "recording_not_found"
    sql, params = cursor.execute.call_args.args
    assert sql.count("r.recordedAt >= %s") == 2 and sql.count("r.recordedAt < %s") == 2
    assert params[-4:] == (
        datetime(2026, 10, 31, 15),
        datetime(2026, 11, 1, 15),
        datetime(2026, 11, 2, 8, 19, 43),
        datetime(2026, 11, 2, 8, 19, 44),
    )
    assert cursor.execute.call_count == 1


def test_barcode_resolution_enriches_only_same_device_barcode_and_file_notification(
    database,
) -> None:
    connection, cursor = database
    result = _load_target(barcode=_BARCODE)
    assert result["status"] == "ready"
    assert result["notificationId"] == 71
    assert result["notificationIssues"] == ["video_decode_failed"]
    sql, params = cursor.execute.call_args_list[1].args
    assert "WHERE deviceSeq = %s AND barcode = %s AND fileId = %s AND code = %s" in sql
    assert "ORDER BY id DESC LIMIT 1" in sql
    assert params == (90, _BARCODE, _FILE_ID, "recording_media_quality_issue")
    connection.close.assert_called_once_with()


def test_optional_original_notification_failure_does_not_block_resolved_video(
    database,
) -> None:
    connection, cursor = database
    cursor.execute.side_effect = [None, RuntimeError("private DB detail")]
    result = _load_target(barcode=_BARCODE)
    assert result["status"] == "ready"
    assert result["notificationIssues"] == []
    assert "private DB detail" not in json.dumps(result)
    connection.close.assert_called_once_with()


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        (_recording(deviceSeq=91), "recording_device_mismatch"),
        (_recording(streamingStatus="UPLOADING"), "upload_not_ready"),
        (_recording(s3Bucket="other-bucket"), "invalid_s3_location"),
        (_recording(s3FileKey=f"81000000001/{_FILE_ID}.mp4"), "invalid_s3_location"),
        (_recording(s3FileKey=f"{_BARCODE}/../{_FILE_ID}.mp4"), "invalid_s3_location"),
    ],
)
def test_target_cannot_expand_to_other_device_bucket_or_object(
    database, row, reason
) -> None:
    _, cursor = database
    cursor.fetchall.return_value = [row]
    result = _load_target(notification_id=71)
    assert result["status"] == "unavailable"
    assert result["reason"] == reason
    assert not any(key.startswith("_") for key in result)


def test_notification_evidence_removes_paths_raw_errors_and_unknown_fields(
    database,
) -> None:
    _, cursor = database
    cursor.fetchone.return_value = _notification(
        details=json.dumps(
            {
                "issues": ["video_decode_failed", "private-secret", {"injected": True}],
                "durationSec": 2,
                "stderr": "private-secret",
                "filePath": "/private/video.mp4",
                "video": {
                    "reason": "private-secret",
                    "diagnosticCode": "invalid_media_data",
                    "stderr": "private-secret",
                    "visualAnalysis": {"raw": "private-secret"},
                },
                "audio": {"stderr": "private-secret"},
            }
        )
    )
    result = _load_target(notification_id=71)
    assert result["notificationIssues"] == ["video_decode_failed"]
    assert (
        result["notificationDetails"]["video"]["diagnosticCode"] == "invalid_media_data"
    )
    encoded = json.dumps(result["notificationDetails"])
    assert "private-secret" not in encoded and "/private/" not in encoded


@pytest.mark.parametrize(
    "invalid_row",
    [
        _recording(fileId="/private/credential"),
        _recording(deviceName="/private/device"),
    ],
)
def test_invalid_recording_identifiers_are_rejected_before_public_evidence(
    database, invalid_row
) -> None:
    _, cursor = database
    cursor.fetchall.return_value = [invalid_row]
    result = _load_target(barcode=_BARCODE)
    assert result["reason"] == "invalid_recording_target"
    assert "fileId" not in result and "deviceName" not in result
    assert "private" not in json.dumps(result)


def _s3(
    monkeypatch: pytest.MonkeyPatch, data: bytes, *, declared_size: int | None = None
):
    stream = io.BytesIO(data)
    body = Mock()
    body.read.side_effect = stream.read
    client = Mock()
    client.get_object.return_value = {
        "Body": body,
        "ContentLength": len(data) if declared_size is None else declared_size,
    }
    boto3 = Mock()
    boto3.client.return_value = client
    config = Mock()
    monkeypatch.setattr(
        media, "_load_boto3_components", Mock(return_value=(boto3, config))
    )
    monkeypatch.setattr(media.core_settings, "S3_QUERY_ENABLED", True)
    return client, body, config


def _download_target() -> dict:
    return {"_bucket": _BUCKET, "_key": f"{_BARCODE}/{_FILE_ID}.mp4", "_owner": _OWNER}


def test_download_reads_only_exact_object_with_owner_and_restricts_file_mode(
    monkeypatch, tmp_path
) -> None:
    client, body, config = _s3(monkeypatch, b"synthetic-video")
    path = tmp_path / "recording.mp4"
    media._download_media_in_process(_download_target(), path)
    assert path.read_bytes() == b"synthetic-video"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    client.get_object.assert_called_once_with(
        Bucket=_BUCKET,
        Key=f"{_BARCODE}/{_FILE_ID}.mp4",
        ExpectedBucketOwner=_OWNER,
    )
    assert config.call_args.kwargs["retries"]["total_max_attempts"] == 1
    client.close.assert_called_once_with()
    body.close.assert_called_once_with()


@pytest.mark.parametrize(
    ("data", "declared_size", "reason"),
    [
        (b"123", 9, "media_size_limit"),
        (b"123456789", 8, "media_size_limit"),
        (b"12345", 4, "media_size_limit"),
        (b"123", 4, "download_incomplete"),
        (b"", 0, "media_size_limit"),
    ],
)
def test_download_bounds_declared_and_actual_sizes_and_closes_resources(
    monkeypatch,
    tmp_path,
    data,
    declared_size,
    reason,
) -> None:
    monkeypatch.setattr(media, "_MAX_BYTES", 8)
    client, body, _ = _s3(monkeypatch, data, declared_size=declared_size)
    with pytest.raises(media._InspectionUnavailable, match=f"^{reason}$"):
        media._download_media_in_process(_download_target(), tmp_path / "recording.mp4")
    client.close.assert_called_once_with()
    body.close.assert_called_once_with()


def test_download_deadline_is_inconclusive_and_closes_stream(
    monkeypatch, tmp_path
) -> None:
    client, body, _ = _s3(monkeypatch, b"123")
    monkeypatch.setattr(
        media.time, "monotonic", Mock(side_effect=[0, media._DOWNLOAD_SEC + 1])
    )
    with pytest.raises(media._InspectionUnavailable, match="^download_timeout$"):
        media._download_media_in_process(_download_target(), tmp_path / "recording.mp4")
    body.read.assert_not_called()
    body.close.assert_called_once_with()
    client.close.assert_called_once_with()


def test_s3_provider_error_does_not_become_video_corruption_or_leak_raw_message(
    monkeypatch, tmp_path
) -> None:
    client, _, _ = _s3(monkeypatch, b"123")
    client.get_object.side_effect = RuntimeError("AccessDenied /private/credential")
    with pytest.raises(media._InspectionUnavailable, match="^media_read_failed$"):
        media._download_media_in_process(_download_target(), tmp_path / "recording.mp4")
    client.close.assert_called_once_with()


def test_provider_exception_text_never_crosses_public_result_boundary(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        media,
        "_load_media_target",
        Mock(
            side_effect=RuntimeError(
                "secret credential and /private/path",
            )
        ),
    )
    assert media.diagnose_recording_media(notification_id=71) == {
        "status": "unavailable",
        "reason": "inspection_failed",
    }


@pytest.mark.parametrize(
    ("output", "returncode", "reason"),
    [
        (b'{"status":"downloaded"}', 0, None),
        (
            b'{"status":"unavailable","reason":"media_size_limit"}',
            0,
            "media_size_limit",
        ),
        (
            b'{"status":"unavailable","reason":"download_timeout"}',
            0,
            "download_timeout",
        ),
        (b'{"status":"unavailable","reason":"private-secret"}', 0, "media_read_failed"),
        (b'{"status":"downloaded"}', 1, "media_read_failed"),
        (b"x" * 1025, 0, "media_read_failed"),
    ],
)
def test_download_supervisor_uses_only_bounded_status_json_and_server_metadata(
    monkeypatch,
    tmp_path,
    output,
    returncode,
    reason,
) -> None:
    process = Mock()
    process.communicate.return_value = (output, None)
    process.returncode = returncode
    process.poll.return_value = returncode
    launch = Mock(return_value=process)
    monkeypatch.setattr(media.subprocess, "Popen", launch)
    path = tmp_path / "recording.mp4"
    target = {**_download_target(), "notificationDetails": "private-secret"}
    if reason:
        with pytest.raises(media._InspectionUnavailable, match=f"^{reason}$"):
            media._download_media(target, path)
    else:
        media._download_media(target, path)
    command = launch.call_args.args[0]
    assert command == [sys.executable, "-m", "boxer_company.recording_media_diagnosis"]
    assert launch.call_args.kwargs["stderr"] == subprocess.DEVNULL
    assert launch.call_args.kwargs["shell"] is False
    payload = json.loads(process.communicate.call_args.kwargs["input"])
    assert payload == {"target": _download_target(), "path": str(path)}
    assert "private-secret" not in json.dumps(payload)
    process.kill.assert_not_called()


def test_real_download_supervisor_kills_slow_child_and_parent_removes_partial_file(
    monkeypatch,
) -> None:
    # SDK 대신 파일 작성 후 잠드는 합성 child를 실행해 전체시간 제한을 실제로 검증한다.
    original_popen = subprocess.Popen
    original_temporary = media.tempfile.TemporaryDirectory
    processes = []
    directories: list[Path] = []

    def launch(_args, **kwargs):
        process = original_popen(
            [
                sys.executable,
                "-c",
                (
                    "import json,sys,time; from pathlib import Path; "
                    "payload=json.loads(sys.stdin.buffer.read()); "
                    "Path(payload['path']).write_bytes(b'synthetic'); time.sleep(5)"
                ),
            ],
            **kwargs,
        )
        processes.append(process)
        return process

    def temporary(**kwargs):
        directory = original_temporary(**kwargs)
        directories.append(Path(directory.name))
        return directory

    monkeypatch.setattr(media.subprocess, "Popen", launch)
    monkeypatch.setattr(media.tempfile, "TemporaryDirectory", temporary)
    monkeypatch.setattr(media, "_DOWNLOAD_SEC", 0.2)
    monkeypatch.setattr(media, "_load_media_target", Mock(return_value=_ready_target()))
    monkeypatch.setattr(media.shutil, "which", Mock(return_value="/mock/ffmpeg"))
    start = time.monotonic()
    result = media.diagnose_recording_media(notification_id=71)
    assert result["reason"] == "download_timeout"
    assert time.monotonic() - start < 2
    assert len(processes) == 1 and processes[0].poll() is not None
    assert directories and not directories[0].exists()
    assert media._CHECK_LOCK.acquire(blocking=False)
    media._CHECK_LOCK.release()


def _ready_target() -> dict:
    return {
        "status": "ready",
        "barcode": _BARCODE,
        "fileId": _FILE_ID,
        "notificationId": 71,
        **_download_target(),
    }


def test_missing_tools_returns_unavailable_without_downloading(monkeypatch) -> None:
    monkeypatch.setattr(media, "_load_media_target", Mock(return_value=_ready_target()))
    monkeypatch.setattr(media.shutil, "which", Mock(return_value=None))
    downloader = Mock()
    monkeypatch.setattr(media, "_download_media", downloader)
    result = media.diagnose_recording_media(notification_id=71)
    assert result["reason"] == "media_tools_missing"
    assert not any(key.startswith("_") for key in result)
    downloader.assert_not_called()


def test_public_service_preserves_explicit_card_session_scope(monkeypatch) -> None:
    loader = Mock(return_value={"status": "not_found", "reason": "recording_not_found"})
    monkeypatch.setattr(media, "_load_media_target", loader)
    result = media.diagnose_recording_media(
        barcode=_BARCODE, recorded_at="2026-11-02 17:19:43"
    )
    assert result["status"] == "not_found"
    assert loader.call_args.kwargs["recorded_at"] == "2026-11-02 17:19:43"


def test_busy_inspection_never_queries_target(monkeypatch) -> None:
    loader = Mock()
    monkeypatch.setattr(media, "_load_media_target", loader)
    assert media._CHECK_LOCK.acquire(blocking=False)
    try:
        assert media.diagnose_recording_media(notification_id=71) == {
            "status": "unavailable",
            "reason": "inspection_busy",
        }
        loader.assert_not_called()
    finally:
        media._CHECK_LOCK.release()


@pytest.mark.parametrize("failure", [False, True])
def test_private_temporary_media_and_lock_are_cleaned_on_every_exit(
    monkeypatch, failure
) -> None:
    paths: list[Path] = []
    monkeypatch.setattr(media, "_load_media_target", Mock(return_value=_ready_target()))
    monkeypatch.setattr(media.shutil, "which", Mock(return_value="/mock/ffmpeg"))

    def download(_target: dict, path: Path) -> None:
        paths.append(path)
        assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
        path.write_bytes(b"synthetic")
        if failure:
            raise media._InspectionUnavailable("media_read_failed")

    monkeypatch.setattr(media, "_download_media", download)
    monkeypatch.setattr(media, "_probe_video", Mock(return_value=2))
    monkeypatch.setattr(
        media, "_decode_video", Mock(return_value={"status": "completed"})
    )
    result = media.diagnose_recording_media(notification_id=71)
    assert result["status"] == ("unavailable" if failure else "analyzed")
    assert paths and not paths[0].parent.exists()
    assert media._CHECK_LOCK.acquire(blocking=False)
    media._CHECK_LOCK.release()
    assert not any(key.startswith("_") for key in result)


@pytest.mark.parametrize(
    ("strict", "normal", "findings", "calls"),
    [
        ({"status": "completed"}, None, ["video_decode_clean"], 1),
        (
            {"status": "unknown", "reason": "media_process_timeout"},
            None,
            ["decode_inconclusive"],
            1,
        ),
        (
            {"status": "decode_error", "diagnosticCode": "h264_corrupt_frame"},
            {"status": "completed"},
            ["partial_frame_corruption", "strict_decode_failed_normal_completed"],
            2,
        ),
    ],
)
def test_strict_decode_failure_and_inspection_timeout_have_different_meanings(
    monkeypatch,
    strict,
    normal,
    findings,
    calls,
) -> None:
    monkeypatch.setattr(media, "_load_media_target", Mock(return_value=_ready_target()))
    monkeypatch.setattr(media.shutil, "which", Mock(return_value="/mock/ffmpeg"))
    monkeypatch.setattr(media, "_download_media", Mock())
    monkeypatch.setattr(media, "_probe_video", Mock(return_value=2))
    decoder = Mock(side_effect=[strict, normal])
    monkeypatch.setattr(media, "_decode_video", decoder)
    result = media.diagnose_recording_media(notification_id=71)
    assert result["findings"] == findings
    assert decoder.call_count == calls


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (
            {
                "returncode": 0,
                "reason": None,
                "diagnosticCode": None,
                "stdout": "frame=20\nout_time_us=2000000\nprogress=end\n",
            },
            "completed",
        ),
        (
            {
                "returncode": 0,
                "reason": None,
                "diagnosticCode": None,
                "stdout": "frame=10\nout_time_us=1000000\nprogress=end\n",
            },
            "unknown",
        ),
        (
            {
                "returncode": 0,
                "reason": None,
                "diagnosticCode": None,
                "stdout": "frame=0\nout_time_us=2000000\nprogress=end\n",
            },
            "unknown",
        ),
        (
            {
                "returncode": 0,
                "reason": None,
                "diagnosticCode": "h264_corrupt_frame",
                "stdout": "frame=20\nout_time_us=2000000\nprogress=end\n",
            },
            "unknown",
        ),
        (
            {
                "returncode": 1,
                "reason": None,
                "diagnosticCode": "unsupported_codec",
                "stdout": "",
            },
            "unknown",
        ),
        (
            {
                "returncode": 1,
                "reason": None,
                "diagnosticCode": "h264_corrupt_frame",
                "stdout": "",
            },
            "decode_error",
        ),
        (
            {
                "returncode": -9,
                "reason": "media_process_timeout",
                "diagnosticCode": "h264_corrupt_frame",
                "stdout": "",
            },
            "unknown",
        ),
    ],
)
def test_decoder_only_claims_completion_after_full_nonempty_video(
    monkeypatch, result, expected
) -> None:
    process = Mock(return_value=result)
    monkeypatch.setattr(media, "_run_process", process)
    decoded = media._decode_video("ffmpeg", Path("/private/input.mp4"), 2, strict=True)
    assert decoded["status"] == expected
    args = process.call_args.args[0]
    assert args[args.index("-protocol_whitelist") + 1] == "file"
    assert args[args.index("-f") + 1] == "mov"
    assert "-nostdin" in args and "-xerror" in args
    assert "-an" in args


@pytest.mark.parametrize(
    "metadata",
    [
        {"streams": [], "format": {"duration": "2"}},
        {"streams": [{"width": 99999, "height": 96, "duration": "2"}]},
        {"streams": [{"width": 160, "height": 96, "duration": "nan"}]},
        {"streams": [{"width": 160, "height": 96, "duration": "7201"}]},
    ],
)
def test_probe_rejects_absent_or_excessive_video_before_decoding(
    monkeypatch, metadata
) -> None:
    monkeypatch.setattr(
        media,
        "_run_process",
        Mock(
            return_value={
                "returncode": 0,
                "reason": None,
                "diagnosticCode": None,
                "stdout": json.dumps(metadata),
            }
        ),
    )
    with pytest.raises(media._InspectionUnavailable):
        media._probe_video("ffprobe", Path("/private/input.mp4"))


def test_process_timeout_and_output_limit_are_bounded_without_leaking_stderr(
    monkeypatch,
) -> None:
    # Python child 하나로 timeout과 큰 stdout을 검증해 FFmpeg 설치 여부와 분리한다.
    timeout = media._run_process(
        [sys.executable, "-c", "import time; time.sleep(2)"], 0.05
    )
    assert timeout["reason"] == "media_process_timeout"
    monkeypatch.setattr(media, "_MAX_OUTPUT_BYTES", 32)
    oversized = media._run_process([sys.executable, "-c", "print('x' * 1000)"], 2)
    assert oversized["reason"] == "media_output_limit"
    assert len(oversized["stdout"].encode()) <= 32
    diagnostic = media._run_process(
        [
            sys.executable,
            "-c",
            "import sys; sys.stderr.write('private-secret /private/path Invalid NAL unit'); sys.exit(1)",
        ],
        2,
    )
    assert diagnostic["diagnosticCode"] == "invalid_media_data"
    assert "private-secret" not in json.dumps(diagnostic)


@pytest.fixture
def synthetic_mp4(tmp_path: Path) -> tuple[Path, str, str]:
    ffmpeg, ffprobe = shutil.which("ffmpeg"), shutil.which("ffprobe")
    if not ffmpeg or not ffprobe:
        pytest.skip("합성 영상 통합 검증에 필요한 FFmpeg 도구가 없어")
    path = tmp_path / "synthetic.mp4"
    generated = subprocess.run(
        [
            ffmpeg,
            "-hide_banner",
            "-v",
            "error",
            "-f",
            "lavfi",
            "-i",
            "testsrc2=size=160x96:rate=10",
            "-t",
            "2",
            "-an",
            "-c:v",
            "libx264",
            "-threads",
            "1",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            str(path),
        ],
        capture_output=True,
        timeout=10,
        check=False,
    )
    if generated.returncode:
        pytest.skip("설치된 FFmpeg가 합성 H264 생성기를 지원하지 않아")
    return path, ffmpeg, ffprobe


def test_real_synthetic_video_completes_strict_and_normal_decoding(
    synthetic_mp4,
) -> None:
    path, ffmpeg, ffprobe = synthetic_mp4
    duration = media._probe_video(ffprobe, path)
    assert duration == pytest.approx(2)
    for strict in (True, False):
        result = media._decode_video(ffmpeg, path, duration, strict=strict)
        assert result["status"] == "completed"
        assert result["decodedFrames"] == 20
        assert result["diagnosticCode"] is None


def test_notification_to_mock_s3_and_real_decoder_returns_safe_result_and_removes_copy(
    monkeypatch,
    database,
    synthetic_mp4,
) -> None:
    # DB와 S3만 대체하고 대상 판정, 임시파일 생성과 실제 디코더를 함께 실행한다.
    source, _, _ = synthetic_mp4
    client, body, _ = _s3(monkeypatch, source.read_bytes())
    monkeypatch.setattr(media, "_download_media", media._download_media_in_process)
    original_probe = media._probe_video
    probe = Mock(wraps=original_probe)
    monkeypatch.setattr(media, "_probe_video", probe)
    result = media.diagnose_recording_media(notification_id=71)
    assert result["status"] == "analyzed"
    assert result["findings"] == ["video_decode_clean"]
    assert result["strictDecode"]["decodedFrames"] == 20
    inspected_copy = probe.call_args.args[1]
    assert not inspected_copy.parent.exists()
    assert not any(key.startswith("_") for key in result)
    assert _BUCKET not in json.dumps(result)
    client.close.assert_called_once_with()
    body.close.assert_called_once_with()


@pytest.mark.parametrize("preserved_percent", [75, 85])
def test_truncated_synthetic_video_cannot_be_reported_as_fully_decoded(
    synthetic_mp4,
    preserved_percent,
) -> None:
    # moov는 남기고 mdat 꼬리를 제거해 메타데이터가 정상이어도 본문이 손상된 경우를 만든다.
    path, ffmpeg, ffprobe = synthetic_mp4
    original = path.read_bytes()
    path.write_bytes(original[: len(original) * preserved_percent // 100])
    duration = media._probe_video(ffprobe, path)
    assert duration == pytest.approx(2)
    strict = media._decode_video(ffmpeg, path, duration, strict=True)
    normal = media._decode_video(ffmpeg, path, duration, strict=False)
    assert strict["status"] == "decode_error"
    assert strict["diagnosticCode"] in {"invalid_media_data", "h264_corrupt_frame"}
    assert normal["status"] != "completed"
    assert normal["lastDecodedSec"] < duration - 0.05


def test_truncated_tail_reaching_end_timestamp_does_not_claim_all_frames_preserved(
    synthetic_mp4,
) -> None:
    from boxer_company.assistant.recording_media_diagnosis_route import _format_evidence

    # 마지막 PTS가 파일 길이에 도달해도 실제 손실된 프레임을 복원했다고 말하면 안 된다.
    path, ffmpeg, ffprobe = synthetic_mp4
    original = path.read_bytes()
    path.write_bytes(original[: len(original) * 95 // 100])
    duration = media._probe_video(ffprobe, path)
    strict = media._decode_video(ffmpeg, path, duration, strict=True)
    normal = media._decode_video(ffmpeg, path, duration, strict=False)
    assert strict["status"] == "decode_error"
    assert normal["status"] == "completed"
    assert normal["diagnosticCode"] in {"invalid_media_data", "h264_corrupt_frame"}
    assert normal["decodedFrames"] < 20
    rendered = _format_evidence(
        {
            "strictDecode": strict,
            "normalDecode": normal,
            "durationSec": duration,
        }
    )
    assert "영상 끝 시각까지" in rendered
    assert "일반 디코딩은 끝까지 완료" not in rendered
