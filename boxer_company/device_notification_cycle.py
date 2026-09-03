from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import re
from typing import Any, Callable, Mapping, Sequence
from zoneinfo import ZoneInfo

from boxer.core import settings as core_settings
from boxer.retrieval.connectors.db import _create_db_connection
from boxer.retrieval.connectors.s3 import _build_s3_client
from boxer_company import settings as cs
from boxer_company.assistant.device_health_alert_action_route import (
    DeviceHealthAlertActionTarget,
    _build_device_health_alert_sms_guide,
    _is_mobile_phone_number,
    _normalize_phone_number,
    _send_device_health_alert_sms,
)
from boxer_company.automation import (
    AutomationCycleContractError,
    AutomationCycleName,
    AutomationCycleRequest,
    AutomationCycleResult,
    AutomationDelivery,
)
from boxer_company.device_health_sheet import (
    _append_device_health_sheet_alerts,
)
from boxer_company.sms_delivery import (
    _SMS_DELIVERY_ACCEPTED,
    _SMS_DELIVERY_CONFIRM_REQUIRED,
    _SMS_DELIVERY_DELIVERED,
    _SMS_DELIVERY_FAILED,
    _SMS_DELIVERY_NOT_SENT,
    _SMS_DELIVERY_REQUEST_FAILED,
)
from boxer_company.sms_delivery_cycle import (
    acquire_automatic_sms_runtime_claim,
    build_automatic_sms_runtime_claim_key,
    claim_automatic_sms_delivery,
    hold_automatic_sms_delivery_claim,
    publish_automatic_sms_runtime_claim_result,
    remember_sms_delivery_sheet_record,
    wait_for_automatic_sms_runtime_claim,
)


_CAPTUREBOARD_CONNECTION_ERROR = "captureboard_connection_error"
_RECORDING_CRITICALLY_STALLED = "recording_critically_stalled"
_SEGMENTED_RECORDINGS_MERGE_ERROR = "segmented_recordings_merge_error"
_VIDEO_DURATION_MISMATCH = "video_duration_mismatch"
_SUPPORTED_CODES = (
    _CAPTUREBOARD_CONNECTION_ERROR,
    _RECORDING_CRITICALLY_STALLED,
    _SEGMENTED_RECORDINGS_MERGE_ERROR,
    _VIDEO_DURATION_MISMATCH,
)
_CAPTUREBOARD_INCIDENT_CODES = {
    _CAPTUREBOARD_CONNECTION_ERROR,
    _RECORDING_CRITICALLY_STALLED,
}
_KST = ZoneInfo("Asia/Seoul")
_RECORDING_STALL_MIN_DURATION_SECONDS = 120
_RECORDING_STALL_MAX_EVENT_GAP_SECONDS = 300
_DEVICE_NOTIFICATION_BATCH_SIZE = 200
_VIDEO_VERIFICATION_QUEUE_LIMIT = 500
_VIDEO_VERIFICATION_RETRY_SECONDS = 300
_VIDEO_UPLOAD_NORMALIZED = "normalized"
_VIDEO_UPLOAD_UNAVAILABLE = "unavailable"
_VIDEO_UPLOAD_UNKNOWN = "unknown"
_VIDEO_UPLOAD_MISSING_RECORDING = "missing_recording"
_VIDEO_UPLOAD_MISSING_OBJECT = "missing_object"
_VIDEO_UPLOAD_UNDERSIZED_OBJECT = "undersized_object"
_VIDEO_UPLOAD_PROVIDER_ERROR = "provider_error"
_S3_ARCHIVE_STORAGE_CLASSES = frozenset({"GLACIER", "DEEP_ARCHIVE"})
_AUTO_SMS_DEDUPE_WINDOW_SECONDS = 60
_AUTO_SMS_ACCEPTED_TEXT = "문자 발송 접수"
_AUTO_SMS_CONFIRM_REQUIRED_TEXT = "문자 발송 여부 확인 필요"
_AUTO_SMS_FAILED_TEXT = "문자 자동발송 실패 - 수동 발송 가능"
_AUTO_SMS_DUPLICATE_TEXT = (
    "동일 장애 문자 중복 발송 생략 - 기존 알림에서 발송 여부 확인 필요"
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
_VOICE_TYPES = frozenset({"n", "s", "ln", "ls"})
_SAFE_PROVIDER_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$")


def _load_latest_device_notification_id() -> int:
    """첫 실행은 과거 알림을 재생하지 않도록 현재 DB 상한만 읽는다."""

    connection = _create_db_connection(core_settings.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COALESCE(MAX(id), 0) AS latestId "
                "FROM device_notification"
            )
            row = cursor.fetchone() or {}
    finally:
        connection.close()
    return max(0, _coerce_int(row.get("latestId")))


def _load_next_device_notification(
    last_seen_id: int,
) -> tuple[int, dict[str, Any] | None]:
    """고정한 상한 안에서 지원 이벤트 한 건만 읽어 ack 순서를 보존한다."""

    normalized_last_seen_id = max(0, _coerce_int(last_seen_id))
    connection = _create_db_connection(core_settings.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COALESCE(MAX(id), 0) AS latestId "
                "FROM device_notification"
            )
            latest_row = cursor.fetchone() or {}
            latest_id = max(0, _coerce_int(latest_row.get("latestId")))
            if latest_id <= normalized_last_seen_id:
                return normalized_last_seen_id, None

            # Slack 발송 성공 receipt를 받기 전에 다음 이벤트를 처리하지 않도록
            # 한 cycle은 정확히 한 이벤트만 delivery로 만든다.
            cursor.execute(
                "SELECT "
                "n.id AS notificationId, "
                "n.deviceSeq AS deviceSeq, "
                "n.deviceName AS deviceName, "
                "n.code AS code, "
                "n.message AS message, "
                "n.barcode AS barcode, "
                "n.fileId AS fileId, "
                "n.details AS details, "
                "n.occurredAt AS occurredAt, "
                "d.hospitalSeq AS hospitalSeq, "
                "d.hospitalRoomSeq AS hospitalRoomSeq, "
                "d.version AS deviceVersion, "
                "h.hospitalName AS hospitalName, "
                "h.telephone AS hospitalTelephone, "
                "h.deviceAlertPhone AS hospitalDeviceAlertPhone, "
                "hr.roomName AS roomName "
                "FROM device_notification n "
                "LEFT JOIN devices d ON n.deviceSeq = d.seq "
                "LEFT JOIN hospitals h ON d.hospitalSeq = h.seq "
                "LEFT JOIN hospital_rooms hr ON d.hospitalRoomSeq = hr.seq "
                "WHERE n.id > %s "
                "AND n.id <= %s "
                "AND n.code IN (%s, %s, %s, %s) "
                "ORDER BY n.id ASC "
                "LIMIT 1",
                (
                    normalized_last_seen_id,
                    latest_id,
                    _CAPTUREBOARD_CONNECTION_ERROR,
                    _RECORDING_CRITICALLY_STALLED,
                    _SEGMENTED_RECORDINGS_MERGE_ERROR,
                    _VIDEO_DURATION_MISMATCH,
                ),
            )
            row = cursor.fetchone() or None
    finally:
        connection.close()

    event = _normalize_event(row)
    if event is None:
        # 지원하지 않는 중간 이벤트는 SQL에서 걸러지므로 다음 지원 row가 없을
        # 때만 조회 시점 상한까지 안전하게 커서를 전진한다.
        return latest_id, None
    return int(event["notificationId"]), event


def _load_device_notification_batch(
    last_seen_id: int,
    *,
    batch_size: int = _DEVICE_NOTIFICATION_BATCH_SIZE,
) -> tuple[int, list[dict[str, Any]]]:
    """legacy reporter와 같이 한 poll의 DB 상한과 최대 200건을 고정한다."""

    normalized_last_seen_id = max(0, _coerce_int(last_seen_id))
    normalized_batch_size = max(1, min(500, _coerce_int(batch_size)))
    connection = _create_db_connection(core_settings.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COALESCE(MAX(id), 0) AS latestId "
                "FROM device_notification"
            )
            latest_row = cursor.fetchone() or {}
            latest_id = max(0, _coerce_int(latest_row.get("latestId")))
            if latest_id <= normalized_last_seen_id:
                return normalized_last_seen_id, []
            cursor.execute(
                "SELECT "
                "n.id AS notificationId, "
                "n.deviceSeq AS deviceSeq, "
                "n.deviceName AS deviceName, "
                "n.code AS code, "
                "n.message AS message, "
                "n.barcode AS barcode, "
                "n.fileId AS fileId, "
                "n.details AS details, "
                "n.occurredAt AS occurredAt, "
                "d.hospitalSeq AS hospitalSeq, "
                "d.hospitalRoomSeq AS hospitalRoomSeq, "
                "d.version AS deviceVersion, "
                "h.hospitalName AS hospitalName, "
                "h.telephone AS hospitalTelephone, "
                "h.deviceAlertPhone AS hospitalDeviceAlertPhone, "
                "hr.roomName AS roomName "
                "FROM device_notification n "
                "LEFT JOIN devices d ON n.deviceSeq = d.seq "
                "LEFT JOIN hospitals h ON d.hospitalSeq = h.seq "
                "LEFT JOIN hospital_rooms hr ON d.hospitalRoomSeq = hr.seq "
                "WHERE n.id > %s "
                "AND n.id <= %s "
                "AND n.code IN (%s, %s, %s, %s) "
                "ORDER BY n.id ASC "
                "LIMIT %s",
                (
                    normalized_last_seen_id,
                    latest_id,
                    _CAPTUREBOARD_CONNECTION_ERROR,
                    _RECORDING_CRITICALLY_STALLED,
                    _SEGMENTED_RECORDINGS_MERGE_ERROR,
                    _VIDEO_DURATION_MISMATCH,
                    normalized_batch_size,
                ),
            )
            rows = cursor.fetchall() or []
    finally:
        connection.close()

    events = [
        event
        for row in rows
        if (event := _normalize_event(row)) is not None
    ]
    if len(rows) >= normalized_batch_size and events:
        return int(events[-1]["notificationId"]), events
    return latest_id, events


@dataclass(frozen=True, slots=True)
class VideoDurationMismatchVerification:
    """중앙 업로드 정상화 판정과 지연 알림에 필요한 안전한 이벤트다."""

    status: str
    reason: str
    event: Mapping[str, Any]


def _verify_video_duration_mismatch(
    notification_id: int,
) -> VideoDurationMismatchVerification:
    """단말 ffprobe 경고 뒤 같은 파일의 중앙 업로드 결과만 확인한다.

    mismatch 이벤트는 단말에서 MP4 duration을 읽은 뒤에만 생성된다. 따라서
    Boxer는 큰 영상을 다시 내려받지 않고, uploader가 성공 뒤 생성한 DB row와
    exact S3 객체를 함께 확인해 중앙 업로드가 정상화됐는지만 판정한다.
    """

    normalized_notification_id = max(0, _coerce_int(notification_id))
    if normalized_notification_id <= 0:
        raise ValueError("video mismatch notification id is invalid")

    connection = _create_db_connection(core_settings.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            # fileId는 JOIN 안에서만 사용하고 SELECT 결과나 durable cursor에는
            # 싣지 않는다. 삭제된 장비의 nullable FK 대신 globally unique fileId를
            # 우선하고, 충돌·장비 불일치는 아래에서 unknown으로 막는다.
            cursor.execute(
                "SELECT "
                "n.id AS notificationId, "
                "n.deviceSeq AS deviceSeq, "
                "n.deviceName AS deviceName, "
                "n.code AS code, "
                "n.message AS message, "
                "n.details AS details, "
                "n.barcode AS barcode, "
                "n.occurredAt AS occurredAt, "
                "d.hospitalSeq AS hospitalSeq, "
                "d.hospitalRoomSeq AS hospitalRoomSeq, "
                "d.version AS deviceVersion, "
                "h.hospitalName AS hospitalName, "
                "h.telephone AS hospitalTelephone, "
                "h.deviceAlertPhone AS hospitalDeviceAlertPhone, "
                "hr.roomName AS roomName, "
                "CASE WHEN n.fileId IS NULL OR TRIM(n.fileId) = '' "
                "THEN 0 ELSE 1 END AS hasFileId, "
                "r.seq AS recordingSeq, "
                "r.deviceSeq AS recordingDeviceSeq, "
                "r.recordedAt AS recordingRecordedAt, "
                "r.streamingStatus AS streamingStatus, "
                "r.s3Bucket AS s3Bucket, "
                "r.s3FileKey AS s3FileKey "
                "FROM device_notification n "
                "LEFT JOIN devices d ON n.deviceSeq = d.seq "
                "LEFT JOIN hospitals h ON d.hospitalSeq = h.seq "
                "LEFT JOIN hospital_rooms hr ON d.hospitalRoomSeq = hr.seq "
                "LEFT JOIN recordings r "
                "ON r.fileId = n.fileId "
                "WHERE n.id = %s AND n.code = %s "
                "ORDER BY r.seq DESC "
                "LIMIT 2",
                (
                    normalized_notification_id,
                    _VIDEO_DURATION_MISMATCH,
                ),
            )
            rows = cursor.fetchall() or []
    finally:
        connection.close()

    if not rows:
        # 원본 notification 자체가 사라진 경우에는 대상 식별을 확정할 수 없다.
        raise RuntimeError("video mismatch notification is unavailable")
    event = _normalize_event(rows[0])
    if event is None:
        raise RuntimeError("video mismatch notification is invalid")
    event = _enrich_video_verification_event(rows[0], event)
    if len(rows) != 1:
        return VideoDurationMismatchVerification(
            status=_VIDEO_UPLOAD_UNKNOWN,
            reason="ambiguous_recording",
            event=event,
        )

    row = rows[0]
    if not bool(_coerce_int(row.get("hasFileId"))):
        return VideoDurationMismatchVerification(
            status=_VIDEO_UPLOAD_UNKNOWN,
            reason="notification_file_id_missing",
            event=event,
        )
    if _coerce_optional_int(row.get("recordingSeq")) is None:
        return VideoDurationMismatchVerification(
            status=_VIDEO_UPLOAD_UNAVAILABLE,
            reason=_VIDEO_UPLOAD_MISSING_RECORDING,
            event=event,
        )
    notification_device_seq = _coerce_optional_int(row.get("deviceSeq"))
    recording_device_seq = _coerce_optional_int(row.get("recordingDeviceSeq"))
    if (
        notification_device_seq is not None
        and recording_device_seq is not None
        and notification_device_seq != recording_device_seq
    ):
        return VideoDurationMismatchVerification(
            status=_VIDEO_UPLOAD_UNKNOWN,
            reason="recording_device_mismatch",
            event=event,
        )

    # AVAILABLE 이외 상태와 잘못된 위치 정보는 일시적 상태일 수 있으므로
    # 실제 누락으로 단정하지 않고 다음 poll에서 다시 확인한다.
    if str(row.get("streamingStatus") or "").strip().upper() != "AVAILABLE":
        return VideoDurationMismatchVerification(
            status=_VIDEO_UPLOAD_UNKNOWN,
            reason="streaming_not_available",
            event=event,
        )
    expected_bucket = str(cs.S3_ULTRASOUND_BUCKET or "").strip()
    bucket = str(row.get("s3Bucket") or "").strip()
    key = str(row.get("s3FileKey") or "").strip()
    owner_id = str(cs.S3_ULTRASOUND_BUCKET_OWNER_ID or "").strip()
    if (
        not expected_bucket
        or bucket != expected_bucket
        or not key.lower().endswith(".mp4")
        or not re.fullmatch(r"[0-9]{12}", owner_id)
    ):
        return VideoDurationMismatchVerification(
            status=_VIDEO_UPLOAD_UNKNOWN,
            reason="invalid_s3_location",
            event=event,
        )

    s3_client = _build_s3_client()
    try:
        # DB에서 확인한 exact key만 HEAD하고 bucket owner까지 고정한다. 영상
        # download/restore/list 같은 확대 권한은 이 판정 경로에서 사용하지 않는다.
        head = s3_client.head_object(
            Bucket=bucket,
            Key=key,
            ExpectedBucketOwner=owner_id,
        )
    except Exception as exc:
        error_code = _aws_error_code(exc)
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return VideoDurationMismatchVerification(
                status=_VIDEO_UPLOAD_UNAVAILABLE,
                reason=_VIDEO_UPLOAD_MISSING_OBJECT,
                event=event,
            )
        # 403은 없는 key와 권한/owner 오류를 구분할 수 없다. 재생 가능한
        # 영상을 누락으로 오판하지 않도록 provider unknown으로 재시도한다.
        raise

    storage_class = str(head.get("StorageClass") or "STANDARD").strip().upper()
    archive_status = str(head.get("ArchiveStatus") or "").strip().upper()
    restore = str(head.get("Restore") or "").strip().lower()
    content_length = max(0, _coerce_int(head.get("ContentLength")))
    if content_length < max(
        1,
        int(cs.DEVICE_NOTIFICATION_VIDEO_MIN_OBJECT_BYTES),
    ):
        # 이 값은 재생 시간이 아니라 MommyBox uploader가 원본 전송 전에
        # 적용하는 파일 크기 invariant다. 짧아도 온전한 영상은 그대로 허용한다.
        return VideoDurationMismatchVerification(
            status=_VIDEO_UPLOAD_UNAVAILABLE,
            reason=_VIDEO_UPLOAD_UNDERSIZED_OBJECT,
            event=event,
        )
    if (
        storage_class in _S3_ARCHIVE_STORAGE_CLASSES
        or bool(archive_status)
        or 'ongoing-request="true"' in restore
    ):
        return VideoDurationMismatchVerification(
            status=_VIDEO_UPLOAD_UNKNOWN,
            reason="object_not_ready",
            event=event,
        )
    return VideoDurationMismatchVerification(
        status=_VIDEO_UPLOAD_NORMALIZED,
        reason="central_object_available",
        event=event,
    )


@dataclass(frozen=True, slots=True)
class DeviceNotificationCycleDeps:
    """DB, Sheets, SMS mutation을 주입해 cycle 계약을 단위 검증한다."""

    load_latest_id: Callable[[], int] = _load_latest_device_notification_id
    load_next_event: Callable[
        [int], tuple[int, dict[str, Any] | None]
    ] = _load_next_device_notification
    load_event_batch: Callable[..., tuple[int, list[dict[str, Any]]]] = (
        _load_device_notification_batch
    )
    verify_video_duration_mismatch: Callable[
        [int], VideoDurationMismatchVerification
    ] = _verify_video_duration_mismatch
    append_sheet_alerts: Callable[..., int | None] = (
        _append_device_health_sheet_alerts
    )
    send_sms: Callable[..., dict[str, Any]] = _send_device_health_alert_sms
    claim_sms_delivery: Callable[..., bool] = claim_automatic_sms_delivery
    hold_sms_delivery_claim: Callable[..., bool] = (
        hold_automatic_sms_delivery_claim
    )
    clock: Callable[[], datetime] = _utc_now
    remember_sms_delivery: Callable[..., bool] = (
        remember_sms_delivery_sheet_record
    )


class DeviceNotificationAlertCycleHandler:
    """실시간 장비 이벤트의 조회·중복방지·mutation을 채널 밖에서 실행한다."""

    name: AutomationCycleName = "device_notification_alert"

    def __init__(
        self,
        deps: DeviceNotificationCycleDeps | None = None,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._deps = deps or DeviceNotificationCycleDeps()
        self._logger = logger or logging.getLogger(__name__)

    def validate(self, request: AutomationCycleRequest) -> None:
        if request.options:
            raise AutomationCycleContractError(
                "device notification cycle does not accept options"
            )

    def run(self, request: AutomationCycleRequest) -> AutomationCycleResult:
        self.validate(request)
        state = _normalize_cursor(request.cursor, now=request.scheduled_at)
        if state["pendingDeliveryContexts"]:
            # Durable coordinator가 pending delivery를 먼저 돌려줘야 하므로 직접
            # handler를 잘못 호출해도 DB cursor를 앞당기지 않는다.
            raise AutomationCycleContractError(
                "device notification delivery receipt is still pending"
            )
        self._flush_pending_sheet_repairs(state)

        if not state["initialized"]:
            latest_id = max(0, int(self._deps.load_latest_id()))
            state.update(
                {
                    "initialized": True,
                    "initializedAt": request.scheduled_at.isoformat(),
                    "lastSeenId": latest_id,
                    "lastPolledAt": request.scheduled_at.isoformat(),
                }
            )
            return _cycle_result(state, deliveries=(), processed_count=0)

        processed_count = 0
        if not state["pendingEvents"]:
            if self._deps.load_next_event is _load_next_device_notification:
                # production은 legacy처럼 한 번 읽은 최대 200건을 cursor queue에
                # 보존하고, Slack receipt마다 일반 알림 한 건씩 확정한다.
                next_cursor, events = self._deps.load_event_batch(
                    int(state["lastSeenId"]),
                    batch_size=_DEVICE_NOTIFICATION_BATCH_SIZE,
                )
                state["lastSeenId"] = max(
                    int(state["lastSeenId"]),
                    int(next_cursor),
                )
                state["pendingEvents"] = list(events)
                state["lastPolledAt"] = request.scheduled_at.isoformat()
            else:
                # 주입형 단위 테스트와 이전 custom port도 조회한 한 건을 먼저
                # queue로 옮겨 유예·재시작 중 cursor가 앞서가며 잃지 않게 한다.
                next_cursor, raw_event = self._deps.load_next_event(
                    int(state["lastSeenId"])
                )
                state["lastSeenId"] = max(
                    int(state["lastSeenId"]),
                    int(next_cursor),
                )
                normalized_event = _normalize_event(raw_event)
                state["pendingEvents"] = (
                    [normalized_event] if normalized_event is not None else []
                )
                state["lastPolledAt"] = request.scheduled_at.isoformat()

        # 길이 mismatch는 일반 delivery queue에서 즉시 분리한다. 지연 검증이
        # 뒤의 캡처보드·녹화 정체 알림과 다음 DB batch를 막지 않게 하기 위함이다.
        if cs.DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_ENABLED:
            processed_count += _defer_video_duration_mismatch_events(
                state,
                now=request.scheduled_at,
                logger=self._logger,
            )
            # 기존 캡처보드·녹화 장애를 영상 검증 backlog보다 우선한다.
            # 일반 queue가 비었을 때만 due mismatch 한 건을 확인한다.
            verification = (
                None
                if state["pendingEvents"]
                else _next_due_video_verification(
                    state,
                    now=request.scheduled_at,
                )
            )
            if verification is not None:
                notification_id = int(verification["notificationId"])
                try:
                    verified = self._deps.verify_video_duration_mismatch(
                        notification_id
                    )
                except Exception as exc:
                    # DB/S3 권한·timeout을 파일 누락으로 바꾸지 않는다. 안전한
                    # retry marker만 갱신하고 같은 poll에서 일반 알림은 계속 처리한다.
                    self._logger.warning(
                        "Video duration mismatch verification deferred "
                        "notification_id=%s error_type=%s",
                        notification_id,
                        type(exc).__name__,
                    )
                    _reschedule_video_verification(
                        state,
                        notification_id,
                        now=request.scheduled_at,
                        reason=_VIDEO_UPLOAD_PROVIDER_ERROR,
                    )
                else:
                    if verified.status == _VIDEO_UPLOAD_NORMALIZED:
                        _remove_video_verification(state, notification_id)
                        processed_count += 1
                    elif verified.status == _VIDEO_UPLOAD_UNAVAILABLE:
                        alert_event = {
                            **dict(verified.event),
                            "videoAvailabilityReason": verified.reason,
                        }
                        delivery, context = self._build_delivery(
                            request,
                            state,
                            alert_event,
                        )
                        if delivery is None or context is None:
                            raise AutomationCycleContractError(
                                "video mismatch alert could not be built"
                            )
                        # deferred entry와 pending delivery가 동시에 같은 이벤트를
                        # 소유하지 않게 원자적으로 owner를 넘겨 recovery 재알림을 막는다.
                        _remove_video_verification(state, notification_id)
                        state["pendingDeliveryContexts"][
                            delivery.delivery_id
                        ] = context
                        return _cycle_result(
                            state,
                            deliveries=(delivery,),
                            processed_count=processed_count + 1,
                        )
                    else:
                        _reschedule_video_verification(
                            state,
                            notification_id,
                            now=request.scheduled_at,
                            reason=verified.reason,
                        )
        else:
            # rollout off 동안 새 mismatch만 관측 marker와 함께 건너뛴다.
            # 이미 검증 queue가 소유한 항목은 pause해 재활성화 때 이어간다.
            processed_count += _discard_disabled_video_duration_mismatches(
                state,
                now=request.scheduled_at,
            )

        raw_event = next(
            (
                item
                for item in state["pendingEvents"]
                if not (
                    cs.DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_ENABLED
                    and isinstance(item, Mapping)
                    and item.get("code") == _VIDEO_DURATION_MISMATCH
                )
            ),
            None,
        )
        event = _normalize_event(raw_event)
        if event is None:
            return _cycle_result(
                state,
                deliveries=(),
                processed_count=processed_count,
            )

        if _should_suppress_open_captureboard_event(
            state,
            event,
            now=request.scheduled_at,
        ):
            _mark_suppressed_captureboard_event(
                state,
                event,
                now=request.scheduled_at,
            )
            _consume_pending_event(state, event["notificationId"])
            return _cycle_result(
                state,
                deliveries=(),
                processed_count=processed_count + 1,
            )

        delivery, context = self._build_delivery(
            request,
            state,
            event,
        )
        if delivery is None or context is None:
            _consume_pending_event(state, event["notificationId"])
            return _cycle_result(
                state,
                deliveries=(),
                processed_count=processed_count + 1,
            )
        state["pendingDeliveryContexts"][delivery.delivery_id] = context
        return _cycle_result(
            state,
            deliveries=(delivery,),
            processed_count=processed_count + 1,
        )

    def acknowledge(
        self,
        request: AutomationCycleRequest,
        receipts: Sequence[Any],
    ) -> Mapping[str, Any]:
        """Slack 성공 receipt 뒤 Sheets와 thread incident 상태를 확정한다."""

        state = _normalize_cursor(request.cursor, now=request.scheduled_at)
        contexts = state["pendingDeliveryContexts"]
        for receipt in receipts:
            if str(_receipt_value(receipt, "status") or "") != "sent":
                continue
            delivery_id = str(
                _receipt_value(receipt, "delivery_id", "deliveryId") or ""
            ).strip()
            context = contexts.get(delivery_id)
            if not isinstance(context, dict):
                continue

            external_message_id = str(
                _receipt_value(
                    receipt,
                    "external_message_id",
                    "externalMessageId",
                )
                or ""
            ).strip()
            permalink = str(_receipt_value(receipt, "permalink") or "").strip()
            self._acknowledge_context(
                state,
                context,
                delivery_id=delivery_id,
                external_message_id=external_message_id,
                permalink=permalink,
                acknowledged_at=(
                    _receipt_value(receipt, "delivered_at", "deliveredAt")
                    or request.scheduled_at
                ),
            )
            _consume_pending_event(
                state,
                _coerce_int(context.get("notificationId")),
            )
            contexts.pop(delivery_id, None)
        state["pendingDeliveryContexts"] = contexts
        return state

    def _flush_pending_sheet_repairs(self, state: dict[str, Any]) -> None:
        """Sheet 실패를 provider outbox 또는 저장된 direct context로 복구한다."""

        pending_repairs = state["pendingSheetRepairs"]
        for delivery_id, repair in tuple(pending_repairs.items()):
            item = repair.get("item")
            detected_at = repair.get("detectedAt")
            if not isinstance(item, dict) or not detected_at:
                continue
            group_id = str(item.get("smsGroupId") or "").strip()
            if group_id:
                try:
                    # provider group이 있는 항목은 기존 durable outbox가
                    # 최종 delivery 확인과 Sheet reconciliation을 소유한다.
                    remembered = self._deps.remember_sms_delivery(
                        dict(item),
                        detected_at=detected_at,
                        sms_accepted_at=item.get("smsAcceptedAt")
                        or detected_at,
                        permalink=repair.get("permalink") or "",
                    )
                except Exception as exc:
                    self._logger.warning(
                        "Device notification Sheet repair queue failed "
                        "error_type=%s",
                        type(exc).__name__,
                    )
                    continue
                if remembered:
                    pending_repairs.pop(delivery_id, None)
                    state["lastSheetWriteStatus"] = "repair_queued"
                continue

            if str(repair.get("status") or "").strip() != "sheet_pending":
                continue
            parsed_detected_at = _parse_event_datetime(detected_at)
            if parsed_detected_at is None:
                continue
            try:
                # group 없는 수동/실패 SMS 결과는 outbox가 받을 수 없으므로
                # cursor의 non-PII allowlist context로 Sheet만 재시도한다. map
                # key를 정본 delivery ID로 덮어써 오래된 cursor도 안전하게 잇는다.
                repair_item = {**dict(item), "sheetDeliveryId": delivery_id}
                row_count = self._deps.append_sheet_alerts(
                    [repair_item],
                    detected_at=parsed_detected_at,
                    slack_permalink=str(repair.get("permalink") or ""),
                )
            except Exception as exc:
                self._logger.warning(
                    "Device notification direct Sheet repair failed "
                    "error_type=%s",
                    type(exc).__name__,
                )
                continue
            pending_repairs.pop(delivery_id, None)
            state["lastSheetWriteStatus"] = (
                "disabled" if row_count is None else "repair_completed"
            )

    def _build_delivery(
        self,
        request: AutomationCycleRequest,
        state: dict[str, Any],
        event: dict[str, Any],
    ) -> tuple[AutomationDelivery | None, dict[str, Any] | None]:
        if event["code"] == _RECORDING_CRITICALLY_STALLED:
            continuation = _build_recording_stall_continuation(
                state,
                event,
            )
            if continuation is not None:
                delivery_id = f"device_notification:{event['notificationId']}"
                delivery = AutomationDelivery(
                    delivery_id=delivery_id,
                    kind="device_notification_thread_reply",
                    payload={
                        "notificationId": event["notificationId"],
                        "code": event["code"],
                        "replyToExternalMessageId": continuation[
                            "replyToExternalMessageId"
                        ],
                        "durationText": continuation["durationText"],
                        "growthRateText": continuation["growthRateText"],
                        "occurredAt": _format_occurred_at(
                            event.get("occurredAt")
                        ),
                    },
                )
                return delivery, {
                    "kind": "thread_reply",
                    "notificationId": event["notificationId"],
                    "recordingIncidentKey": continuation["incidentKey"],
                    "recordingIncident": continuation["nextIncident"],
                }
            if not _is_recording_stall_alert(event):
                return None, None

        alert_summary, alert_item, recording_context = _build_root_alert(event)
        if event["code"] == _VIDEO_DURATION_MISMATCH:
            # 중앙 영상 누락은 CX가 확인할 운영 알림이다. 병원 문자나 장비 음성
            # 안내로 연결하면 현장 조치가 불필요한 장애를 병원에 전파하게 된다.
            sms_receipt = {
                "attempted": False,
                "status": "not_applicable",
                "ok": False,
                "contactActionEnabled": False,
                "deliveryStatus": _SMS_DELIVERY_NOT_SENT,
            }
        else:
            alert_summary, alert_item, sms_receipt = self._apply_auto_sms(
                request,
                state,
                event,
                alert_summary,
                alert_item,
            )
        delivery_id = f"device_notification:{event['notificationId']}"
        delivery = AutomationDelivery(
            delivery_id=delivery_id,
            kind="device_notification_alert",
            payload={
                "notificationId": event["notificationId"],
                "code": event["code"],
                "occurredAt": _format_occurred_at(event.get("occurredAt")),
                "alertSummary": alert_summary,
                # Slack은 이 힌트만 보고 기존 Block renderer와 action 조립을
                # 실행하며 DB/SMS/Sheets 판단은 다시 하지 않는다.
                "render": {
                    "type": "device_health_abnormal_alert",
                    "includeActions": event["code"]
                    != _VIDEO_DURATION_MISMATCH,
                    "includeDeviceVoiceAction": event["code"]
                    != _VIDEO_DURATION_MISMATCH,
                },
                # alertSummary에는 legacy 자동발송 확인 action의 번호·본문을
                # 유지하고, 별도 receipt에는 provider 식별값을 싣지 않는다.
                "smsReceipt": _public_sms_receipt(sms_receipt),
            },
        )
        context: dict[str, Any] = {
            "kind": "root_alert",
            "notificationId": event["notificationId"],
            "code": event["code"],
            "deviceSeq": event.get("deviceSeq"),
            "deviceName": event["deviceName"],
            "occurredAt": event["occurredAt"],
            "sheetAlertItem": alert_item,
        }
        if recording_context is not None:
            context["recordingIncidentKey"] = recording_context["incidentKey"]
            context["recordingIncident"] = recording_context["incident"]
        return delivery, context

    def _apply_auto_sms(
        self,
        request: AutomationCycleRequest,
        state: dict[str, Any],
        event: Mapping[str, Any],
        alert_summary: dict[str, Any],
        alert_item: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        target = DeviceHealthAlertActionTarget(
            hospital_seq=max(0, _coerce_int(event.get("hospitalSeq"))),
            hospital_name=str(event.get("hospitalName") or "병원 미확인"),
            room_name=str(event.get("roomName") or "병실 미확인"),
            device_name=str(event.get("deviceName") or ""),
            issue=str(alert_item.get("issue") or "상세 확인 필요"),
            alert_category=str(alert_item.get("alertCategory") or ""),
            problem_components=tuple(alert_item.get("problemComponents") or ()),
        )
        guide = _build_device_health_alert_sms_guide(target)
        phone_number = _normalize_phone_number(
            event.get("hospitalDeviceAlertPhone")
        )
        if not _is_mobile_phone_number(phone_number):
            receipt = _manual_sms_receipt(
                status="manual_required",
                template_id=str(guide.get("templateId") or ""),
            )
            return _apply_sms_receipt(alert_summary, alert_item, receipt)
        message = str(guide.get("message") or "").strip()
        if not guide.get("supported") or not message:
            receipt = _manual_sms_receipt(
                status="unsupported_issue",
                template_id=str(guide.get("templateId") or ""),
            )
            return _apply_sms_receipt(alert_summary, alert_item, receipt)

        uses_runtime_claim = (
            self._deps.claim_sms_delivery
            is claim_automatic_sms_delivery
        )
        runtime_claim: dict[str, Any] = {}
        if uses_runtime_claim:
            # health와 notification은 legacy Slack에서 같은 sender의
            # process-memory claim을 공유했다. API에서도 같은 60초 key를 쓴다.
            runtime_claim_key = build_automatic_sms_runtime_claim_key(
                alert_item
            )
            claimed, runtime_claim = acquire_automatic_sms_runtime_claim(
                runtime_claim_key
            )
            if not claimed:
                wait_for_automatic_sms_runtime_claim(
                    runtime_claim_key,
                    runtime_claim,
                    logger=self._logger,
                )
                receipt = {
                    "attempted": False,
                    "status": "duplicate_suppressed",
                    "ok": False,
                    "statusText": _AUTO_SMS_DUPLICATE_TEXT,
                    "contactActionEnabled": False,
                    "deliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
                    "templateId": str(guide.get("templateId") or ""),
                    "deduplicated": True,
                }
                return _apply_sms_receipt(
                    alert_summary,
                    alert_item,
                    receipt,
                )
        else:
            # 주입된 port는 기존 recovery/단위 테스트 호환 경로로 유지한다.
            claim_key = _auto_sms_claim_key(alert_item)
            claims = state["autoSmsClaims"]
            if claim_key and claim_key in claims:
                receipt = {
                    "attempted": False,
                    "status": "duplicate_suppressed",
                    "ok": False,
                    "statusText": _AUTO_SMS_DUPLICATE_TEXT,
                    "contactActionEnabled": False,
                    "deliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
                    "templateId": str(guide.get("templateId") or ""),
                    "deduplicated": True,
                }
                return _apply_sms_receipt(
                    alert_summary,
                    alert_item,
                    receipt,
                )
            claim_now = self._deps.clock()
            try:
                claimed = self._deps.claim_sms_delivery(
                    target.device_name,
                    target.alert_category,
                    claimed_at=claim_now,
                )
            except Exception as exc:
                self._logger.warning(
                    "Device notification automatic SMS claim failed "
                    "notification_id=%s error_type=%s",
                    event["notificationId"],
                    type(exc).__name__,
                )
                receipt = {
                    "attempted": False,
                    "status": "claim_unavailable",
                    "ok": False,
                    "statusText": _AUTO_SMS_CONFIRM_REQUIRED_TEXT,
                    "contactActionEnabled": False,
                    "deliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
                    "templateId": str(guide.get("templateId") or ""),
                }
                return _apply_sms_receipt(
                    alert_summary,
                    alert_item,
                    receipt,
                )
            if not claimed:
                receipt = {
                    "attempted": False,
                    "status": "duplicate_suppressed",
                    "ok": False,
                    "statusText": _AUTO_SMS_DUPLICATE_TEXT,
                    "contactActionEnabled": False,
                    "deliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
                    "templateId": str(guide.get("templateId") or ""),
                    "deduplicated": True,
                }
                return _apply_sms_receipt(
                    alert_summary,
                    alert_item,
                    receipt,
                )
            if claim_key:
                claims[claim_key] = {
                    "claimedAt": claim_now.isoformat(),
                    "notificationId": event["notificationId"],
                }

        payload = {
            "actionId": "device_health_alert_contact_hospital",
            "requestType": "sms",
            "createdAt": request.scheduled_at.isoformat(),
            "actorUserId": "automation",
            "hospital": {
                "seq": target.hospital_seq,
                "name": target.hospital_name,
                "phoneNumber": phone_number,
            },
            "device": {
                "name": target.device_name,
                "room": target.room_name,
                "issue": target.issue,
            },
            "sms": {
                "to": phone_number,
                "templateId": str(guide.get("templateId") or ""),
                "message": message,
                "testMode": False,
            },
            "origin": {
                "channel": "automation",
                "tenantId": request.tenant_id,
                "requestId": request.request_id,
            },
        }
        try:
            raw_result = self._deps.send_sms(payload, logger=self._logger)
            if not isinstance(raw_result, dict):
                raw_result = {"status": "invalid_result", "ok": False}
        except Exception as exc:
            # 호출이 공급자에 도달했을 수 있으므로 결과 불명은 confirm_required로
            # 고정하고 같은 notification을 자동 재실행하지 않는다.
            self._logger.warning(
                "Device notification automatic SMS result unknown "
                "notification_id=%s error_type=%s",
                event["notificationId"],
                type(exc).__name__,
            )
            raw_result = {
                "status": "error",
                "ok": False,
                "smsDeliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
            }

        receipt = _normalize_sms_receipt(
            raw_result,
            template_id=str(guide.get("templateId") or ""),
            accepted_at=request.scheduled_at,
        )
        receipt.update(
            {
                # legacy 자동발송 상태 버튼이 실제 발송값을 다시 보여준다.
                "phoneNumber": phone_number,
                "message": message,
            }
        )
        next_summary, next_alert_item, next_receipt = _apply_sms_receipt(
            alert_summary,
            alert_item,
            receipt,
        )
        delivery_status = str(receipt.get("deliveryStatus") or "").strip()
        group_id = str(receipt.get("groupId") or "").strip()
        remembered = False
        if delivery_status in {
            _SMS_DELIVERY_ACCEPTED,
            _SMS_DELIVERY_DELIVERED,
            _SMS_DELIVERY_CONFIRM_REQUIRED,
        }:
            if group_id:
                try:
                    # provider mutation 직후 receipt를 API-local outbox에 먼저
                    # fsync해 Slack ack/Sheets 실패와 무관하게 추적을 이어간다.
                    remembered = self._deps.remember_sms_delivery(
                        dict(next_alert_item),
                        detected_at=event.get("occurredAt")
                        or request.scheduled_at,
                        sms_accepted_at=receipt.get("acceptedAt")
                        or request.scheduled_at,
                    )
                except Exception as exc:
                    self._logger.warning(
                        "Device notification SMS receipt persist failed "
                        "notification_id=%s error_type=%s",
                        event["notificationId"],
                        type(exc).__name__,
                    )
            if not remembered and delivery_status != _SMS_DELIVERY_DELIVERED:
                # 공급자가 받았을 수 있으므로 실패 뒤 자동/수동 재발송은 모두
                # 잠그고 conversation에는 추적 ID 없이 결과 불명만 전달한다.
                next_receipt = {
                    **next_receipt,
                    "status": "receipt_persist_failed",
                    "statusText": _AUTO_SMS_CONFIRM_REQUIRED_TEXT,
                    "contactActionEnabled": False,
                    "deliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
                }
                next_summary, next_alert_item, next_receipt = _apply_sms_receipt(
                    alert_summary,
                    alert_item,
                    next_receipt,
                )
        if uses_runtime_claim:
            publish_automatic_sms_runtime_claim_result(
                runtime_claim,
                next_receipt,
            )
        else:
            claim_state = (
                "settled"
                if delivery_status in {
                    _SMS_DELIVERY_REQUEST_FAILED,
                    _SMS_DELIVERY_FAILED,
                }
                else (
                    "accepted"
                    if remembered and group_id
                    else "uncertain"
                )
            )
            try:
                self._deps.hold_sms_delivery_claim(
                    target.device_name,
                    target.alert_category,
                    held_at=self._deps.clock(),
                    state=claim_state,
                    group_id=(
                        group_id if claim_state == "accepted" else None
                    ),
                )
            except Exception as exc:
                self._logger.warning(
                    "Device notification SMS claim hold failed "
                    "notification_id=%s error_type=%s",
                    event["notificationId"],
                    type(exc).__name__,
                )
                next_receipt = {
                    **next_receipt,
                    "status": "claim_hold_failed",
                    "statusText": _AUTO_SMS_CONFIRM_REQUIRED_TEXT,
                    "contactActionEnabled": False,
                    "deliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
                }
                next_summary, next_alert_item, next_receipt = (
                    _apply_sms_receipt(
                        alert_summary,
                        alert_item,
                        next_receipt,
                    )
                )
        return next_summary, next_alert_item, next_receipt

    def _acknowledge_context(
        self,
        state: dict[str, Any],
        context: Mapping[str, Any],
        *,
        delivery_id: str,
        external_message_id: str,
        permalink: str,
        acknowledged_at: Any,
    ) -> None:
        actual_acknowledged_at = _coerce_aware_datetime(
            acknowledged_at,
            fallback=datetime.now(timezone.utc),
        )
        if context.get("kind") == "thread_reply":
            incident_key = str(context.get("recordingIncidentKey") or "")
            incident = context.get("recordingIncident")
            if incident_key and isinstance(incident, dict):
                state["recordingStallIncidents"][incident_key] = dict(incident)
            state["lastSentAt"] = actual_acknowledged_at.isoformat()
            state["lastSentNotificationId"] = _coerce_int(
                context.get("notificationId")
            )
            return

        alert_item = context.get("sheetAlertItem")
        row_count: int | None = None
        outbox_ready = False
        if isinstance(alert_item, dict):
            detected_at = _parse_event_datetime(context.get("occurredAt"))
            if detected_at is None:
                detected_at = actual_acknowledged_at
            has_sms_group = bool(
                str(alert_item.get("smsGroupId") or "").strip()
            )
            # Slack receipt의 stable delivery ID를 Sheet 전용 metadata로 전달해
            # append 반영 후 timeout을 다음 poll에서 안전하게 reconcile한다.
            sheet_alert_item = {
                **dict(alert_item),
                "sheetDeliveryId": delivery_id,
            }
            if has_sms_group:
                try:
                    # 최초 provider receipt에 permalink를 병합해 direct append가
                    # 실패해도 SMS reconciliation cycle이 Sheet 행을 복구한다.
                    outbox_ready = self._deps.remember_sms_delivery(
                        dict(alert_item),
                        detected_at=detected_at,
                        sms_accepted_at=alert_item.get("smsAcceptedAt")
                        or detected_at,
                        permalink=permalink,
                    )
                except Exception as exc:
                    self._logger.warning(
                        "Device notification SMS outbox permalink update failed "
                        "notification_id=%s error_type=%s",
                        context.get("notificationId"),
                        type(exc).__name__,
                    )
            sheet_append_failed = False
            try:
                row_count = self._deps.append_sheet_alerts(
                    [sheet_alert_item],
                    detected_at=detected_at,
                    slack_permalink=permalink,
                )
            except Exception as exc:
                # Slack 성공 context를 그대로 버리지 않고 별도 repair 표식을
                # 남긴다. provider receipt가 있는 경우 실제 payload는 durable
                # SMS outbox가 소유하므로 다음 poll을 막지 않아도 복구 가능하다.
                self._logger.warning(
                    "Device notification Sheet append failed "
                    "notification_id=%s error_type=%s",
                    context.get("notificationId"),
                    type(exc).__name__,
                )
                sheet_append_failed = True
            else:
                # enabled Sheet가 0건을 반환한 경우에도 outbox가 payload를
                # 소유하고 있으면 reconciliation 대상이라는 사실을 남긴다.
                sheet_append_failed = row_count == 0 and outbox_ready
            if sheet_append_failed:
                repair_key = hashlib.sha256(
                    str(alert_item.get("smsGroupId") or delivery_id).encode(
                        "utf-8"
                    )
                ).hexdigest()[:24]
                state["pendingSheetRepairs"][delivery_id] = {
                    "queuedAt": actual_acknowledged_at.isoformat(),
                    "detectedAt": detected_at.isoformat(),
                    "permalink": permalink,
                    # direct Sheet 복구에는 연락처·본문이 필요 없으므로
                    # 별도 allowlist item으로 좁혀 보존한다.
                    "item": _sheet_repair_item(sheet_alert_item),
                    "repairKey": repair_key,
                    "status": (
                        "outbox_pending"
                        if has_sms_group
                        else "sheet_pending"
                    ),
                }
                state["lastSheetWriteStatus"] = "repair_pending"
            else:
                state["pendingSheetRepairs"].pop(delivery_id, None)
                state["lastSheetWriteStatus"] = (
                    "disabled" if row_count is None else "completed"
                )

        code = str(context.get("code") or "")
        device_name = str(context.get("deviceName") or "").strip()
        if code == _CAPTUREBOARD_CONNECTION_ERROR and device_name:
            state["recentCaptureboardAlerts"][device_name] = {
                "lastAlertedAt": actual_acknowledged_at.isoformat(),
                "notificationId": _coerce_int(context.get("notificationId")),
            }
        incident_key = str(context.get("recordingIncidentKey") or "")
        raw_incident = context.get("recordingIncident")
        if incident_key and isinstance(raw_incident, dict):
            incident = dict(raw_incident)
            incident.update(
                {
                    "phase": "alerted",
                    "rootExternalMessageId": external_message_id,
                    "rootPermalink": permalink,
                }
            )
            state["recordingStallIncidents"][incident_key] = incident
        if code in _CAPTUREBOARD_INCIDENT_CODES and device_name:
            # Slack 루트가 실제 전송된 시점부터 API cursor가 incident 정본을
            # 소유한다. Sheet append 결과는 업무 현황 복구에만 쓰고 중복 억제
            # 여부에는 영향을 주지 않는다.
            state["captureboardIncidents"][device_name] = {
                "deviceName": device_name,
                "deviceSeq": _coerce_optional_int(context.get("deviceSeq")),
                "status": "대기",
                "openedNotificationId": _coerce_int(
                    context.get("notificationId")
                ),
                "openedCode": code,
                "openedAt": actual_acknowledged_at.isoformat(),
                "rootExternalMessageId": external_message_id,
                "rootPermalink": permalink,
                "rowNumber": None,
                "lastSheetCheckedAt": "",
                "lastSuppressedAt": "",
                "lastSuppressedNotificationId": None,
                "lastSuppressedCode": "",
                "suppressedCount": 0,
            }
        state.update(
            {
                "lastSentAt": actual_acknowledged_at.isoformat(),
                "lastSentNotificationId": _coerce_int(
                    context.get("notificationId")
                ),
                "lastExternalMessageId": external_message_id,
                "lastPermalink": permalink,
            }
        )


def _cycle_result(
    state: Mapping[str, Any],
    *,
    deliveries: tuple[AutomationDelivery, ...],
    processed_count: int,
) -> AutomationCycleResult:
    cursor = {**dict(state), "cycleCompleted": False}
    return AutomationCycleResult(
        cycle="device_notification_alert",
        outcome="completed" if deliveries else "no_change",
        cursor=cursor,
        deliveries=deliveries,
        metrics={
            "processedCount": max(0, int(processed_count)),
            "deliveryCount": len(deliveries),
            "lastSeenId": max(0, _coerce_int(cursor.get("lastSeenId"))),
            "pendingVideoVerificationCount": len(
                cursor.get("pendingVideoVerifications") or {}
            ),
            "droppedVideoVerificationCount": max(
                0,
                _coerce_int(cursor.get("videoVerificationDroppedCount")),
            ),
            "disabledVideoVerificationCount": max(
                0,
                _coerce_int(cursor.get("videoVerificationDisabledCount")),
            ),
        },
    )


def _normalize_cursor(
    value: Mapping[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    source = dict(value) if isinstance(value, Mapping) else {}
    pending_contexts = {
        str(key): dict(item)
        for key, item in (
            source.get("pendingDeliveryContexts") or {}
        ).items()
        if str(key or "").strip() and isinstance(item, Mapping)
    } if isinstance(source.get("pendingDeliveryContexts"), Mapping) else {}
    recording_incidents = {
        str(key): dict(item)
        for key, item in (source.get("recordingStallIncidents") or {}).items()
        if str(key or "").strip() and isinstance(item, Mapping)
    } if isinstance(source.get("recordingStallIncidents"), Mapping) else {}
    captureboard_incidents = {
        str(key): dict(item)
        for key, item in (source.get("captureboardIncidents") or {}).items()
        if str(key or "").strip() and isinstance(item, Mapping)
    } if isinstance(source.get("captureboardIncidents"), Mapping) else {}
    pending_video_verifications: dict[str, dict[str, Any]] = {}
    normalized_video_verification_drop_count = 0
    normalized_video_verification_last_dropped_id = 0
    raw_video_verifications = source.get("pendingVideoVerifications")
    if isinstance(raw_video_verifications, Mapping):
        for raw_key, raw_item in sorted(
            raw_video_verifications.items(),
            key=lambda item: _coerce_int(item[0]),
        ):
            if len(pending_video_verifications) >= _VIDEO_VERIFICATION_QUEUE_LIMIT:
                normalized_video_verification_drop_count += 1
                normalized_video_verification_last_dropped_id = max(
                    normalized_video_verification_last_dropped_id,
                    _coerce_int(raw_key),
                )
                continue
            if not isinstance(raw_item, Mapping):
                normalized_video_verification_drop_count += 1
                normalized_video_verification_last_dropped_id = max(
                    normalized_video_verification_last_dropped_id,
                    _coerce_int(raw_key),
                )
                continue
            notification_id = _coerce_int(
                raw_item.get("notificationId") or raw_key
            )
            verify_after = _parse_event_datetime(raw_item.get("verifyAfter"))
            if notification_id <= 0 or verify_after is None:
                normalized_video_verification_drop_count += 1
                normalized_video_verification_last_dropped_id = max(
                    normalized_video_verification_last_dropped_id,
                    notification_id,
                )
                continue
            if str(notification_id) in pending_video_verifications:
                normalized_video_verification_drop_count += 1
                normalized_video_verification_last_dropped_id = max(
                    normalized_video_verification_last_dropped_id,
                    notification_id,
                )
                continue
            last_attempt_at = _parse_event_datetime(
                raw_item.get("lastAttemptAt")
            )
            pending_video_verifications[str(notification_id)] = {
                "notificationId": notification_id,
                "verifyAfter": verify_after.isoformat(),
                "attemptCount": max(
                    0,
                    min(100_000, _coerce_int(raw_item.get("attemptCount"))),
                ),
                "lastAttemptAt": (
                    last_attempt_at.isoformat()
                    if last_attempt_at is not None
                    else ""
                ),
                "lastReason": str(raw_item.get("lastReason") or "")[:64],
            }
    pending_events = [
        event
        for item in (
            source.get("pendingEvents")
            if isinstance(source.get("pendingEvents"), (list, tuple))
            else []
        )
        if (event := _normalize_event(item)) is not None
    ][:_DEVICE_NOTIFICATION_BATCH_SIZE]
    first_video_verification_dropped_at = _parse_event_datetime(
        source.get("videoVerificationFirstDroppedAt")
    )
    last_video_verification_dropped_at = _parse_event_datetime(
        source.get("videoVerificationLastDroppedAt")
    )
    last_video_verification_disabled_at = _parse_event_datetime(
        source.get("videoVerificationLastDisabledAt")
    )
    if normalized_video_verification_drop_count:
        normalized_at = _coerce_aware_datetime(now, fallback=_utc_now())
        first_video_verification_dropped_at = (
            first_video_verification_dropped_at or normalized_at
        )
        last_video_verification_dropped_at = normalized_at
    recent_alerts = {
        str(key): dict(item)
        for key, item in (source.get("recentCaptureboardAlerts") or {}).items()
        if str(key or "").strip() and isinstance(item, Mapping)
    } if isinstance(source.get("recentCaptureboardAlerts"), Mapping) else {}

    repair_cutoff = _coerce_aware_datetime(
        now,
        fallback=datetime.now(timezone.utc),
    ) - timedelta(hours=72)
    pending_sheet_repairs: dict[str, dict[str, Any]] = {}
    raw_sheet_repairs = source.get("pendingSheetRepairs")
    if isinstance(raw_sheet_repairs, Mapping):
        for raw_key, raw_repair in raw_sheet_repairs.items():
            if not isinstance(raw_repair, Mapping):
                continue
            queued_at = _parse_event_datetime(raw_repair.get("queuedAt"))
            repair_status = str(raw_repair.get("status") or "")[:64]
            # outbox marker는 실제 payload가 별도 파일에 있지만 direct repair는
            # cursor가 유일한 복구 context라 성공/disabled 전에는 만료시키지 않는다.
            if queued_at is None or (
                repair_status != "sheet_pending"
                and queued_at < repair_cutoff.astimezone(timezone.utc)
            ):
                continue
            pending_sheet_repairs[str(raw_key)] = {
                "queuedAt": queued_at.isoformat(),
                "detectedAt": str(raw_repair.get("detectedAt") or "")[:64],
                "permalink": str(raw_repair.get("permalink") or "")[:2048],
                "item": _sheet_repair_item(
                    raw_repair.get("item")
                    if isinstance(raw_repair.get("item"), Mapping)
                    else {}
                ),
                "repairKey": str(raw_repair.get("repairKey") or "")[:64],
                "status": repair_status,
            }

    cutoff = _coerce_aware_datetime(now, fallback=datetime.now(timezone.utc)) - timedelta(
        seconds=_AUTO_SMS_DEDUPE_WINDOW_SECONDS
    )
    claims: dict[str, dict[str, Any]] = {}
    raw_claims = source.get("autoSmsClaims")
    if isinstance(raw_claims, Mapping):
        for raw_key, raw_claim in raw_claims.items():
            if not isinstance(raw_claim, Mapping):
                continue
            claimed_at = _parse_event_datetime(raw_claim.get("claimedAt"))
            if claimed_at is None or claimed_at < cutoff.astimezone(timezone.utc):
                continue
            claims[str(raw_key)] = {
                "claimedAt": claimed_at.isoformat(),
                "notificationId": max(
                    0,
                    _coerce_int(raw_claim.get("notificationId")),
                ),
            }

    return {
        **source,
        "initialized": bool(source.get("initialized")),
        "lastSeenId": max(0, _coerce_int(source.get("lastSeenId"))),
        "pendingDeliveryContexts": pending_contexts,
        "recordingStallIncidents": recording_incidents,
        "captureboardIncidents": captureboard_incidents,
        "pendingEvents": pending_events,
        # 지연 검증에는 notification ID와 timing/status만 보존하고 원본
        # barcode/fileId/연락처는 due 시점의 exact DB JOIN으로 다시 읽는다.
        "pendingVideoVerifications": pending_video_verifications,
        # queue 포화 시 기존 장비 알림을 막지 않기 위해 overflow mismatch만
        # 버리되, PII 없는 누적 marker를 남겨 운영 알람이 잡을 수 있게 한다.
        "videoVerificationDroppedCount": max(
            0,
            min(
                1_000_000_000,
                _coerce_int(source.get("videoVerificationDroppedCount"))
                + normalized_video_verification_drop_count,
            ),
        ),
        "videoVerificationFirstDroppedAt": (
            first_video_verification_dropped_at.isoformat()
            if first_video_verification_dropped_at is not None
            else ""
        ),
        "videoVerificationLastDroppedAt": (
            last_video_verification_dropped_at.isoformat()
            if last_video_verification_dropped_at is not None
            else ""
        ),
        "videoVerificationLastDroppedNotificationId": max(
            0,
            _coerce_int(
                source.get("videoVerificationLastDroppedNotificationId")
            ),
            normalized_video_verification_last_dropped_id,
        ),
        "videoVerificationDisabledCount": max(
            0,
            min(
                1_000_000_000,
                _coerce_int(source.get("videoVerificationDisabledCount")),
            ),
        ),
        "videoVerificationLastDisabledAt": (
            last_video_verification_disabled_at.isoformat()
            if last_video_verification_disabled_at is not None
            else ""
        ),
        "videoVerificationLastDisabledNotificationId": max(
            0,
            _coerce_int(
                source.get("videoVerificationLastDisabledNotificationId")
            ),
        ),
        "recentCaptureboardAlerts": recent_alerts,
        # 이 표식은 delivery 재발송을 막지 않고 durable outbox repair가 진행
        # 중임을 cursor에서 확인할 수 있게 최대 72시간만 유지한다.
        "pendingSheetRepairs": pending_sheet_repairs,
        "lastSheetWriteStatus": str(
            source.get("lastSheetWriteStatus") or ""
        )[:64],
        "autoSmsClaims": claims,
        "cycleCompleted": False,
    }


def _normalize_event(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    notification_id = _coerce_int(value.get("notificationId"))
    code = str(value.get("code") or "").strip()
    device_name = str(value.get("deviceName") or "").strip()
    if notification_id <= 0 or code not in _SUPPORTED_CODES or not device_name:
        return None
    details = _normalize_json_object(value.get("details"))
    error_detail = str(
        details.get("errorDetail") or details.get("error") or ""
    ).strip()
    if len(error_detail) > 300:
        error_detail = f"{error_detail[:297]}..."
    # path/barcode 원문은 cursor에 보존하지 않고, 기존 Slack
    # 병합 실패 카드가 보여주던 오류 요약만 300자로 유지한다.
    safe_details = {
        "voiceType": _normalize_voice_type(details.get("voiceType")),
        "segmentCount": _coerce_optional_int(details.get("segmentCount")),
        "currentStatus": str(details.get("currentStatus") or "").strip().lower(),
        "durationSeconds": _coerce_optional_int(details.get("durationSeconds")),
        "growthRate": _coerce_optional_float(details.get("growthRate")),
        "expectedMinGrowth": _coerce_optional_float(
            details.get("expectedMinGrowth")
        ),
        "currentSize": _coerce_optional_int(details.get("currentSize")),
        "fileType": str(details.get("fileType") or "").strip().lower(),
        "errorDetail": error_detail,
    }
    return {
        "notificationId": notification_id,
        "deviceSeq": _coerce_optional_int(value.get("deviceSeq")),
        "deviceName": device_name,
        "deviceVersion": str(value.get("deviceVersion") or "").strip(),
        "code": code,
        "message": str(value.get("message") or "").strip(),
        "details": safe_details,
        "occurredAt": _serialize_datetime(value.get("occurredAt")),
        "hospitalSeq": _coerce_optional_int(value.get("hospitalSeq")),
        "hospitalName": str(value.get("hospitalName") or "").strip(),
        "hospitalTelephone": str(
            value.get("hospitalTelephone") or ""
        ).strip(),
        # 자동발송과 기존 Slack 연락처 표시에 같이 사용한다.
        "hospitalDeviceAlertPhone": str(
            value.get("hospitalDeviceAlertPhone") or ""
        ).strip(),
        "hospitalRoomSeq": _coerce_optional_int(value.get("hospitalRoomSeq")),
        "roomName": str(value.get("roomName") or "").strip(),
        # 녹화 단위 dedupe key는 원문 대신 해시로만 cursor에 남긴다.
        "incidentDiscriminator": (
            str(value.get("incidentDiscriminator"))
            if re.fullmatch(
                r"[0-9a-f]{64}",
                str(value.get("incidentDiscriminator") or ""),
            )
            else hashlib.sha256(
                "\0".join(
                    (
                        device_name,
                        str(
                            value.get("fileId")
                            or details.get("fileId")
                            or "-"
                        ),
                        str(
                            value.get("barcode")
                            or details.get("barcode")
                            or "-"
                        ),
                        str(details.get("fileType") or "recording"),
                    )
                ).encode("utf-8")
            ).hexdigest()
        ),
    }


def _consume_pending_event(
    state: dict[str, Any],
    notification_id: int,
) -> None:
    """Slack 성공·domain 억제 뒤 legacy queue의 해당 앞 이벤트를 제거한다."""

    if notification_id <= 0:
        return
    pending = [
        item
        for item in (state.get("pendingEvents") or [])
        if isinstance(item, Mapping)
    ]
    if pending and _coerce_int(pending[0].get("notificationId")) == notification_id:
        state["pendingEvents"] = pending[1:]
        return
    state["pendingEvents"] = [
        item
        for item in pending
        if _coerce_int(item.get("notificationId")) != notification_id
    ]


def _defer_video_duration_mismatch_events(
    state: dict[str, Any],
    *,
    now: datetime,
    logger: logging.Logger,
) -> int:
    """mismatch를 PII 없는 검증 queue로 옮겨 일반 알림 순서를 연다."""

    pending = state.get("pendingEvents") or []
    remaining: list[dict[str, Any]] = []
    dropped_count = 0
    current_at = _coerce_aware_datetime(now, fallback=_utc_now())
    for raw_event in pending:
        event = _normalize_event(raw_event)
        if event is None:
            continue
        if event["code"] != _VIDEO_DURATION_MISMATCH:
            remaining.append(event)
            continue
        notification_id = int(event["notificationId"])
        key = str(notification_id)
        verifications = state["pendingVideoVerifications"]
        if key in verifications:
            continue
        if len(verifications) >= _VIDEO_VERIFICATION_QUEUE_LIMIT:
            # verifier 장애로 queue가 찼을 때 overflow mismatch 한 건 때문에
            # 뒤의 캡처보드 등 기존 알림까지 막지 않는다. 원문 대신 누적 marker와
            # ERROR 로그를 남겨 운영 감시가 provider 장애로 처리하게 한다.
            logger.error(
                "Video duration mismatch verification dropped queue_full "
                "notification_id=%s",
                notification_id,
            )
            _record_dropped_video_verification(
                state,
                notification_id,
                now=current_at,
            )
            dropped_count += 1
            continue
        occurred_at = _parse_event_datetime(event.get("occurredAt"))
        # 장비 시계가 미래이거나 깨졌다면 관찰 시각을 기준으로 온전한 유예를 준다.
        base_at = (
            occurred_at
            if occurred_at is not None and occurred_at <= current_at
            else current_at
        )
        verify_after = base_at + timedelta(
            seconds=max(
                1,
                int(cs.DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_GRACE_SEC),
            )
        )
        verifications[key] = {
            "notificationId": notification_id,
            "verifyAfter": verify_after.isoformat(),
            "attemptCount": 0,
            "lastAttemptAt": "",
            "lastReason": "",
        }
    state["pendingEvents"] = remaining
    return dropped_count


def _record_dropped_video_verification(
    state: dict[str, Any],
    notification_id: int,
    *,
    now: datetime,
) -> None:
    """queue overflow를 식별자 원문 없이 durable 운영 지표로 남긴다."""

    dropped_at = _coerce_aware_datetime(now, fallback=_utc_now()).isoformat()
    previous_count = max(
        0,
        _coerce_int(state.get("videoVerificationDroppedCount")),
    )
    state["videoVerificationDroppedCount"] = min(
        1_000_000_000,
        previous_count + 1,
    )
    if not str(state.get("videoVerificationFirstDroppedAt") or "").strip():
        state["videoVerificationFirstDroppedAt"] = dropped_at
    state["videoVerificationLastDroppedAt"] = dropped_at
    state["videoVerificationLastDroppedNotificationId"] = max(
        0,
        _coerce_int(notification_id),
    )


def _next_due_video_verification(
    state: Mapping[str, Any],
    *,
    now: datetime,
) -> dict[str, Any] | None:
    """검증 시각이 지난 가장 오래된 notification 하나만 고른다."""

    current_at = _coerce_aware_datetime(now, fallback=_utc_now())
    candidates: list[tuple[datetime, int, dict[str, Any]]] = []
    raw_items = state.get("pendingVideoVerifications")
    if not isinstance(raw_items, Mapping):
        return None
    for raw_item in raw_items.values():
        if not isinstance(raw_item, Mapping):
            continue
        notification_id = _coerce_int(raw_item.get("notificationId"))
        verify_after = _parse_event_datetime(raw_item.get("verifyAfter"))
        if (
            notification_id <= 0
            or verify_after is None
            or verify_after > current_at
        ):
            continue
        candidates.append(
            (verify_after, notification_id, dict(raw_item))
        )
    if not candidates:
        return None
    return min(candidates, key=lambda item: (item[0], item[1]))[2]


def _reschedule_video_verification(
    state: dict[str, Any],
    notification_id: int,
    *,
    now: datetime,
    reason: str,
) -> None:
    """불명확한 외부 조회를 누락으로 바꾸지 않고 bounded backoff한다."""

    key = str(max(0, _coerce_int(notification_id)))
    raw_item = state["pendingVideoVerifications"].get(key)
    if not isinstance(raw_item, Mapping):
        return
    attempt_count = min(
        100_000,
        max(0, _coerce_int(raw_item.get("attemptCount"))) + 1,
    )
    retry_seconds = min(
        3600,
        _VIDEO_VERIFICATION_RETRY_SECONDS
        * (2 ** min(max(0, attempt_count - 1), 3)),
    )
    current_at = _coerce_aware_datetime(now, fallback=_utc_now())
    safe_reasons = {
        _VIDEO_UPLOAD_PROVIDER_ERROR,
        "ambiguous_recording",
        "notification_file_id_missing",
        "recording_device_mismatch",
        "streaming_not_available",
        "invalid_s3_location",
        "object_not_ready",
    }
    state["pendingVideoVerifications"][key] = {
        "notificationId": int(key),
        "verifyAfter": (current_at + timedelta(seconds=retry_seconds)).isoformat(),
        "attemptCount": attempt_count,
        "lastAttemptAt": current_at.isoformat(),
        "lastReason": reason if reason in safe_reasons else "indeterminate",
    }


def _remove_video_verification(
    state: dict[str, Any],
    notification_id: int,
) -> None:
    state["pendingVideoVerifications"].pop(
        str(max(0, _coerce_int(notification_id))),
        None,
    )


def _discard_disabled_video_duration_mismatches(
    state: dict[str, Any],
    *,
    now: datetime,
) -> int:
    """rollout off의 새 mismatch만 건너뛰고 기존 deferred는 pause한다."""

    discarded = 0
    remaining: list[dict[str, Any]] = []
    for raw_event in state.get("pendingEvents") or []:
        event = _normalize_event(raw_event)
        if event is None:
            continue
        if event["code"] == _VIDEO_DURATION_MISMATCH:
            discarded += 1
            state["videoVerificationDisabledCount"] = min(
                1_000_000_000,
                max(
                    0,
                    _coerce_int(state.get("videoVerificationDisabledCount")),
                )
                + 1,
            )
            state["videoVerificationLastDisabledAt"] = (
                _coerce_aware_datetime(now, fallback=_utc_now()).isoformat()
            )
            state["videoVerificationLastDisabledNotificationId"] = int(
                event["notificationId"]
            )
        else:
            remaining.append(event)
    state["pendingEvents"] = remaining
    return discarded


def _enrich_video_verification_event(
    row: Mapping[str, Any],
    event: Mapping[str, Any],
) -> dict[str, Any]:
    """due 시점 조회값에서 Slack에 필요한 최소 세션 정보만 추린다."""

    details = _normalize_json_object(row.get("details"))
    raw_barcode = str(row.get("barcode") or details.get("barcode") or "").strip()
    barcode = raw_barcode if re.fullmatch(r"[0-9]{11}", raw_barcode) else ""
    expected_duration = _coerce_optional_float(details.get("expectedDuration"))
    if (
        expected_duration is None
        or expected_duration <= 0
        or expected_duration > 86_400
    ):
        expected_duration = None
    return {
        **dict(event),
        "sessionBarcode": barcode,
        "recordingRecordedAt": _serialize_datetime(
            row.get("recordingRecordedAt")
        ),
        "expectedDuration": expected_duration,
    }


def _video_session_fields(event: Mapping[str, Any]) -> dict[str, str]:
    """실제 녹화 시작 시각을 우선하고, DB row가 없으면 시작을 추정한다."""

    details = (
        event.get("details")
        if isinstance(event.get("details"), Mapping)
        else {}
    )
    raw_barcode = str(
        event.get("sessionBarcode")
        or event.get("barcode")
        or details.get("barcode")
        or ""
    ).strip()
    barcode = raw_barcode if re.fullmatch(r"[0-9]{11}", raw_barcode) else "미확인"

    recorded_at = _parse_event_datetime(event.get("recordingRecordedAt"))
    if recorded_at is not None:
        return {
            "barcode": barcode,
            "sessionAtLabel": "세션 시작",
            "sessionAt": _format_occurred_at(recorded_at),
        }

    expected_duration = _coerce_optional_float(
        event.get("expectedDuration") or details.get("expectedDuration")
    )
    occurred_at = _parse_event_datetime(event.get("occurredAt"))
    if (
        occurred_at is not None
        and expected_duration is not None
        and 0 < expected_duration <= 86_400
    ):
        return {
            "barcode": barcode,
            "sessionAtLabel": "세션 시작(추정)",
            "sessionAt": _format_occurred_at(
                occurred_at - timedelta(seconds=expected_duration)
            ),
        }
    return {
        "barcode": barcode,
        "sessionAtLabel": "영상 이상 감지 시각",
        "sessionAt": _format_occurred_at(event.get("occurredAt")),
    }


def _build_root_alert(
    event: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any] | None]:
    code = str(event.get("code") or "")
    hospital_seq = _coerce_optional_int(event.get("hospitalSeq"))
    hospital_name = str(event.get("hospitalName") or "").strip() or "병원 미확인"
    room_name = str(event.get("roomName") or "").strip() or "병실 미확인"
    device_name = str(event.get("deviceName") or "").strip() or "장비명 미확인"
    details = (
        event.get("details")
        if isinstance(event.get("details"), Mapping)
        else {}
    )
    problem_components: list[str] = []
    recording_context: dict[str, Any] | None = None

    if code == _CAPTUREBOARD_CONNECTION_ERROR:
        issue = _format_issue(
            str(event.get("message") or "").strip()
            or "캡처보드 연결 장애가 발생했어",
            event.get("occurredAt"),
        )
        alert_category = "video_signal"
        problem_components = ["캡처보드"]
        component_labels = {
            "audio": "정상",
            "pm2": "정상",
            "storage": "정상",
            "captureboard": "이상",
            "led": "정상",
        }
    elif code == _SEGMENTED_RECORDINGS_MERGE_ERROR:
        segment_count = _coerce_optional_int(details.get("segmentCount"))
        segment_text = (
            f" / 분할 파일 {segment_count}개"
            if segment_count is not None and segment_count > 0
            else ""
        )
        issue_parts = [
            str(event.get("message") or "").strip()
            or "분할된 녹화 파일 병합에 실패했어"
        ]
        if segment_text:
            issue_parts.append(segment_text.removeprefix(" / "))
        error_detail = str(details.get("errorDetail") or "").strip()
        if error_detail:
            issue_parts.append(f"오류: {error_detail}")
        issue = _format_issue(
            " / ".join(issue_parts),
            event.get("occurredAt"),
        )
        alert_category = "recording_processing"
        component_labels = {
            "audio": "정상",
            "pm2": "정상",
            "storage": "정상",
            "captureboard": "정상",
            "led": "정상",
        }
    elif code == _VIDEO_DURATION_MISMATCH:
        unavailable_reason = str(
            event.get("videoAvailabilityReason") or ""
        ).strip()
        # DB row나 S3 객체 같은 구현 용어를 노출하지 않고, CX가 확인해야 할
        # 업로드 결과와 실제 영상 파일 상태로 설명한다.
        if unavailable_reason == _VIDEO_UPLOAD_MISSING_RECORDING:
            unavailable_text = "영상 업로드가 확인되지 않았어"
        elif unavailable_reason == _VIDEO_UPLOAD_UNDERSIZED_OBJECT:
            unavailable_text = "업로드된 영상 파일의 크기가 비정상적으로 작아"
        else:
            unavailable_text = "업로드된 영상 파일을 찾을 수 없어"
        issue = _format_issue(
            "영상 길이 이상을 감지한 뒤 업로드 완료를 기다렸지만 "
            + unavailable_text,
            event.get("occurredAt"),
        )
        alert_category = "upload"
        problem_components = ["영상 업로드"]
        component_labels = {
            "audio": "정상",
            "pm2": "정상",
            "storage": "정상",
            "captureboard": "정상",
            "led": "정상",
        }
        video_session_fields = _video_session_fields(event)
    else:
        duration_seconds = max(0, _coerce_int(details.get("durationSeconds")))
        growth_rate = _coerce_optional_float(details.get("growthRate"))
        issue = _format_issue(
            "녹화 파일 증가 정지가 "
            f"{_format_duration(duration_seconds)} 동안 지속됐어: "
            f"{_format_growth_rate(growth_rate)}",
            event.get("occurredAt"),
        )
        alert_category = "recording"
        component_labels = {
            "audio": "정상",
            "pm2": "정상",
            "storage": "정상",
            "captureboard": "정상",
            "led": "정상",
        }
        incident_key = str(event.get("incidentDiscriminator") or "")
        recording_context = {
            "incidentKey": incident_key,
            "incident": {
                "phase": "awaiting_delivery",
                "deviceName": device_name,
                "lastNotificationId": _coerce_int(event.get("notificationId")),
                "lastOccurredAt": str(event.get("occurredAt") or ""),
                "lastDurationSeconds": duration_seconds,
                "lastCurrentSize": _coerce_optional_int(details.get("currentSize")),
                "rootExternalMessageId": "",
                "rootPermalink": "",
                "lastCommentNotificationId": None,
            },
        }

    device_result = {
        "hospitalSeq": hospital_seq,
        "hospitalName": hospital_name,
        "hospitalTelephone": str(
            event.get("hospitalTelephone") or ""
        ).strip(),
        "hospitalDeviceAlertPhone": str(
            event.get("hospitalDeviceAlertPhone") or ""
        ).strip(),
        "hospitalRoomSeq": _coerce_optional_int(event.get("hospitalRoomSeq")),
        "roomName": room_name,
        "deviceName": device_name,
        "deviceVersion": str(event.get("deviceVersion") or ""),
        "voiceType": _normalize_voice_type(details.get("voiceType")),
        "overallLabel": "이상",
        "priorityReason": issue,
        "alertCategory": alert_category,
        "componentLabels": component_labels,
    }
    if code == _VIDEO_DURATION_MISMATCH:
        # 병원·병실·장비와 같은 카드에서 문제 영상을 바로 특정할 수 있게 한다.
        device_result.update(video_session_fields)
    if code == _CAPTUREBOARD_CONNECTION_ERROR:
        device_result["statusPayload"] = {
            "overview": {
                "captureboard": {
                    "status": "fail",
                    "label": "이상",
                    "summary": issue,
                }
            }
        }
    summary = {
        "hospitalSeq": hospital_seq,
        "hospitalName": hospital_name,
        "statusCounts": {
            "정상": 0,
            "확인 필요": 0,
            "이상": 1,
            "점검 불가": 0,
        },
        "deviceResults": [device_result],
    }
    alert_item = {
        "hospitalSeq": str(hospital_seq or ""),
        "hospitalName": hospital_name,
        "telephone": str(event.get("hospitalTelephone") or "").strip(),
        "deviceAlertPhone": str(
            event.get("hospitalDeviceAlertPhone") or ""
        ).strip(),
        "room": room_name,
        "device": device_name,
        "deviceVersion": str(event.get("deviceVersion") or ""),
        "voiceType": _normalize_voice_type(details.get("voiceType")),
        "issue": issue,
        "alertCategory": alert_category,
        "problemComponents": problem_components,
    }
    return summary, alert_item, recording_context


def _build_recording_stall_continuation(
    state: Mapping[str, Any],
    event: Mapping[str, Any],
) -> dict[str, Any] | None:
    incident_key = str(event.get("incidentDiscriminator") or "")
    incident = (state.get("recordingStallIncidents") or {}).get(incident_key)
    if not isinstance(incident, Mapping) or incident.get("phase") != "alerted":
        return None
    root_message_id = str(incident.get("rootExternalMessageId") or "").strip()
    if not root_message_id:
        return None
    details = event.get("details") if isinstance(event.get("details"), Mapping) else {}
    duration_seconds = max(0, _coerce_int(details.get("durationSeconds")))
    previous_duration_seconds = max(
        0,
        _coerce_int(incident.get("lastDurationSeconds")),
    )
    occurred_at = _parse_event_datetime(event.get("occurredAt"))
    previous_occurred_at = _parse_event_datetime(incident.get("lastOccurredAt"))
    if occurred_at is None or previous_occurred_at is None:
        return None
    gap_seconds = (occurred_at - previous_occurred_at).total_seconds()
    if not (
        _is_recording_stall_scope(event)
        and duration_seconds > previous_duration_seconds
        and 0 < gap_seconds <= _RECORDING_STALL_MAX_EVENT_GAP_SECONDS
    ):
        # 끊긴 incident는 새 root 판단으로 이어지도록 현재 상태에서 제거한다.
        recording_incidents = state.get("recordingStallIncidents")
        if isinstance(recording_incidents, dict):
            recording_incidents.pop(incident_key, None)
        return None
    next_incident = {
        **dict(incident),
        "lastNotificationId": _coerce_int(event.get("notificationId")),
        "lastOccurredAt": str(event.get("occurredAt") or ""),
        "lastDurationSeconds": duration_seconds,
        "lastCurrentSize": _coerce_optional_int(details.get("currentSize")),
        "lastCommentNotificationId": _coerce_int(event.get("notificationId")),
    }
    return {
        "incidentKey": incident_key,
        "replyToExternalMessageId": root_message_id,
        "durationText": _format_duration(duration_seconds),
        "growthRateText": _format_growth_rate(details.get("growthRate")),
        "nextIncident": next_incident,
    }


def _is_recording_stall_scope(event: Mapping[str, Any]) -> bool:
    details = event.get("details") if isinstance(event.get("details"), Mapping) else {}
    duration_seconds = _coerce_optional_int(details.get("durationSeconds"))
    return (
        str(details.get("currentStatus") or "") == "recording"
        and str(details.get("fileType") or "") != "motion"
        and duration_seconds is not None
        and duration_seconds >= _RECORDING_STALL_MIN_DURATION_SECONDS
    )


def _is_recording_stall_alert(event: Mapping[str, Any]) -> bool:
    details = event.get("details") if isinstance(event.get("details"), Mapping) else {}
    growth_rate = _coerce_optional_float(details.get("growthRate"))
    return (
        _is_recording_stall_scope(event)
        and growth_rate == 0
        and details.get("currentSize") is not None
    )


def _apply_sms_receipt(
    summary: dict[str, Any],
    alert_item: dict[str, Any],
    receipt: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    device_result = dict(summary["deviceResults"][0])
    device_result["smsContactActionEnabled"] = (
        "true" if receipt.get("contactActionEnabled", True) else "false"
    )
    # Slack renderer가 쓰는 표시값에는 provider 추적 ID를 싣지 않는다.
    public_receipt_to_result = {
        "statusText": "smsStatusText",
        "phoneNumber": "smsPhoneNumber",
        "message": "smsMessage",
        "templateId": "smsTemplateId",
        "deliveryStatus": "smsDeliveryStatus",
    }
    next_alert_item = dict(alert_item)
    for receipt_key, result_key in public_receipt_to_result.items():
        text = str(receipt.get(receipt_key) or "").strip()
        if text:
            device_result[result_key] = text
    # Sheets의 숨김 추적 메타데이터는 provider ID가 필요하므로 cursor 안의
    # ack context에만 보존하고 delivery alertSummary에는 포함하지 않는다.
    receipt_to_sheet_item = {
        **public_receipt_to_result,
        "provider": "smsProvider",
        "groupId": "smsGroupId",
        "messageId": "smsMessageId",
        "acceptedAt": "smsAcceptedAt",
    }
    for receipt_key, result_key in receipt_to_sheet_item.items():
        text = str(receipt.get(receipt_key) or "").strip()
        if text:
            next_alert_item[result_key] = text
    return (
        {**summary, "deviceResults": [device_result]},
        next_alert_item,
        dict(receipt),
    )


def _sheet_repair_item(value: Mapping[str, Any]) -> dict[str, Any]:
    """outbox와 direct Sheet 재시도에 필요한 non-PII 필드만 보존한다."""

    raw_components = value.get("problemComponents")
    components = (
        [str(item) for item in raw_components if str(item).strip()]
        if isinstance(raw_components, (list, tuple))
        else []
    )
    return {
        "device": str(value.get("device") or "장비명 미확인")[:255],
        "hospitalName": str(
            value.get("hospitalName")
            or value.get("hospital")
            or "병원 미확인"
        )[:255],
        "room": str(value.get("room") or "병실 미확인")[:255],
        "problemComponents": components[:20],
        "issue": str(value.get("issue") or "상세 확인 필요")[:1000],
        "smsStatusText": str(value.get("smsStatusText") or "")[:255],
        "smsDeliveryStatus": str(
            value.get("smsDeliveryStatus") or ""
        )[:64],
        "smsGroupId": str(value.get("smsGroupId") or "")[:256],
        "smsAcceptedAt": str(value.get("smsAcceptedAt") or "")[:64],
        "sheetDeliveryId": str(value.get("sheetDeliveryId") or "")[:512],
    }


def _public_sms_receipt(value: Mapping[str, Any]) -> dict[str, Any]:
    """Slack 표시와 action gating에 필요한 비식별 상태만 노출한다."""

    allowed_keys = (
        "attempted",
        "status",
        "ok",
        "statusText",
        "contactActionEnabled",
        "deliveryStatus",
        "templateId",
        "deduplicated",
    )
    return {
        key: value[key]
        for key in allowed_keys
        if key in value
    }


def _normalize_sms_receipt(
    value: Mapping[str, Any],
    *,
    template_id: str,
    accepted_at: datetime,
) -> dict[str, Any]:
    status = str(value.get("status") or "error").strip().lower()
    request_accepted = bool(value.get("ok")) and status == "sent"
    delivery_status = str(value.get("smsDeliveryStatus") or "").strip()
    if not delivery_status:
        delivery_status = (
            _SMS_DELIVERY_CONFIRM_REQUIRED
            if request_accepted
            else _SMS_DELIVERY_REQUEST_FAILED
        )
    confirm_required = delivery_status == _SMS_DELIVERY_CONFIRM_REQUIRED
    receipt = {
        "attempted": True,
        "status": "sent" if request_accepted else "failed",
        "ok": request_accepted,
        "statusText": (
            _AUTO_SMS_CONFIRM_REQUIRED_TEXT
            if confirm_required
            else (
                _AUTO_SMS_ACCEPTED_TEXT
                if request_accepted
                else _AUTO_SMS_FAILED_TEXT
            )
        ),
        "contactActionEnabled": not request_accepted and not confirm_required,
        "deliveryStatus": delivery_status,
        "templateId": template_id,
        "provider": _safe_provider_identifier(value.get("provider")),
        "groupId": _safe_provider_identifier(value.get("groupId")),
        "messageId": _safe_provider_identifier(value.get("messageId")),
        "providerStatusCode": _safe_provider_identifier(
            value.get("providerStatusCode")
        ),
        "acceptedAt": accepted_at.isoformat() if request_accepted else "",
    }
    # 빈 추적값은 payload에서 제거해 receipt 계약을 작게 유지한다.
    return {
        key: item
        for key, item in receipt.items()
        if item not in {"", None}
    }


def _manual_sms_receipt(*, status: str, template_id: str) -> dict[str, Any]:
    return {
        "attempted": False,
        "status": status,
        "ok": False,
        "contactActionEnabled": True,
        "deliveryStatus": _SMS_DELIVERY_NOT_SENT,
        "templateId": template_id,
    }


def _auto_sms_claim_key(alert_item: Mapping[str, Any]) -> str:
    category = str(alert_item.get("alertCategory") or "").strip()
    family = (
        "captureboard_recording"
        if category in {"video_signal", "recording"}
        else category
    )
    if not family:
        return ""
    raw = "\0".join(
        (
            str(alert_item.get("hospitalSeq") or ""),
            str(alert_item.get("device") or ""),
            family,
        )
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _should_suppress_open_captureboard_event(
    state: Mapping[str, Any],
    event: Mapping[str, Any],
    *,
    now: datetime,
) -> bool:
    if event.get("code") != _CAPTUREBOARD_CONNECTION_ERROR:
        return False
    device_name = str(event.get("deviceName") or "").strip()
    incident = (state.get("captureboardIncidents") or {}).get(device_name)
    if not isinstance(incident, Mapping):
        return False

    # 반복 이벤트마다 lastSuppressedAt을 전진시키는 sliding quiet window다.
    # 오래됐거나 손상되거나 미래인 cursor 시각은 새 루트를 허용해
    # 실제 장애를 영구히 가리지 않는다.
    activity_value = incident.get("lastSuppressedAt") or incident.get("openedAt")
    activity_at = _parse_event_datetime(activity_value)
    current_at = _parse_event_datetime(now)
    if activity_at is None or current_at is None:
        return False
    age = current_at - activity_at
    quiet_seconds = max(
        1,
        int(cs.DEVICE_NOTIFICATION_CAPTUREBOARD_INCIDENT_QUIET_SEC),
    )
    return timedelta(0) <= age < timedelta(seconds=quiet_seconds)


def _mark_suppressed_captureboard_event(
    state: dict[str, Any],
    event: Mapping[str, Any],
    *,
    now: datetime,
) -> None:
    device_name = str(event.get("deviceName") or "").strip()
    incident = dict(state["captureboardIncidents"].get(device_name) or {})
    incident.update(
        {
            "lastSuppressedAt": now.isoformat(),
            "lastSuppressedNotificationId": _coerce_int(
                event.get("notificationId")
            ),
            "lastSuppressedCode": str(event.get("code") or ""),
            "suppressedCount": max(
                0,
                _coerce_int(incident.get("suppressedCount")),
            )
            + 1,
        }
    )
    state["captureboardIncidents"][device_name] = incident


def _receipt_value(receipt: Any, *names: str) -> Any:
    for name in names:
        if isinstance(receipt, Mapping) and name in receipt:
            return receipt.get(name)
        if hasattr(receipt, name):
            return getattr(receipt, name)
    return None


def _normalize_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _aws_error_code(exc: Exception) -> str:
    """botocore import 없이 provider 오류의 공개 code만 추출한다."""

    response = getattr(exc, "response", None)
    if not isinstance(response, Mapping):
        return ""
    error = response.get("Error")
    if not isinstance(error, Mapping):
        return ""
    return str(error.get("Code") or "").strip()


def _serialize_datetime(value: Any) -> str:
    if isinstance(value, datetime):
        actual = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return actual.isoformat()
    return str(value or "").strip()


def _parse_event_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        actual = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            actual = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if actual.tzinfo is None:
        actual = actual.replace(tzinfo=timezone.utc)
    return actual.astimezone(timezone.utc)


def _coerce_aware_datetime(value: Any, *, fallback: datetime) -> datetime:
    parsed = _parse_event_datetime(value)
    if parsed is not None:
        return parsed
    if fallback.tzinfo is None:
        return fallback.replace(tzinfo=timezone.utc)
    return fallback


def _format_occurred_at(value: Any) -> str:
    parsed = _parse_event_datetime(value)
    if parsed is None:
        return str(value or "미확인")
    return parsed.astimezone(_KST).strftime("%Y-%m-%d %H:%M:%S KST")


def _format_issue(issue: str, occurred_at: Any) -> str:
    return f"{issue} (발생 {_format_occurred_at(occurred_at)})"


def _format_duration(duration_seconds: int) -> str:
    if duration_seconds >= 60 and duration_seconds % 60 == 0:
        return f"{duration_seconds}초 ({duration_seconds // 60}분)"
    return f"{duration_seconds}초"


def _format_growth_rate(value: Any) -> str:
    growth_rate = _coerce_optional_float(value)
    if growth_rate is None:
        return "미확인"
    return f"{growth_rate / 1024:.2f} KB/sec"


def _safe_provider_identifier(value: Any) -> str:
    text = str(value or "").strip()
    return text if _SAFE_PROVIDER_ID_PATTERN.fullmatch(text) else ""


def _normalize_voice_type(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in _VOICE_TYPES else ""


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_optional_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = [
    "DeviceNotificationAlertCycleHandler",
    "DeviceNotificationCycleDeps",
    "VideoDurationMismatchVerification",
]
