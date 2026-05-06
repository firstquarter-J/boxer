import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from boxer.core import settings as s
from boxer.retrieval.connectors.db import _create_db_connection
from boxer.retrieval.connectors.s3 import _build_s3_client
from boxer_company.routers.box_db import _local_zone
from boxer_company.routers.mda_graphql import (
    _get_mda_stopped_recording_restore_candidates,
    _restore_mda_stopped_recordings,
)


@dataclass(frozen=True)
class _StreamingRestoreHospitalSummary:
    hospital_seq: int
    hospital_name: str
    db_target_count: int
    mda_candidate_count: int
    restorable_count: int


@dataclass(frozen=True)
class RecordingStreamingRestoreResult:
    barcode: str
    target_year: int
    target_month: int
    db_target_count: int
    mda_candidate_count: int
    restorable_count: int
    requested_count: int
    restored_count: int
    failed_count: int
    message: str
    failed_items: list[dict[str, Any]]
    hospitals: list[_StreamingRestoreHospitalSummary]
    s3_restore_requested_count: int = 0
    s3_restore_already_in_progress_count: int = 0
    s3_restore_already_restored_count: int = 0
    s3_restore_failed_count: int = 0
    s3_restore_items: list[dict[str, Any]] = field(default_factory=list)

_YEAR_MONTH_PATTERN = re.compile(
    r"(20\d{2})\s*(?:"
    r"년\s*(0?[1-9]|1[0-2])(?:\s*월(?!\s*(?:[0-3]?\d\s*일|\d))|(?!\s*(?:월|일|[0-3]?\d\s*일|\d)))"
    r"|[-./]\s*(0?[1-9]|1[0-2])(?!\s*(?:[-./]\s*\d{1,2}|\d))"
    r")"
)
_COMPACT_YEAR_MONTH_PATTERN = re.compile(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])(?!\d)")
# 운영 요청에서는 복원/복구/블라인드 해제를 같은 MDA 복원 의도로 본다.
_STREAMING_RESTORE_ACTION_PATTERN = re.compile(
    r"(스트리밍\s*종료.*(?:복원|복구|해제|원복)|복원|복구|원복|"
    r"블라인드(?:를|을)?\s*해제|숨김(?:을|를)?\s*해제|unblind|reveal|"
    r"공개\s*(?:처리|전환|해줘|해|시켜)|노출\s*(?:처리|전환|해줘|해|시켜|가능))",
    re.IGNORECASE,
)
_RECORDING_MEDIA_PATTERN = re.compile(
    r"(영상|동영상|녹화|recording|recordings|ultrasound)",
    re.IGNORECASE,
)
_S3_ARCHIVE_STORAGE_CLASSES = {"GLACIER", "DEEP_ARCHIVE"}
_S3_ARCHIVE_STATUSES = {"ARCHIVE_ACCESS", "DEEP_ARCHIVE_ACCESS"}
_S3_RESTORE_DAYS = 7
_S3_RESTORE_TIER = "Standard"


def _is_recording_streaming_restore_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    normalized = question or ""
    return bool(
        _RECORDING_MEDIA_PATTERN.search(normalized)
        and _STREAMING_RESTORE_ACTION_PATTERN.search(normalized)
    )


def _extract_recording_streaming_restore_month(question: str) -> tuple[int, int]:
    normalized = question or ""
    year_month_match = _YEAR_MONTH_PATTERN.search(normalized)
    if year_month_match:
        month_text = year_month_match.group(2) or year_month_match.group(3)
        return int(year_month_match.group(1)), int(month_text)

    compact_match = _COMPACT_YEAR_MONTH_PATTERN.search(normalized)
    if compact_match:
        return int(compact_match.group(1)), int(compact_match.group(2))

    raise ValueError("복원할 연도와 월을 같이 입력해줘. 예: `35033165423 2024년 4월 영상 복원`")


def _to_local_datetime(value: Any) -> datetime | None:
    parsed: datetime | None = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        raw = value.strip().replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError:
            return None

    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(_local_zone())


def _recording_row_year_month(row: dict[str, Any]) -> tuple[int, int] | None:
    local_dt = _to_local_datetime(row.get("recordedAt"))
    if local_dt is None:
        return None
    return local_dt.year, local_dt.month


def _local_month_to_utc_range(target_year: int, target_month: int) -> tuple[datetime, datetime]:
    if target_month < 1 or target_month > 12:
        raise ValueError("월은 1월부터 12월까지만 입력할 수 있어")

    local_tz = _local_zone()
    local_start = datetime(
        year=int(target_year),
        month=int(target_month),
        day=1,
        hour=0,
        minute=0,
        second=0,
        tzinfo=local_tz,
    )
    if target_month == 12:
        local_end = datetime(
            year=int(target_year) + 1,
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            tzinfo=local_tz,
        )
    else:
        local_end = datetime(
            year=int(target_year),
            month=int(target_month) + 1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            tzinfo=local_tz,
        )
    return (
        local_start.astimezone(timezone.utc).replace(tzinfo=None),
        local_end.astimezone(timezone.utc).replace(tzinfo=None),
    )


def _query_recording_streaming_restore_rows(
    barcode: str,
    *,
    requested_year: int,
    requested_month: int,
) -> list[dict[str, Any]]:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    utc_start, utc_end = _local_month_to_utc_range(requested_year, requested_month)
    where_clauses = [
        "r.fullBarcode = %s",
        "r.recordedAt IS NOT NULL",
        "r.recordedAt >= %s",
        "r.recordedAt < %s",
    ]
    params: list[Any] = [barcode, utc_start, utc_end]

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT "
                "r.seq, "
                "r.hospitalSeq, "
                "h.hospitalName AS hospitalName, "
                "r.fileId, "
                "r.s3Bucket, "
                "r.s3FileKey, "
                "r.recordedAt, "
                "r.createdAt "
                "FROM recordings r "
                "LEFT JOIN hospitals h ON r.hospitalSeq = h.seq "
                f"WHERE {' AND '.join(where_clauses)} "
                "ORDER BY COALESCE(r.recordedAt, r.createdAt) ASC, r.seq ASC",
                tuple(params),
            )
            return list(cursor.fetchall() or [])
    finally:
        connection.close()


def _load_recording_streaming_restore_targets(
    barcode: str,
    *,
    requested_year: int,
    requested_month: int,
) -> tuple[int, list[dict[str, Any]]]:
    normalized_barcode = str(barcode or "").strip()
    if not normalized_barcode:
        raise ValueError("바코드가 필요해")

    rows = _query_recording_streaming_restore_rows(
        normalized_barcode,
        requested_year=requested_year,
        requested_month=requested_month,
    )
    if not rows:
        raise ValueError(
            f"`{normalized_barcode}` `{requested_year}-{requested_month:02d}` "
            "recordings DB row가 없어"
        )
    return requested_year, rows


def _candidate_seq(candidate: dict[str, Any]) -> int | None:
    try:
        seq = int(candidate.get("seq") or 0)
    except (TypeError, ValueError):
        return None
    return seq if seq > 0 else None


def _row_seq(row: dict[str, Any]) -> int | None:
    try:
        seq = int(row.get("seq") or 0)
    except (TypeError, ValueError):
        return None
    return seq if seq > 0 else None


def _row_hospital_seq(row: dict[str, Any]) -> int | None:
    try:
        seq = int(row.get("hospitalSeq") or 0)
    except (TypeError, ValueError):
        return None
    return seq if seq > 0 else None


def _row_hospital_name(row: dict[str, Any]) -> str:
    return str(row.get("hospitalName") or "미확인").strip() or "미확인"


def _aws_error_code(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if not isinstance(response, dict):
        return ""
    error = response.get("Error")
    if not isinstance(error, dict):
        return ""
    return str(error.get("Code") or "").strip()


def _s3_restore_ongoing(restore_header: Any) -> bool | None:
    normalized = str(restore_header or "").strip().lower()
    if 'ongoing-request="true"' in normalized:
        return True
    if 'ongoing-request="false"' in normalized:
        return False
    return None


def _is_s3_archive_restore_needed(head_response: dict[str, Any]) -> bool:
    storage_class = str(head_response.get("StorageClass") or "STANDARD").strip().upper()
    archive_status = str(head_response.get("ArchiveStatus") or "").strip().upper()
    return storage_class in _S3_ARCHIVE_STORAGE_CLASSES or archive_status in _S3_ARCHIVE_STATUSES


def _request_s3_archive_restore(
    *,
    s3_client: Any,
    recording_seq: int,
    bucket: str,
    key: str,
) -> dict[str, Any]:
    base_item: dict[str, Any] = {
        "seq": recording_seq,
        "bucket": bucket,
        "key": key,
        "storageClass": "",
        "archiveStatus": "",
        "status": "",
        "message": "",
    }
    try:
        head_response = s3_client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:
        return {
            **base_item,
            "status": "failed",
            "message": _aws_error_code(exc) or str(exc),
        }

    storage_class = str(head_response.get("StorageClass") or "STANDARD").strip().upper()
    archive_status = str(head_response.get("ArchiveStatus") or "").strip().upper()
    restore_state = _s3_restore_ongoing(head_response.get("Restore"))
    base_item = {
        **base_item,
        "storageClass": storage_class,
        "archiveStatus": archive_status,
    }

    if restore_state is False:
        return {**base_item, "status": "already_restored", "message": "S3 복원 완료 상태"}
    if restore_state is True:
        return {**base_item, "status": "already_in_progress", "message": "S3 복원 진행 중"}
    if not _is_s3_archive_restore_needed(head_response):
        return {**base_item, "status": "not_needed", "message": "장기보관 복원 불필요"}

    # S3 보관 복원은 비동기 작업이라 요청 성공 이후에도 재생 가능 상태까지 시간이 걸린다.
    try:
        s3_client.restore_object(
            Bucket=bucket,
            Key=key,
            RestoreRequest={
                "Days": _S3_RESTORE_DAYS,
                "GlacierJobParameters": {"Tier": _S3_RESTORE_TIER},
            },
        )
    except Exception as exc:
        if _aws_error_code(exc) == "RestoreAlreadyInProgress":
            return {**base_item, "status": "already_in_progress", "message": "S3 복원 진행 중"}
        return {
            **base_item,
            "status": "failed",
            "message": _aws_error_code(exc) or str(exc),
        }

    return {**base_item, "status": "requested", "message": "S3 장기보관 복원 요청됨"}


def _recording_s3_restore_target(
    *,
    row: dict[str, Any],
    candidate: dict[str, Any],
) -> tuple[str, str] | None:
    bucket = str(row.get("s3Bucket") or s.S3_ULTRASOUND_BUCKET or "").strip()
    key = str(candidate.get("expectedS3FileKey") or row.get("s3FileKey") or "").strip()
    if not bucket or not key:
        return None
    return bucket, key


def _request_s3_archive_restores_for_recordings(
    *,
    rows_by_seq: dict[int, dict[str, Any]],
    candidates_by_seq: dict[int, dict[str, Any]],
    restored_seqs: list[int],
) -> list[dict[str, Any]]:
    targets: list[tuple[int, str, str]] = []
    for seq in restored_seqs:
        row = rows_by_seq.get(seq)
        candidate = candidates_by_seq.get(seq)
        if not row or not candidate:
            continue
        target = _recording_s3_restore_target(row=row, candidate=candidate)
        if target is None:
            continue
        bucket, key = target
        targets.append((seq, bucket, key))
    if not targets:
        return []

    try:
        s3_client = _build_s3_client()
    except Exception as exc:
        message = _aws_error_code(exc) or str(exc)
        return [
            {
                "seq": seq,
                "bucket": bucket,
                "key": key,
                "storageClass": "",
                "archiveStatus": "",
                "status": "failed",
                "message": message,
            }
            for seq, bucket, key in targets
        ]

    return [
        _request_s3_archive_restore(
            s3_client=s3_client,
            recording_seq=seq,
            bucket=bucket,
            key=key,
        )
        for seq, bucket, key in targets
    ]


def _group_recording_rows_by_hospital(rows: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    groups: dict[int, dict[str, Any]] = {}
    for row in rows:
        hospital_seq = _row_hospital_seq(row)
        recording_seq = _row_seq(row)
        if hospital_seq is None or recording_seq is None:
            continue
        if hospital_seq not in groups:
            groups[hospital_seq] = {
                "hospitalSeq": hospital_seq,
                "hospitalName": _row_hospital_name(row),
                "rows": [],
                "recordingSeqs": set(),
            }
        groups[hospital_seq]["rows"].append(row)
        groups[hospital_seq]["recordingSeqs"].add(recording_seq)
    return groups


def _build_streaming_restore_reason(
    barcode: str,
    hospital_seq: int,
    *,
    target_year: int,
    target_month: int,
    requester: str,
    requester_name: str | None = None,
) -> str:
    normalized_requester_name = str(requester_name or "").strip()
    requester_segment = f"requester={requester}"
    if normalized_requester_name:
        requester_segment = f"{requester_segment} / requesterName={normalized_requester_name}"

    return (
        f"Boxer 요청: {barcode} {target_year}-{target_month:02d} "
        f"스트리밍 종료 영상 복원 / hospitalSeq={hospital_seq} / {requester_segment}"
    )


def _restore_streaming_stopped_recordings_by_barcode_month(
    barcode: str,
    *,
    requested_year: int,
    requested_month: int,
    requester: str,
    requester_name: str | None = None,
) -> RecordingStreamingRestoreResult:
    normalized_barcode = str(barcode or "").strip()
    target_year, target_rows = _load_recording_streaming_restore_targets(
        normalized_barcode,
        requested_year=requested_year,
        requested_month=requested_month,
    )
    hospital_groups = _group_recording_rows_by_hospital(target_rows)

    if not hospital_groups:
        raise ValueError(
            "대상 recordings row에 hospitalSeq가 없어 MDA 복원 대상을 확정할 수 없어"
        )

    hospital_summaries: list[_StreamingRestoreHospitalSummary] = []
    total_mda_candidate_count = 0
    total_restorable_count = 0
    total_requested_count = 0
    total_restored_count = 0
    total_failed_count = 0
    failed_items: list[dict[str, Any]] = []
    s3_restore_items: list[dict[str, Any]] = []
    messages: list[str] = []

    for hospital_seq, group in sorted(hospital_groups.items()):
        target_recording_seqs = set(group["recordingSeqs"])
        rows_by_seq = {
            seq: row
            for row in group["rows"]
            if (seq := _row_seq(row)) in target_recording_seqs
        }
        candidates = _get_mda_stopped_recording_restore_candidates(normalized_barcode, hospital_seq)
        candidate_by_seq = {
            seq: candidate
            for candidate in candidates
            if (seq := _candidate_seq(candidate)) in target_recording_seqs
        }
        scoped_candidates = list(candidate_by_seq.values())
        restorable_seqs = [
            seq
            for seq, candidate in sorted(candidate_by_seq.items())
            if bool(candidate.get("restorable"))
        ]

        total_mda_candidate_count += len(scoped_candidates)
        total_restorable_count += len(restorable_seqs)
        hospital_summaries.append(
            _StreamingRestoreHospitalSummary(
                hospital_seq=hospital_seq,
                hospital_name=str(group.get("hospitalName") or "미확인"),
                db_target_count=len(group["rows"]),
                mda_candidate_count=len(scoped_candidates),
                restorable_count=len(restorable_seqs),
            )
        )

        if not restorable_seqs:
            continue

        # DB에서 확정한 대상 월의 recording seq만 MDA 복원 mutation에 넘긴다.
        mda_result = _restore_mda_stopped_recordings(
            barcode=normalized_barcode,
            hospital_seq=hospital_seq,
            recording_seqs=restorable_seqs,
            reason=_build_streaming_restore_reason(
                normalized_barcode,
                hospital_seq,
                target_year=target_year,
                target_month=requested_month,
                requester=requester,
                requester_name=requester_name,
            ),
        )
        message = str(mda_result.get("message") or "").strip()
        mda_status = bool(mda_result.get("status"))
        requested_count = int(mda_result.get("requestedCount") or len(restorable_seqs))
        restored_count = int(mda_result.get("restoredCount") or 0)
        failed_count = int(mda_result.get("failedCount") or 0)
        failed_item_seqs = {
            seq
            for item in mda_result.get("failedItems") or []
            if (seq := _candidate_seq(item)) is not None
        }

        total_requested_count += requested_count
        if mda_status:
            total_restored_count += restored_count
            total_failed_count += failed_count
            # MDA 응답에는 성공 seq 목록이 없으므로 failedItems를 제외하고 restoredCount까지만 S3 복원 대상으로 본다.
            restored_seqs = [seq for seq in restorable_seqs if seq not in failed_item_seqs][
                : max(restored_count, 0)
            ]
            s3_restore_items.extend(
                _request_s3_archive_restores_for_recordings(
                    rows_by_seq=rows_by_seq,
                    candidates_by_seq=candidate_by_seq,
                    restored_seqs=restored_seqs,
                )
            )
        else:
            # MDA가 transaction 중 예외를 반환하면 restoredCount가 남아도 실제 반영은 롤백될 수 있다.
            # status=false일 때는 성공 카운트를 믿지 않고 요청 건을 실패로 집계한다.
            fallback_failed_count = requested_count or len(restorable_seqs)
            total_failed_count += fallback_failed_count
            if not mda_result.get("failedItems"):
                for seq in restorable_seqs:
                    failed_items.append(
                        {
                            "seq": seq,
                            "fileId": "",
                            "reason": message or "MDA 복원 status=false",
                            "hospitalSeq": hospital_seq,
                            "hospitalName": group.get("hospitalName") or "미확인",
                        }
                    )
        if message:
            messages.append(f"#{hospital_seq}: {message}")
        for item in mda_result.get("failedItems") or []:
            failed_item = dict(item)
            failed_item["hospitalSeq"] = hospital_seq
            failed_item["hospitalName"] = group.get("hospitalName") or "미확인"
            failed_items.append(failed_item)

    return RecordingStreamingRestoreResult(
        barcode=normalized_barcode,
        target_year=target_year,
        target_month=requested_month,
        db_target_count=len(target_rows),
        mda_candidate_count=total_mda_candidate_count,
        restorable_count=total_restorable_count,
        requested_count=total_requested_count,
        restored_count=total_restored_count,
        failed_count=total_failed_count,
        message=" / ".join(messages),
        failed_items=failed_items,
        hospitals=hospital_summaries,
        s3_restore_requested_count=sum(
            1 for item in s3_restore_items if item.get("status") == "requested"
        ),
        s3_restore_already_in_progress_count=sum(
            1 for item in s3_restore_items if item.get("status") == "already_in_progress"
        ),
        s3_restore_already_restored_count=sum(
            1 for item in s3_restore_items if item.get("status") == "already_restored"
        ),
        s3_restore_failed_count=sum(
            1 for item in s3_restore_items if item.get("status") == "failed"
        ),
        s3_restore_items=s3_restore_items,
    )


def _format_recording_streaming_restore_result(result: RecordingStreamingRestoreResult) -> str:
    target_month_label = f"{result.target_year}-{result.target_month:02d}"
    if result.restored_count > 0 and result.failed_count > 0:
        status_line = (
            f"• 결과: *일부 복원* "
            f"(성공 `{result.restored_count}개`, 실패 `{result.failed_count}개`)"
        )
    elif result.restored_count > 0:
        status_line = f"• 결과: *복원 완료* (`{result.restored_count}개`)"
    elif result.failed_count > 0:
        status_line = f"• 결과: *복원 실패* (`{result.failed_count}개`)"
    elif result.restorable_count <= 0:
        status_line = "• 결과: 복원 가능한 영상이 없어"
    else:
        status_line = "• 결과: MDA 복원이 완료되지 않았어"

    lines = [
        "*스트리밍 종료 영상 복원 결과*",
        f"• 바코드: `{result.barcode}`",
        f"• 대상 월(KST): `{target_month_label}`",
        f"• DB 대상 recordings: `{result.db_target_count}개`",
        status_line,
    ]
    if result.hospitals:
        lines.append("• 병원별 대상:")
        for hospital in result.hospitals[:5]:
            lines.append(
                f"  - `{hospital.hospital_name}` `#{hospital.hospital_seq}` | "
                f"대상 `{hospital.db_target_count}개`, "
                f"복원 가능 `{hospital.restorable_count}개`"
            )
    if result.message:
        lines.append(f"• MDA 메시지: `{result.message}`")
    s3_visible_items = [
        item
        for item in result.s3_restore_items
        if item.get("status") and item.get("status") != "not_needed"
    ]
    if s3_visible_items:
        lines.append(
            "• S3 장기보관 복원: "
            f"요청 `{result.s3_restore_requested_count}개`, "
            f"진행 중 `{result.s3_restore_already_in_progress_count}개`, "
            f"이미 복원됨 `{result.s3_restore_already_restored_count}개`, "
            f"실패 `{result.s3_restore_failed_count}개`"
        )
        if result.s3_restore_requested_count or result.s3_restore_already_in_progress_count:
            lines.append("• 안내: S3 복원은 비동기라 완료 전까지 재생이 안 될 수 있어")
        if result.s3_restore_failed_count:
            lines.append("• S3 복원 실패 항목:")
            failed_s3_items = [
                entry for entry in s3_visible_items if entry.get("status") == "failed"
            ]
            for item in failed_s3_items[:5]:
                lines.append(
                    f"  - recordingSeq `{item.get('seq')}` | "
                    f"`{item.get('storageClass') or '미확인'}` | "
                    f"`{item.get('message') or '미확인'}`"
                )
    if result.failed_items:
        lines.append("• 실패 항목:")
        for item in result.failed_items[:5]:
            lines.append(
                f"  - hospitalSeq `{item.get('hospitalSeq') or '미확인'}` | "
                f"recordingSeq `{item.get('seq')}` | "
                f"fileId `{item.get('fileId') or '미확인'}` | "
                f"`{item.get('reason') or '미확인'}`"
            )
    return "\n".join(lines)


def _query_recording_streaming_restore_by_barcode_month(
    barcode: str,
    question: str,
    *,
    requester: str,
    requester_name: str | None = None,
) -> str:
    requested_year, requested_month = _extract_recording_streaming_restore_month(question)
    result = _restore_streaming_stopped_recordings_by_barcode_month(
        barcode,
        requested_year=requested_year,
        requested_month=requested_month,
        requester=requester,
        requester_name=requester_name,
    )
    return _format_recording_streaming_restore_result(result)
