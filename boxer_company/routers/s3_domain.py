from typing import Any

from botocore.exceptions import ClientError

from boxer_company import settings as cs
from boxer.core.utils import _truncate_text
from boxer_company.utils import (
    _display_value,
    _format_datetime,
    _format_size,
)


def _fetch_s3_device_log_lines(
    s3_client: Any,
    device_name: str,
    log_date: str,
    tail_only: bool = True,
) -> dict[str, Any]:
    key = f"{device_name}/log-{log_date}.log"
    try:
        head_response = s3_client.head_object(Bucket=cs.S3_LOG_BUCKET, Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        is_get_only_missing = bool(
            code in {"403", "AccessDenied"}
            and cs.BOXER_COMPANY_API_S3_HEAD_403_AS_MISSING
        )
        if code in {"404", "NotFound", "NoSuchKey"} or is_get_only_missing:
            # GetObject-only API role은 없는 key의 HeadObject도 403을 받는다.
            # 이 모드는 API에서만 명시적으로 켜고, 배포 smoke에서 실제
            # 존재 key 접근을 먼저 확인해 권한 장애와 구분한다.
            return {
                "found": False,
                "device_name": device_name,
                "key": key,
                "content_length": 0,
                "lines": [],
                "lookup_status": (
                    "not_found_or_forbidden"
                    if is_get_only_missing
                    else "not_found"
                ),
            }
        raise

    content_length = int(head_response.get("ContentLength") or 0)
    tail_bytes = max(1024, cs.S3_LOG_TAIL_BYTES)
    use_range = tail_only and content_length > tail_bytes
    get_params: dict[str, Any] = {
        "Bucket": cs.S3_LOG_BUCKET,
        "Key": key,
    }
    if use_range:
        range_start = max(0, content_length - tail_bytes)
        get_params["Range"] = f"bytes={range_start}-{content_length - 1}"

    get_response = s3_client.get_object(**get_params)
    body = get_response["Body"].read()
    text = body.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if use_range and lines:
        lines = lines[1:]

    return {
        "found": True,
        "device_name": device_name,
        "key": key,
        "content_length": content_length,
        "lines": lines,
    }


def _query_s3_ultrasound_by_barcode(s3_client: Any, barcode: str) -> str:
    prefix = f"{barcode}/"
    continuation_token = None
    scanned_objects = 0
    reached_scan_limit = False
    video_objects: list[dict[str, Any]] = []
    image_objects: list[dict[str, Any]] = []

    while True:
        params: dict[str, Any] = {
            "Bucket": cs.S3_ULTRASOUND_BUCKET,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        response = s3_client.list_objects_v2(**params)
        contents = response.get("Contents") or []
        for obj in contents:
            scanned_objects += 1
            key = str(obj.get("Key") or "")
            lowered = key.lower()
            if lowered.endswith(".mp4"):
                video_objects.append(obj)
            elif lowered.endswith((".jpg", ".jpeg")):
                image_objects.append(obj)

            if scanned_objects >= max(1, cs.S3_QUERY_MAX_KEYS):
                reached_scan_limit = True
                break

        if reached_scan_limit:
            break
        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")
        if not continuation_token:
            break

    if scanned_objects == 0:
        return (
            f"S3 조회 결과가 없어. "
            f"버킷 `{cs.S3_ULTRASOUND_BUCKET}`에서 prefix `{prefix}`로 찾지 못했어"
        )

    def _last_modified_to_ts(obj: dict[str, Any]) -> float:
        value = obj.get("LastModified")
        if hasattr(value, "timestamp"):
            return float(value.timestamp())
        return 0.0

    video_objects.sort(key=_last_modified_to_ts, reverse=True)
    image_objects.sort(key=_last_modified_to_ts, reverse=True)

    display_limit = max(1, min(50, cs.S3_QUERY_MAX_ITEMS))
    lines = [
        "*S3 초음파 객체 조회 결과*",
        f"• 버킷: `{cs.S3_ULTRASOUND_BUCKET}`",
        f"• 바코드: `{barcode}`",
        f"• 영상(mp4): *{len(video_objects)}개*",
        f"• 이미지(jpg/jpeg): *{len(image_objects)}개*",
        f"• 스캔한 전체 객체 수: `{scanned_objects}`",
    ]
    if reached_scan_limit:
        lines.append(
            f"• 주의: 스캔 상한({max(1, cs.S3_QUERY_MAX_KEYS)}개)에 도달해서 집계가 일부 누락될 수 있어"
        )

    lines.append("")
    lines.append(f"*최근 영상 상위 {min(len(video_objects), display_limit)}개*")
    if not video_objects:
        lines.append("- 없음")
    else:
        for index, obj in enumerate(video_objects[:display_limit], start=1):
            key = _display_value(obj.get("Key"), default="unknown")
            size = _format_size(obj.get("Size"))
            modified = _format_datetime(obj.get("LastModified"))
            lines.append(f"{index}. `{key}` | `{size}` | `{modified}`")

    lines.append("")
    lines.append(f"*최근 이미지 상위 {min(len(image_objects), display_limit)}개*")
    if not image_objects:
        lines.append("- 없음")
    else:
        for index, obj in enumerate(image_objects[:display_limit], start=1):
            key = _display_value(obj.get("Key"), default="unknown")
            size = _format_size(obj.get("Size"))
            modified = _format_datetime(obj.get("LastModified"))
            lines.append(f"{index}. `{key}` | `{size}` | `{modified}`")

    return _truncate_text("\n".join(lines), cs.S3_QUERY_MAX_RESULT_CHARS)


def _query_s3_device_log(s3_client: Any, device_name: str, log_date: str) -> str:
    log_data = _fetch_s3_device_log_lines(s3_client, device_name, log_date)
    if not log_data["found"]:
        return f"S3 로그 파일을 찾지 못했어: `{log_data['key']}`"

    lines = log_data["lines"]
    if not lines:
        return (
            "*S3 로그 조회 결과*\n"
            f"• 버킷: `{cs.S3_LOG_BUCKET}`\n"
            f"• 파일: `{log_data['key']}`\n"
            "• 로그 내용이 비어 있어"
        )

    max_lines = max(1, min(500, cs.S3_LOG_TAIL_LINES))
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
    excerpt = _truncate_text("\n".join(lines), cs.S3_QUERY_MAX_RESULT_CHARS)

    response_lines = [
        "*S3 로그 조회 결과*",
        f"• 버킷: `{cs.S3_LOG_BUCKET}`",
        f"• 장비: `{device_name}`",
        f"• 파일: `{log_data['key']}`",
        f"• 파일 크기: `{_format_size(log_data['content_length'])}`",
        f"• 표시 범위: 최근 `{len(lines)}줄`",
        "```text",
        excerpt,
        "```",
    ]
    return "\n".join(response_lines)
