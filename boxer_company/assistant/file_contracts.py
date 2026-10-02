"""API와 Slack이 공유하는 provider-free 보고서 파일 검증·직렬화 계약."""

import base64
import binascii
import hashlib
import re
from typing import Any

from boxer_company.assistant.contracts import AssistantFile

MAX_REPORT_FILE_BYTES = 4 * 1024 * 1024
MAX_REPORT_BASE64_CHARS = 4 * ((MAX_REPORT_FILE_BYTES + 2) // 3)
# 본문 1MiB와 파일 4MiB의 base64·메타데이터를 별도 예산으로 보존한다.
MAX_FILE_RESPONSE_BYTES = 7 * 1024 * 1024
XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
_FILENAME = re.compile(r"recordings-trend-\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}\.xlsx")
_FILE_KEYS = frozenset({"filename", "mediaType", "deliveryScope", "sizeBytes", "sha256", "contentBase64"})


def validate_report_file(file: AssistantFile) -> None:
    # 외부 URL·파일 경로·채널 목적지는 파일 계약에 넣지 않는다.
    if (
        not isinstance(file.filename, str) or not _FILENAME.fullmatch(file.filename)
        or file.media_type != XLSX_MEDIA_TYPE or file.delivery_scope != "requester"
        or not isinstance(file.content, bytes) or not 4 <= len(file.content) <= MAX_REPORT_FILE_BYTES
        or not file.content.startswith(b"PK\x03\x04")
    ):
        raise ValueError("report file is invalid")


def serialize_report_files(files: tuple[AssistantFile, ...], *, route: str) -> list[dict[str, Any]]:
    if len(files) > 1 or (files and route != "weekly_recordings_summary"):
        raise ValueError("report file route is invalid")
    result = []
    for file in files:
        validate_report_file(file)
        result.append({
            "filename": file.filename, "mediaType": file.media_type, "deliveryScope": "requester",
            "sizeBytes": len(file.content), "sha256": hashlib.sha256(file.content).hexdigest(),
            "contentBase64": base64.b64encode(file.content).decode("ascii"),
        })
    return result


def deserialize_report_files(value: Any, *, route: str) -> tuple[AssistantFile, ...]:
    if not isinstance(value, list) or len(value) != 1 or route != "weekly_recordings_summary":
        raise ValueError("report files are invalid")
    item = value[0]
    if (
        not isinstance(item, dict) or frozenset(item) != _FILE_KEYS
        or type(item.get("sizeBytes")) is not int or not 4 <= item["sizeBytes"] <= MAX_REPORT_FILE_BYTES
        or not isinstance(item.get("contentBase64"), str)
        or len(item["contentBase64"]) > MAX_REPORT_BASE64_CHARS
        or not isinstance(item.get("sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", item["sha256"])
    ):
        raise ValueError("report file payload is invalid")
    try:
        content = base64.b64decode(item["contentBase64"], validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("report file encoding is invalid") from exc
    if len(content) != item["sizeBytes"] or hashlib.sha256(content).hexdigest() != item["sha256"]:
        raise ValueError("report file integrity is invalid")
    file = AssistantFile(filename=item["filename"], content=content,
                         media_type=item["mediaType"], delivery_scope=item["deliveryScope"])
    validate_report_file(file)
    return (file,)
