from __future__ import annotations

import pymysql
from botocore.exceptions import BotoCoreError, ClientError


def build_dependency_failure_message(
    action_label: str,
    exc: Exception,
) -> str:
    """외부 의존성 오류를 비밀값 없이 운영자가 구분 가능한 문구로 바꾼다."""
    base = f"{action_label} 중 오류가 발생했어."
    if isinstance(exc, pymysql.MySQLError):
        return f"{base} DB 연결 또는 조회에 실패했어"
    if isinstance(exc, ClientError):
        code = str(
            exc.response.get("Error", {}).get("Code", "")
        ).strip()
        if code in {
            "403",
            "AccessDenied",
            "InvalidAccessKeyId",
            "SignatureDoesNotMatch",
        }:
            return f"{base} S3 접근 권한을 확인해줘"
        return f"{base} S3 로그 접근에 실패했어"
    if isinstance(exc, BotoCoreError):
        return f"{base} S3 로그 접근에 실패했어"
    if isinstance(exc, RuntimeError):
        lowered = str(exc).lower()
        if any(token in lowered for token in ("db", "mysql", "read-only")):
            return f"{base} DB 연결 또는 조회에 실패했어"
        if any(
            token in lowered
            for token in ("s3", "bucket", "credential")
        ):
            return f"{base} S3 로그 접근에 실패했어"
    return f"{base} 잠시 후 다시 시도해줘"


__all__ = ["build_dependency_failure_message"]
