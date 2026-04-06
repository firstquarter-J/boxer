import pymysql
from botocore.exceptions import BotoCoreError, ClientError


def _format_ping_llm_status(ok: bool | None) -> str:
    if ok is None:
        return "미설정"
    return "가능" if ok else "불가"


def _build_dependency_failure_reply(action_label: str, exc: Exception) -> str:
    base = f"{action_label} 중 오류가 발생했어."

    if isinstance(exc, pymysql.MySQLError):
        return f"{base} DB 연결 또는 조회에 실패했어"

    if isinstance(exc, ClientError):
        code = str(exc.response.get("Error", {}).get("Code", "")).strip()
        if code in {"403", "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch"}:
            return f"{base} S3 접근 권한을 확인해줘"
        return f"{base} S3 로그 접근에 실패했어"

    if isinstance(exc, BotoCoreError):
        return f"{base} S3 로그 접근에 실패했어"

    if isinstance(exc, RuntimeError):
        lowered = str(exc).lower()
        if any(token in lowered for token in ("db", "mysql", "read-only")):
            return f"{base} DB 연결 또는 조회에 실패했어"
        if any(token in lowered for token in ("s3", "bucket", "credential")):
            return f"{base} S3 로그 접근에 실패했어"

    return f"{base} 잠시 후 다시 시도해줘"
