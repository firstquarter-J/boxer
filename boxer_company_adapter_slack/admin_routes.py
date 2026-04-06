import logging
from dataclasses import dataclass
from typing import Any, Callable

import pymysql
from botocore.exceptions import BotoCoreError, ClientError

from boxer_adapter_slack.common import (
    MentionPayload,
    SlackReplyFn,
    _merge_request_log_metadata,
    _set_request_log_route,
)
from boxer.core import settings as s
from boxer.retrieval.connectors.db import _query_db, _validate_readonly_sql
from boxer_company import settings as cs
from boxer_company.routers.db_query import _extract_db_query, _format_db_query_result
from boxer_company.routers.request_log_query import (
    _extract_request_log_query,
    _query_request_log_text,
)
from boxer_company.routers.s3_domain import (
    _extract_s3_request,
    _query_s3_device_log,
    _query_s3_ultrasound_by_barcode,
)


@dataclass(frozen=True)
class AdminRoutesContext:
    question: str
    payload: MentionPayload
    user_id: str | None
    thread_ts: str
    reply: SlackReplyFn
    logger: logging.Logger


@dataclass(frozen=True)
class AdminRoutesDeps:
    get_s3_client: Callable[[], Any]
    reply_with_retrieval_synthesis: Callable[..., None]


def _is_request_log_query_allowed(target_user_id: str | None) -> bool:
    if not cs.REQUEST_LOG_QUERY_ALLOWED_USER_IDS:
        return True
    return bool(target_user_id) and target_user_id in cs.REQUEST_LOG_QUERY_ALLOWED_USER_IDS


def _handle_admin_routes(
    context: AdminRoutesContext,
    deps: AdminRoutesDeps,
) -> bool:
    question = context.question

    try:
        s3_request = _extract_s3_request(question)
    except ValueError as exc:
        context.reply(f"S3 조회 요청 형식 오류: {exc}")
        return True

    if s3_request is not None:
        if not s.S3_QUERY_ENABLED:
            context.reply("S3 조회 기능이 꺼져 있어. .env에서 S3_QUERY_ENABLED=true로 설정해줘")
            return True

        try:
            client_s3 = deps.get_s3_client()
            if s3_request["kind"] == "ultrasound":
                result_text = _query_s3_ultrasound_by_barcode(
                    client_s3,
                    s3_request["barcode"],
                )
                evidence_payload = {
                    "route": "s3_ultrasound",
                    "source": "s3",
                    "request": {
                        "kind": "ultrasound",
                        "barcode": s3_request["barcode"],
                    },
                    "result": result_text,
                }
                deps.reply_with_retrieval_synthesis(
                    result_text,
                    evidence_payload,
                    route_name="s3 ultrasound result",
                )
            else:
                result_text = _query_s3_device_log(
                    client_s3,
                    s3_request["device_name"],
                    s3_request["log_date"],
                )
                evidence_payload = {
                    "route": "s3_device_log",
                    "source": "s3",
                    "request": {
                        "kind": "log",
                        "deviceName": s3_request["device_name"],
                        "logDate": s3_request["log_date"],
                    },
                    "result": result_text,
                }
                deps.reply_with_retrieval_synthesis(
                    result_text,
                    evidence_payload,
                    route_name="s3 log result",
                )
        except (BotoCoreError, ClientError):
            context.logger.exception("S3 query failed")
            context.reply("S3 조회 중 오류가 발생했어. 버킷 권한/리전/키 경로를 확인해줘")
        except Exception:
            context.logger.exception("S3 query failed")
            context.reply("S3 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    request_log_query = _extract_request_log_query(question)
    if request_log_query is not None:
        _set_request_log_route(
            context.payload,
            "request log query",
            route_mode=request_log_query.mode,
            requested_date=request_log_query.target_date,
            subject_type="request_log",
        )
        _merge_request_log_metadata(
            context.payload,
            queryMode=request_log_query.mode,
            queryScope=request_log_query.scope_label,
            queryLimit=request_log_query.limit,
        )
        if not s.REQUEST_LOG_SQLITE_ENABLED:
            context.reply("요청 로그 저장 기능이 꺼져 있어. .env에서 REQUEST_LOG_SQLITE_ENABLED=true로 설정해줘")
            return True
        if not _is_request_log_query_allowed(context.user_id):
            approval_text = "요청 로그 조회는 권한이 필요해"
            if cs.DD_USER_ID:
                approval_text = f"요청 로그 조회는 <@{cs.DD_USER_ID}> 승인이 필요해"
            context.reply(approval_text, mention_user=False)
            context.logger.info(
                "Rejected request log query for unauthorized user=%s mode=%s date=%s",
                context.user_id,
                request_log_query.mode,
                request_log_query.target_date,
            )
            return True
        try:
            result_text = _query_request_log_text(request_log_query)
            context.reply(result_text)
            context.logger.info(
                "Responded with request log query in thread_ts=%s user=%s mode=%s date=%s limit=%s",
                context.thread_ts,
                context.user_id,
                request_log_query.mode,
                request_log_query.target_date,
                request_log_query.limit,
            )
        except Exception:
            context.logger.exception("Request log query failed")
            context.reply("요청 로그 조회 중 오류가 발생했어. SQLite 파일과 권한 상태를 확인해줘")
        return True

    db_query = _extract_db_query(question)
    if db_query is not None:
        if not s.DB_QUERY_ENABLED:
            context.reply("DB 조회 기능이 꺼져 있어. .env에서 DB_QUERY_ENABLED=true로 설정해줘")
            return True

        try:
            safe_sql = _validate_readonly_sql(db_query)
            db_result = _query_db(safe_sql)
            formatted_result = _format_db_query_result(db_result)
            evidence_payload = {
                "route": "db_query",
                "source": "db",
                "request": {
                    "question": question,
                    "sql": safe_sql,
                },
                "dbResult": db_result,
                "formattedResult": formatted_result,
            }
            deps.reply_with_retrieval_synthesis(
                formatted_result,
                evidence_payload,
                route_name="db query result",
            )
        except ValueError as exc:
            context.reply(f"DB 조회 요청 형식 오류: {exc}")
        except pymysql.MySQLError:
            context.logger.exception("DB query failed")
            context.reply("DB 조회 중 오류가 발생했어. 연결 정보와 네트워크 상태를 확인해줘")
        return True

    return False
