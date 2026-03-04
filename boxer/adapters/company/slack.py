import logging
from typing import Any

import pymysql
from anthropic import Anthropic
from botocore.exceptions import BotoCoreError, ClientError
from slack_bolt import App

from boxer.adapters.common.slack import MentionPayload, SlackReplyFn, create_slack_app
from boxer.company import settings as cs
from boxer.company.utils import _extract_barcode
from boxer.core import settings as s
from boxer.core.llm import _ask_claude, _ask_ollama
from boxer.core.thread_context import _build_model_input, _load_thread_context
from boxer.core.utils import _validate_tokens
from boxer.routers.company.app_user import _lookup_app_user_by_barcode, _should_lookup_barcode
from boxer.routers.company.barcode_log import (
    _analyze_barcode_log_errors,
    _analyze_barcode_log_scan_events,
    _extract_log_date,
    _is_barcode_log_analysis_request,
    _is_barcode_last_recorded_at_request,
    _is_barcode_video_recorded_on_date_request,
    _is_barcode_video_count_request,
    _is_error_focused_request,
    _is_scan_focused_request,
)
from boxer.routers.company.db_query import _extract_db_query, _format_db_query_result
from boxer.routers.company.box_db import (
    _query_last_recorded_at_by_barcode,
    _query_recordings_count_by_barcode,
    _query_recordings_on_date_by_barcode,
)
from boxer.routers.company.s3_domain import (
    _extract_s3_request,
    _query_s3_device_log,
    _query_s3_ultrasound_by_barcode,
)
from boxer.routers.common.db import _query_db, _validate_readonly_sql
from boxer.routers.common.s3 import _build_s3_client


def create_app() -> App:
    cs.apply_legacy_db_compat(s)
    _validate_tokens(include_llm=True, include_data_sources=True)
    claude_client = Anthropic(api_key=s.ANTHROPIC_API_KEY) if s.LLM_PROVIDER == "claude" else None
    s3_client: Any | None = None

    def _get_s3_client() -> Any:
        nonlocal s3_client
        if s3_client is None:
            s3_client = _build_s3_client()
        return s3_client

    def _handle_company_mention(
        payload: MentionPayload,
        reply: SlackReplyFn,
        client: Any,
        logger: logging.Logger,
    ) -> None:
        text = payload["text"]
        question = payload["question"]
        user_id = payload["user_id"]
        channel_id = payload["channel_id"]
        current_ts = payload["current_ts"]
        thread_ts = payload["thread_ts"]

        if "ping" in text:
            reply("pong-ec2")
            logger.info("Responded with pong-ec2 in thread_ts=%s", thread_ts)
            return

        try:
            s3_request = _extract_s3_request(question)
        except ValueError as exc:
            reply(f"S3 조회 요청 형식 오류: {exc}")
            return

        if s3_request is not None:
            if not s.S3_QUERY_ENABLED:
                reply("S3 조회 기능이 꺼져 있어. .env에서 S3_QUERY_ENABLED=true로 설정해줘")
                return

            try:
                client_s3 = _get_s3_client()
                if s3_request["kind"] == "ultrasound":
                    result_text = _query_s3_ultrasound_by_barcode(
                        client_s3,
                        s3_request["barcode"],
                    )
                    logger.info(
                        "Responded with s3 ultrasound result in thread_ts=%s barcode=%s",
                        thread_ts,
                        s3_request["barcode"],
                    )
                else:
                    result_text = _query_s3_device_log(
                        client_s3,
                        s3_request["device_name"],
                        s3_request["log_date"],
                    )
                    logger.info(
                        "Responded with s3 log result in thread_ts=%s device=%s date=%s",
                        thread_ts,
                        s3_request["device_name"],
                        s3_request["log_date"],
                    )
                reply(result_text)
            except (BotoCoreError, ClientError):
                logger.exception("S3 query failed")
                reply("S3 조회 중 오류가 발생했어. 버킷 권한/리전/키 경로를 확인해줘")
            except Exception:
                logger.exception("S3 query failed")
                reply("S3 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        db_query = _extract_db_query(question)
        barcode = _extract_barcode(question)

        if _is_barcode_log_analysis_request(question, barcode):
            if not s.S3_QUERY_ENABLED:
                reply("로그 분석 기능이 꺼져 있어. .env에서 S3_QUERY_ENABLED=true로 설정해줘")
                return

            if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
                reply("바코드 로그 분석을 위해 DB 접속 정보(DB_*)가 필요해")
                return

            try:
                log_date = _extract_log_date(question)
                analysis_mode = (
                    "error"
                    if _is_error_focused_request(question) and not _is_scan_focused_request(question)
                    else "scan"
                )
                if analysis_mode == "error":
                    result_text = _analyze_barcode_log_errors(
                        _get_s3_client(),
                        barcode or "",
                        log_date,
                    )
                else:
                    result_text = _analyze_barcode_log_scan_events(
                        _get_s3_client(),
                        barcode or "",
                        log_date,
                    )
                reply(result_text)
                logger.info(
                    "Responded with barcode log analysis in thread_ts=%s barcode=%s date=%s mode=%s",
                    thread_ts,
                    barcode,
                    log_date,
                    analysis_mode,
                )
            except ValueError as exc:
                reply(f"로그 분석 요청 형식 오류: {exc}")
            except (BotoCoreError, ClientError, pymysql.MySQLError):
                logger.exception("Barcode log analysis failed")
                reply("바코드 로그 분석 중 오류가 발생했어. DB 연결/S3 권한/로그 경로를 확인해줘")
            except Exception:
                logger.exception("Barcode log analysis failed")
                reply("바코드 로그 분석 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_barcode_video_count_request(question, barcode):
            try:
                count_result = _query_recordings_count_by_barcode(barcode or "")
                reply(count_result)
                logger.info(
                    "Responded with barcode video count in thread_ts=%s barcode=%s",
                    thread_ts,
                    barcode,
                )
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Barcode video count query failed")
                reply("영상 개수 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Barcode video count query failed")
                reply("영상 개수 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_barcode_last_recorded_at_request(question, barcode):
            try:
                result_text = _query_last_recorded_at_by_barcode(barcode or "")
                reply(result_text)
                logger.info(
                    "Responded with barcode last recordedAt in thread_ts=%s barcode=%s",
                    thread_ts,
                    barcode,
                )
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Barcode last recordedAt query failed")
                reply("마지막 녹화 날짜 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Barcode last recordedAt query failed")
                reply("마지막 녹화 날짜 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_barcode_video_recorded_on_date_request(question, barcode):
            try:
                target_date = _extract_log_date(question)
                result_text = _query_recordings_on_date_by_barcode(barcode or "", target_date)
                reply(result_text)
                logger.info(
                    "Responded with barcode recordedAt-on-date in thread_ts=%s barcode=%s date=%s",
                    thread_ts,
                    barcode,
                    target_date,
                )
            except ValueError as exc:
                reply(f"영상 날짜 조회 요청 형식 오류: {exc}")
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Barcode recordedAt-on-date query failed")
                reply("날짜별 녹화 여부 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Barcode recordedAt-on-date query failed")
                reply("날짜별 녹화 여부 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if barcode and _should_lookup_barcode(question, barcode):
            if user_id in cs.APP_USER_LOOKUP_ALLOWED_USER_IDS:
                try:
                    lookup_result = _lookup_app_user_by_barcode(barcode)
                    reply(lookup_result)
                    logger.info(
                        "Responded with barcode lookup in thread_ts=%s barcode=%s",
                        thread_ts,
                        barcode,
                    )
                except Exception:
                    logger.exception("Barcode lookup failed")
                    reply("바코드 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
                return
            if db_query is None:
                approval_text = "보안 책임자의 승인이 필요합니다."
                if cs.DD_USER_ID:
                    approval_text = f"보안 책임자 <@{cs.DD_USER_ID}> 의 승인이 필요합니다."
                reply(
                    approval_text,
                    mention_user=False,
                )
                logger.info(
                    "Rejected app-user barcode lookup for unauthorized user=%s barcode=%s",
                    user_id,
                    barcode,
                )
                return
            logger.info(
                "Skipped app-user barcode lookup for unauthorized user=%s barcode=%s",
                user_id,
                barcode,
            )

        if db_query is not None:
            if not s.DB_QUERY_ENABLED:
                reply("DB 조회 기능이 꺼져 있어. .env에서 DB_QUERY_ENABLED=true로 설정해줘")
                return

            try:
                safe_sql = _validate_readonly_sql(db_query)
                db_result = _query_db(safe_sql)
                reply(_format_db_query_result(db_result))
                logger.info("Responded with db query result in thread_ts=%s", thread_ts)
            except ValueError as exc:
                reply(f"DB 조회 요청 형식 오류: {exc}")
            except pymysql.MySQLError:
                logger.exception("DB query failed")
                reply("DB 조회 중 오류가 발생했어. 연결 정보와 네트워크 상태를 확인해줘")
            return

        if s.LLM_PROVIDER == "claude" and claude_client:
            if not question:
                reply("질문 내용을 같이 보내줘")
                return
            if not cs.HYUN_USER_ID:
                reply("Claude 질문 권한 사용자가 설정되지 않았어. HYUN_USER_ID를 설정해줘")
                logger.warning("HYUN_USER_ID is not configured")
                return
            if user_id != cs.HYUN_USER_ID:
                reply("Claude 질문은 현재 지정된 사용자만 사용할 수 있어")
                logger.info("Rejected claude call for user=%s", user_id)
                return
            try:
                thread_context = _load_thread_context(
                    client,
                    logger,
                    channel_id,
                    thread_ts,
                    current_ts,
                )
                model_input = _build_model_input(question, thread_context)
                answer = _ask_claude(claude_client, model_input, system_prompt=cs.SYSTEM_PROMPT)
                if not answer:
                    answer = "답변을 생성하지 못했어. 다시 질문해줘"
                reply(answer)
                logger.info("Responded with claude answer in thread_ts=%s", thread_ts)
            except Exception:
                logger.exception("Claude API call failed")
                reply("AI 응답 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if s.LLM_PROVIDER == "ollama":
            if not question:
                reply("질문 내용을 같이 보내줘")
                return
            try:
                thread_context = _load_thread_context(
                    client,
                    logger,
                    channel_id,
                    thread_ts,
                    current_ts,
                )
                model_input = _build_model_input(question, thread_context)
                answer = _ask_ollama(model_input, system_prompt=cs.SYSTEM_PROMPT)
                if not answer:
                    answer = "답변을 생성하지 못했어. 다시 질문해줘"
                reply(answer)
                logger.info("Responded with ollama answer in thread_ts=%s", thread_ts)
            except Exception:
                logger.exception("Ollama API call failed")
                reply("Ollama 응답 중 오류가 발생했어. 서버 연결 상태를 확인해줘")
            return

        reply("현재는 ping, s3 조회, db 조회 또는 LLM 질문에 응답해")

    return create_slack_app(_handle_company_mention)
