import logging
from dataclasses import dataclass
from typing import Any, Callable

import pymysql

from boxer_adapter_slack.common import SlackReplyFn
from boxer_company import settings as cs
from boxer_company.routers.app_user import _lookup_app_user_by_barcode, _should_lookup_barcode
from boxer_company.routers.barcode_validation import (
    _is_barcode_validation_status_request,
    _query_barcode_validation_status,
)
from boxer_company.routers.barcode_log import (
    _extract_log_date,
    _extract_log_date_with_presence,
    _is_barcode_all_recorded_dates_request,
    _is_barcode_baby_ai_list_request,
    _is_baby_ai_list_request_without_barcode,
    _is_barcode_video_info_request,
    _is_barcode_last_recorded_at_request,
    _is_barcode_video_length_request,
    _is_barcode_video_list_request,
    _is_barcode_video_recorded_on_date_request,
    _is_barcode_video_count_request,
)
from boxer_company.routers.box_db import (
    _query_all_recorded_dates_by_barcode,
    _query_baby_ai_list_by_barcode,
    _query_last_recorded_at_by_barcode,
    _query_recordings_count_by_barcode,
    _query_recordings_detail_by_barcode,
    _query_recordings_length_by_barcode,
    _query_recordings_length_on_date_by_barcode,
    _query_recordings_list_by_barcode,
    _query_recordings_on_date_by_barcode,
)


@dataclass(frozen=True)
class BarcodeQueryRoutesContext:
    question: str
    barcode: str | None
    user_id: str | None
    thread_ts: str
    reply: SlackReplyFn
    logger: logging.Logger


@dataclass(frozen=True)
class BarcodeQueryRoutesDeps:
    get_recordings_context: Callable[[], dict[str, Any]]
    attach_recordings_context_to_evidence: Callable[[dict[str, Any], dict[str, Any]], None]
    reply_with_retrieval_synthesis: Callable[..., None]


def _handle_barcode_query_routes(
    context: BarcodeQueryRoutesContext,
    deps: BarcodeQueryRoutesDeps,
) -> bool:
    question = context.question
    barcode = context.barcode

    if _is_barcode_validation_status_request(question, barcode):
        try:
            result_text = _query_barcode_validation_status(barcode or "")
            context.reply(result_text)
            context.logger.info(
                "Responded with barcode validation status in thread_ts=%s barcode=%s",
                context.thread_ts,
                barcode,
            )
        except ValueError as exc:
            context.reply(f"바코드 유효성 검사 확인 요청 형식 오류: {exc}")
        except RuntimeError:
            context.logger.exception("Barcode validation status query failed")
            context.reply("바코드 유효성 검사 확인 중 오류가 발생했어. MDA 연결 상태를 확인해줘")
        except Exception:
            context.logger.exception("Barcode validation status query failed")
            context.reply("바코드 유효성 검사 확인 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_barcode_video_count_request(question, barcode):
        try:
            count_result = _query_recordings_count_by_barcode(
                barcode or "",
                recordings_context=deps.get_recordings_context(),
            )
            context.reply(count_result)
            context.logger.info(
                "Responded with barcode video count in thread_ts=%s barcode=%s",
                context.thread_ts,
                barcode,
            )
        except (pymysql.MySQLError, RuntimeError):
            context.logger.exception("Barcode video count query failed")
            context.reply("영상 개수 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
        except Exception:
            context.logger.exception("Barcode video count query failed")
            context.reply("영상 개수 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_baby_ai_list_request_without_barcode(question, barcode):
        context.reply("베이비매직 조회는 바코드가 필요해. 예: `12345678910 베이비매직 목록`")
        context.logger.info(
            "Responded with baby_ai barcode guidance in thread_ts=%s question=%s",
            context.thread_ts,
            question,
        )
        return True

    if _is_barcode_baby_ai_list_request(question, barcode):
        try:
            target_date, has_requested_date = _extract_log_date_with_presence(question)
            result_text = _query_baby_ai_list_by_barcode(
                barcode or "",
                target_date if has_requested_date else None,
            )
            context.reply(result_text)
            context.logger.info(
                "Responded with barcode baby_ai list in thread_ts=%s barcode=%s has_date=%s",
                context.thread_ts,
                barcode,
                has_requested_date,
            )
        except ValueError as exc:
            context.reply(f"베이비매직 목록 조회 요청 형식 오류: {exc}")
        except (pymysql.MySQLError, RuntimeError):
            context.logger.exception("Barcode baby_ai list query failed")
            context.reply("베이비매직 목록 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
        except Exception:
            context.logger.exception("Barcode baby_ai list query failed")
            context.reply("베이비매직 목록 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_barcode_video_info_request(question, barcode):
        try:
            result_text = _query_recordings_detail_by_barcode(
                barcode or "",
                recordings_context=deps.get_recordings_context(),
            )
            context.reply(result_text)
            context.logger.info(
                "Responded with barcode video detail in thread_ts=%s barcode=%s",
                context.thread_ts,
                barcode,
            )
        except (pymysql.MySQLError, RuntimeError):
            context.logger.exception("Barcode video detail query failed")
            context.reply("영상 정보 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
        except Exception:
            context.logger.exception("Barcode video detail query failed")
            context.reply("영상 정보 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_barcode_video_list_request(question, barcode):
        try:
            result_text = _query_recordings_list_by_barcode(
                barcode or "",
                recordings_context=deps.get_recordings_context(),
            )
            context.reply(result_text)
            context.logger.info(
                "Responded with barcode video list in thread_ts=%s barcode=%s",
                context.thread_ts,
                barcode,
            )
        except (pymysql.MySQLError, RuntimeError):
            context.logger.exception("Barcode video list query failed")
            context.reply("영상 목록 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
        except Exception:
            context.logger.exception("Barcode video list query failed")
            context.reply("영상 목록 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_barcode_video_length_request(question, barcode):
        try:
            recordings_context = deps.get_recordings_context()
            target_date, has_requested_date = _extract_log_date_with_presence(question)
            if has_requested_date:
                result_text = _query_recordings_length_on_date_by_barcode(
                    barcode or "",
                    target_date,
                    recordings_context=recordings_context,
                )
            else:
                result_text = _query_recordings_length_by_barcode(
                    barcode or "",
                    recordings_context=recordings_context,
                )
            context.reply(result_text)
            context.logger.info(
                "Responded with barcode video length in thread_ts=%s barcode=%s has_date=%s",
                context.thread_ts,
                barcode,
                has_requested_date,
            )
        except ValueError as exc:
            context.reply(f"영상 길이 조회 요청 형식 오류: {exc}")
        except (pymysql.MySQLError, RuntimeError):
            context.logger.exception("Barcode video length query failed")
            context.reply("영상 길이 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
        except Exception:
            context.logger.exception("Barcode video length query failed")
            context.reply("영상 길이 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_barcode_all_recorded_dates_request(question, barcode):
        try:
            result_text = _query_all_recorded_dates_by_barcode(
                barcode or "",
                recordings_context=deps.get_recordings_context(),
            )
            context.reply(result_text)
            context.logger.info(
                "Responded with barcode all recorded dates in thread_ts=%s barcode=%s",
                context.thread_ts,
                barcode,
            )
        except (pymysql.MySQLError, RuntimeError):
            context.logger.exception("Barcode all recorded dates query failed")
            context.reply("전체 녹화 날짜 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
        except Exception:
            context.logger.exception("Barcode all recorded dates query failed")
            context.reply("전체 녹화 날짜 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_barcode_last_recorded_at_request(question, barcode):
        try:
            result_text = _query_last_recorded_at_by_barcode(
                barcode or "",
                recordings_context=deps.get_recordings_context(),
            )
            recordings_context = deps.get_recordings_context()
            evidence_payload = {
                "route": "barcode_last_recorded_at",
                "source": "box_db.recordings",
                "request": {
                    "barcode": barcode,
                    "question": question,
                },
                "queryResult": result_text,
            }
            deps.attach_recordings_context_to_evidence(evidence_payload, recordings_context)
            deps.reply_with_retrieval_synthesis(
                result_text,
                evidence_payload,
                route_name="barcode last recordedAt",
            )
        except (pymysql.MySQLError, RuntimeError):
            context.logger.exception("Barcode last recordedAt query failed")
            context.reply("마지막 녹화 날짜 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
        except Exception:
            context.logger.exception("Barcode last recordedAt query failed")
            context.reply("마지막 녹화 날짜 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_barcode_video_recorded_on_date_request(question, barcode):
        try:
            target_date = _extract_log_date(question)
            result_text = _query_recordings_on_date_by_barcode(
                barcode or "",
                target_date,
                recordings_context=deps.get_recordings_context(),
            )
            recordings_context = deps.get_recordings_context()
            evidence_payload = {
                "route": "barcode_recorded_on_date",
                "source": "box_db.recordings",
                "request": {
                    "barcode": barcode,
                    "question": question,
                    "targetDate": target_date,
                },
                "queryResult": result_text,
            }
            deps.attach_recordings_context_to_evidence(evidence_payload, recordings_context)
            deps.reply_with_retrieval_synthesis(
                result_text,
                evidence_payload,
                route_name="barcode recordedAt-on-date",
            )
        except ValueError as exc:
            context.reply(f"영상 날짜 조회 요청 형식 오류: {exc}")
        except (pymysql.MySQLError, RuntimeError):
            context.logger.exception("Barcode recordedAt-on-date query failed")
            context.reply("날짜별 녹화 여부 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
        except Exception:
            context.logger.exception("Barcode recordedAt-on-date query failed")
            context.reply("날짜별 녹화 여부 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if barcode and _should_lookup_barcode(question, barcode):
        if context.user_id in cs.APP_USER_LOOKUP_ALLOWED_USER_IDS:
            try:
                lookup_result = _lookup_app_user_by_barcode(barcode)
                context.reply(lookup_result)
                context.logger.info(
                    "Responded with barcode lookup in thread_ts=%s barcode=%s",
                    context.thread_ts,
                    barcode,
                )
            except Exception:
                context.logger.exception("Barcode lookup failed")
                context.reply("바코드 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return True
        approval_text = "보안 책임자의 승인이 필요합니다."
        if cs.DD_USER_ID:
            approval_text = f"보안 책임자 <@{cs.DD_USER_ID}> 의 승인이 필요합니다."
        context.reply(
            approval_text,
            mention_user=False,
        )
        context.logger.info(
            "Rejected app-user barcode lookup for unauthorized user=%s barcode=%s",
            context.user_id,
            barcode,
        )
        return True

    return False
