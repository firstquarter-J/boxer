import logging
from dataclasses import dataclass
from datetime import date

import pymysql

from boxer_adapter_slack.common import (
    MentionPayload,
    SlackReplyFn,
    _merge_request_log_metadata,
    _set_request_log_route,
)
from boxer_company.assistant import CompanyAssistantService
from boxer_company.assistant.barcode_query_route import (
    match_barcode_timeline_route,
)
from boxer_company_adapter_slack.assistant_bridge import (
    _commonmark_to_slack,
    assistant_slack_route_name,
    build_company_assistant_request,
    render_company_assistant_result,
)
from boxer_company_adapter_slack.notion_freeform import _is_generic_count_or_existence_request
from boxer_company_adapter_slack.weekly_reports import (
    _build_weekly_recordings_report_reply_payload,
    _extract_optional_requested_date,
    _is_weekly_recordings_report_request,
)
from boxer_company.routers.barcode_log import (
    _extract_capture_seq_filters,
    _extract_device_flag_filters,
    _extract_device_name_scope,
    _extract_device_seq_filter,
    _extract_device_status_filter,
    _extract_hospital_room_scope,
    _extract_leading_hospital_scope,
    _extract_year_filter,
    _is_devices_filter_query_request,
    _is_hospitals_filter_query_request,
    _is_hospital_rooms_filter_query_request,
    _is_recordings_filter_query_request,
    _is_ultrasound_capture_filter_query_request,
)
from boxer_company.routers.recording_streaming_restore import (
    _is_recording_streaming_restore_request,
)
from boxer_company.weekly_recordings_report import (
    _resolve_weekly_recordings_report_question_target_date,
)
from boxer_company.routers.box_db import (
    _query_devices_by_filters,
    _query_hospitals_by_filters,
    _query_hospital_rooms_by_filters,
    _query_recordings_by_filters,
    _query_ultrasound_captures_by_filters,
)


@dataclass(frozen=True)
class StructuredRoutesContext:
    question: str
    barcode: str | None
    payload: MentionPayload
    thread_ts: str
    reply: SlackReplyFn
    logger: logging.Logger
    assistant_service: CompanyAssistantService | None = None
    client: object | None = None


def _handle_structured_routes(context: StructuredRoutesContext) -> bool:
    question = context.question
    barcode = context.barcode
    assistant_request = build_company_assistant_request(
        context.payload,
        metadata={"barcode": barcode},
    )

    if context.assistant_service is not None:
        result = context.assistant_service.answer(assistant_request)
        if result is not None:
            route_name = assistant_slack_route_name(result.route)
            _set_request_log_route(
                context.payload,
                route_name,
                handler_type="router",
            )
            _merge_request_log_metadata(
                context.payload,
                assistantOutcome=result.outcome,
                assistantFallbackReason=result.fallback_reason,
                assistantUsedLlm=result.used_llm,
            )
            if result.route == "weekly_recordings_summary":
                # API는 집계와 CommonMark까지만 소유하고 Slack adapter가
                # 최종 Block transport를 만든다. scheduler/state는 기존
                # reporter에 그대로 남고 사용자 요청에만 이 경로를 쓴다.
                _reply_weekly_summary_result(context, result)
            else:
                render_company_assistant_result(
                    result,
                    reply=context.reply,
                    actor_id=context.payload.get("user_id"),
                    client=context.client,
                    logger=context.logger,
                )
            context.logger.info(
                "Responded with structured assistant route=%s outcome=%s thread_ts=%s",
                result.route,
                result.outcome,
                context.thread_ts,
            )
            return True

    if _is_recording_streaming_restore_request(question, barcode):
        # 복원 요청은 "영상 + 연월" 형태라 구조화 영상 조회가 먼저 잡기 쉬워서
        # 전용 MDA 복원 라우터까지 내려가게 한다.
        return False
    if match_barcode_timeline_route(assistant_request) is not None:
        # service가 None을 반환한 뒤 실행되는 legacy fallback도 같은
        # precedence를 지켜 timeline 질문을 barcode handler로 넘긴다.
        return False

    try:
        structured_target_date, _ = _extract_optional_requested_date(question)
    except ValueError as exc:
        structured_target_date = None
        structured_date_error = exc
    else:
        structured_date_error = None

    weekly_target_date = structured_target_date
    if structured_date_error is None:
        explicit_weekly_date = (
            date.fromisoformat(structured_target_date)
            if structured_target_date
            else None
        )
        resolved_weekly_date = (
            _resolve_weekly_recordings_report_question_target_date(
                question,
                explicit_target_date=explicit_weekly_date,
            )
        )
        weekly_target_date = (
            resolved_weekly_date.isoformat()
            if resolved_weekly_date is not None
            else None
        )

    structured_target_year = _extract_year_filter(question)
    if structured_target_year is not None and structured_target_date is None:
        structured_date_error = None
    structured_hospital_name, structured_room_name = _extract_hospital_room_scope(question)
    if not structured_hospital_name:
        structured_hospital_name = _extract_leading_hospital_scope(question)
    structured_hospital_seq, structured_hospital_room_seq = _extract_capture_seq_filters(question)
    structured_device_name = _extract_device_name_scope(question)
    structured_device_seq = _extract_device_seq_filter(question)
    structured_device_status = _extract_device_status_filter(question)
    structured_active_flag, structured_install_flag = _extract_device_flag_filters(question)

    if _is_hospitals_filter_query_request(
        question,
        target_date=structured_target_date,
        target_year=structured_target_year,
        hospital_name=structured_hospital_name,
        hospital_seq=structured_hospital_seq,
    ):
        try:
            if structured_date_error is not None:
                raise structured_date_error
            result_text = _query_hospitals_by_filters(
                hospital_name=structured_hospital_name,
                hospital_seq=structured_hospital_seq,
                target_date=structured_target_date,
                target_year=structured_target_year,
                count_only=_is_generic_count_or_existence_request(question),
            )
            context.reply(result_text)
            context.logger.info(
                "Responded with hospitals filters in thread_ts=%s date=%s year=%s hospital=%s hospitalSeq=%s",
                context.thread_ts,
                structured_target_date,
                structured_target_year,
                structured_hospital_name,
                structured_hospital_seq,
            )
        except ValueError as exc:
            context.reply(f"병원 조회 요청 형식 오류: {exc}")
        except (pymysql.MySQLError, RuntimeError):
            context.logger.exception("Hospitals filters query failed")
            context.reply("병원 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
        except Exception:
            context.logger.exception("Hospitals filters query failed")
            context.reply("병원 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_hospital_rooms_filter_query_request(
        question,
        hospital_name=structured_hospital_name,
        room_name=structured_room_name,
        hospital_seq=structured_hospital_seq,
        hospital_room_seq=structured_hospital_room_seq,
    ):
        try:
            result_text = _query_hospital_rooms_by_filters(
                hospital_name=structured_hospital_name,
                room_name=structured_room_name,
                hospital_seq=structured_hospital_seq,
                hospital_room_seq=structured_hospital_room_seq,
                count_only=_is_generic_count_or_existence_request(question),
            )
            context.reply(result_text)
            context.logger.info(
                "Responded with hospital rooms filters in thread_ts=%s hospital=%s room=%s hospitalSeq=%s hospitalRoomSeq=%s",
                context.thread_ts,
                structured_hospital_name,
                structured_room_name,
                structured_hospital_seq,
                structured_hospital_room_seq,
            )
        except ValueError as exc:
            context.reply(f"병실 조회 요청 형식 오류: {exc}")
        except (pymysql.MySQLError, RuntimeError):
            context.logger.exception("Hospital rooms filters query failed")
            context.reply("병실 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
        except Exception:
            context.logger.exception("Hospital rooms filters query failed")
            context.reply("병실 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_devices_filter_query_request(
        question,
        device_name=structured_device_name,
        device_seq=structured_device_seq,
        hospital_name=structured_hospital_name,
        room_name=structured_room_name,
        hospital_seq=structured_hospital_seq,
        hospital_room_seq=structured_hospital_room_seq,
        status=structured_device_status,
        active_flag=structured_active_flag,
        install_flag=structured_install_flag,
    ):
        try:
            result_text = _query_devices_by_filters(
                device_name=structured_device_name,
                device_seq=structured_device_seq,
                hospital_name=structured_hospital_name,
                room_name=structured_room_name,
                hospital_seq=structured_hospital_seq,
                hospital_room_seq=structured_hospital_room_seq,
                status=structured_device_status,
                active_flag=structured_active_flag,
                install_flag=structured_install_flag,
                count_only=_is_generic_count_or_existence_request(question),
            )
            context.reply(result_text)
            context.logger.info(
                "Responded with devices filters in thread_ts=%s deviceName=%s deviceSeq=%s hospital=%s room=%s hospitalSeq=%s hospitalRoomSeq=%s status=%s activeFlag=%s installFlag=%s",
                context.thread_ts,
                structured_device_name,
                structured_device_seq,
                structured_hospital_name,
                structured_room_name,
                structured_hospital_seq,
                structured_hospital_room_seq,
                structured_device_status,
                structured_active_flag,
                structured_install_flag,
            )
        except ValueError as exc:
            context.reply(f"장비 조회 요청 형식 오류: {exc}")
        except (pymysql.MySQLError, RuntimeError):
            context.logger.exception("Devices filters query failed")
            context.reply("장비 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
        except Exception:
            context.logger.exception("Devices filters query failed")
            context.reply("장비 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_weekly_recordings_report_request(
        question,
        barcode=barcode,
        target_date=weekly_target_date,
    ):
        try:
            if structured_date_error is not None:
                raise structured_date_error
            _set_request_log_route(
                context.payload,
                "weekly recordings report",
                route_mode="summary",
                handler_type="router",
                requested_date=weekly_target_date,
            )
            (
                result_text,
                result_blocks,
                resolved_week_start_date,
                resolved_week_end_date,
            ) = _build_weekly_recordings_report_reply_payload(
                target_date=weekly_target_date
            )
            if resolved_week_start_date:
                _set_request_log_route(
                    context.payload,
                    "weekly recordings report",
                    route_mode="summary",
                    handler_type="router",
                    requested_date=resolved_week_start_date,
                )
            context.reply(
                result_text,
                mention_user=False,
                blocks=result_blocks,
            )
            context.logger.info(
                "Responded with weekly recordings report in thread_ts=%s week_start=%s week_end=%s",
                context.thread_ts,
                resolved_week_start_date,
                resolved_week_end_date,
            )
        except ValueError as exc:
            context.reply(f"주간 영상 현황 요청 형식 오류: {exc}")
        except (pymysql.MySQLError, RuntimeError):
            context.logger.exception("Weekly recordings report query failed")
            context.reply("주간 영상 현황 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
        except Exception:
            context.logger.exception("Weekly recordings report query failed")
            context.reply("주간 영상 현황 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_ultrasound_capture_filter_query_request(
        question,
        barcode=barcode,
        target_date=structured_target_date,
        target_year=structured_target_year,
        hospital_name=structured_hospital_name,
        room_name=structured_room_name,
        hospital_seq=structured_hospital_seq,
        hospital_room_seq=structured_hospital_room_seq,
    ):
        try:
            if structured_date_error is not None:
                raise structured_date_error
            result_text = _query_ultrasound_captures_by_filters(
                barcode=barcode,
                target_date=structured_target_date,
                target_year=structured_target_year,
                hospital_name=structured_hospital_name,
                room_name=structured_room_name,
                hospital_seq=structured_hospital_seq,
                hospital_room_seq=structured_hospital_room_seq,
                count_only=_is_generic_count_or_existence_request(question),
            )
            context.reply(result_text)
            context.logger.info(
                "Responded with ultrasound capture filters in thread_ts=%s barcode=%s date=%s year=%s hospital=%s room=%s hospitalSeq=%s hospitalRoomSeq=%s",
                context.thread_ts,
                barcode,
                structured_target_date,
                structured_target_year,
                structured_hospital_name,
                structured_room_name,
                structured_hospital_seq,
                structured_hospital_room_seq,
            )
        except ValueError as exc:
            context.reply(f"캡처 조회 요청 형식 오류: {exc}")
        except (pymysql.MySQLError, RuntimeError):
            context.logger.exception("Ultrasound captures query failed")
            context.reply("캡처 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
        except Exception:
            context.logger.exception("Ultrasound captures query failed")
            context.reply("캡처 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    if _is_recordings_filter_query_request(
        question,
        barcode=barcode,
        target_date=structured_target_date,
        target_year=structured_target_year,
        hospital_name=structured_hospital_name,
        room_name=structured_room_name,
        hospital_seq=structured_hospital_seq,
        hospital_room_seq=structured_hospital_room_seq,
    ):
        try:
            if structured_date_error is not None:
                raise structured_date_error
            result_text = _query_recordings_by_filters(
                barcode=barcode,
                target_date=structured_target_date,
                target_year=structured_target_year,
                hospital_name=structured_hospital_name,
                room_name=structured_room_name,
                hospital_seq=structured_hospital_seq,
                hospital_room_seq=structured_hospital_room_seq,
                count_only=_is_generic_count_or_existence_request(question),
            )
            context.reply(result_text)
            context.logger.info(
                "Responded with recordings filters in thread_ts=%s barcode=%s date=%s year=%s hospital=%s room=%s hospitalSeq=%s hospitalRoomSeq=%s",
                context.thread_ts,
                barcode,
                structured_target_date,
                structured_target_year,
                structured_hospital_name,
                structured_room_name,
                structured_hospital_seq,
                structured_hospital_room_seq,
            )
        except ValueError as exc:
            context.reply(f"영상 조회 요청 형식 오류: {exc}")
        except (pymysql.MySQLError, RuntimeError):
            context.logger.exception("Recordings filters query failed")
            context.reply("영상 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
        except Exception:
            context.logger.exception("Recordings filters query failed")
            context.reply("영상 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True

    return False


def _reply_weekly_summary_result(
    context: StructuredRoutesContext,
    result: object,
) -> None:
    messages = getattr(result, "messages", ())
    body = "\n\n".join(
        str(message.body or "").strip()
        for message in messages
        if str(message.body or "").strip()
    )
    slack_text = _commonmark_to_slack(body)
    # section text 상한보다 여유 있게 줄 단위로 나눠 긴 병원 목록도
    # Block 계약 안에서 보낸다. fallback text는 전체 내용을 유지한다.
    blocks: list[dict[str, object]] = []
    chunk_lines: list[str] = []
    chunk_chars = 0
    for line in slack_text.splitlines() or [slack_text]:
        added = len(line) + (1 if chunk_lines else 0)
        if chunk_lines and chunk_chars + added > 2_800:
            blocks.append(
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "\n".join(chunk_lines),
                    },
                }
            )
            chunk_lines = []
            chunk_chars = 0
        chunk_lines.append(line[:2_800])
        chunk_chars += min(len(line), 2_800) + (
            1 if len(chunk_lines) > 1 else 0
        )
    if chunk_lines:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\n".join(chunk_lines),
                },
            }
        )
    context.reply(
        slack_text,
        mention_user=False,
        blocks=blocks,
    )
