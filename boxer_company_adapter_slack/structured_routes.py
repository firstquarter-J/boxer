import logging
import re
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
    _is_barcode_all_recorded_dates_request,
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
    if _is_barcode_all_recorded_dates_request(question, barcode):
        # `전체`/`모든`을 병원명으로 오인하는 legacy parser보다 구체적인
        # 바코드 날짜 route를 우선해 뒤의 공통 API stage로 넘긴다.
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
    legacy_blocks = _build_legacy_weekly_summary_blocks(slack_text)
    if legacy_blocks is not None:
        # API 본문은 기존 formatter의 완전한 fallback text다. 같은 본문에서
        # Slack 전용 표현만 복원해 DB를 다시 조회하지 않고 legacy Block을 유지한다.
        context.reply(
            slack_text,
            mention_user=False,
            blocks=legacy_blocks,
        )
        return

    # section text 상한보다 여유 있게 줄 단위로 나눠 긴 병원 목록도
    # 알 수 없는 이전/이후 API 본문도 Block 계약 안에서 안전하게 보낸다.
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


def _build_legacy_weekly_summary_blocks(
    slack_text: str,
) -> list[dict[str, object]] | None:
    """공유 formatter 본문을 기존 주간 요약 Slack Block으로 되돌린다."""

    lines = str(slack_text or "").strip().splitlines()
    if len(lines) < 7 or lines[0] != "*주간 초음파 촬영 요약*":
        return None

    range_match = re.fullmatch(
        r"• 기준 주간: (?P<current>`[^`]+`) \| "
        r"비교 주간: (?P<previous>`[^`]+`)",
        lines[1],
    )
    sent_match = re.fullmatch(r"• 발송: (?P<sent>`[^`]+`)", lines[2])
    total_match = re.fullmatch(
        r"• 전체 row: (?P<total>`[^`]+`) \| "
        r"병원: (?P<hospitals>`[^`]+`)",
        lines[3],
    )
    previous_match = re.fullmatch(
        r"• 전주 대비: (?P<counts>`[^`]+`) "
        r"\((?P<delta>`[^`]+`), (?P<rate>`[^`]+`)\)",
        lines[4],
    )
    changes_match = re.fullmatch(
        r"• 변화 병원: 급증 (?P<surge>`[^`]+`) \| "
        r"급감 (?P<drop>`[^`]+`)",
        lines[5],
    )
    if not all(
        (
            range_match,
            sent_match,
            total_match,
            previous_match,
            changes_match,
        )
    ):
        return None
    assert range_match is not None
    assert sent_match is not None
    assert total_match is not None
    assert previous_match is not None
    assert changes_match is not None

    blocks: list[dict[str, object]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "주간 초음파 촬영 요약",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"기준 주간 {range_match.group('current')} | "
                        f"비교 주간 {range_match.group('previous')} | "
                        f"발송 {sent_match.group('sent')}"
                    ),
                }
            ],
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*전체 row*\n{total_match.group('total')}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*집계 병원*\n{total_match.group('hospitals')}",
                },
                {
                    "type": "mrkdwn",
                    "text": (
                        "*전주 대비*\n"
                        f"{previous_match.group('counts')}\n"
                        f"{previous_match.group('delta')} "
                        f"({previous_match.group('rate')})"
                    ),
                },
                {
                    "type": "mrkdwn",
                    "text": (
                        "*변화 병원*\n"
                        f"급증 {changes_match.group('surge')} | "
                        f"급감 {changes_match.group('drop')}"
                    ),
                },
            ],
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        "기준 주간은 `월요일 ~ 일요일`이고, 변화 기준은 "
                        "증감 `20개 이상` + 급증 `2배 이상` / 급감 `50% 이하`야"
                    ),
                }
            ],
        },
    ]

    tail = _trim_blank_weekly_lines(lines[6:])
    if len(tail) == 1 and tail[0].startswith("• 결과: "):
        blocks.extend(
            (
                {"type": "divider"},
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*결과*\n" + tail[0].removeprefix("• 결과: "),
                    },
                },
            )
        )
        return blocks

    top_index = _find_weekly_heading(tail, r"\*상위 병원 Top \d+\*")
    surge_index = _find_weekly_heading(tail, r"\*급증\*")
    drop_index = _find_weekly_heading(tail, r"\*급감\*")
    if not (0 <= top_index < surge_index < drop_index):
        return None

    _append_legacy_weekly_section(
        blocks,
        tail[top_index],
        tail[top_index + 1 : surge_index],
    )
    _append_legacy_weekly_section(
        blocks,
        tail[surge_index],
        tail[surge_index + 1 : drop_index],
    )
    _append_legacy_weekly_section(
        blocks,
        tail[drop_index],
        tail[drop_index + 1 :],
    )
    return blocks


def _find_weekly_heading(lines: list[str], pattern: str) -> int:
    return next(
        (
            index
            for index, line in enumerate(lines)
            if re.fullmatch(pattern, line)
        ),
        -1,
    )


def _trim_blank_weekly_lines(lines: list[str]) -> list[str]:
    start = 0
    end = len(lines)
    while start < end and not lines[start].strip():
        start += 1
    while end > start and not lines[end - 1].strip():
        end -= 1
    return lines[start:end]


def _append_legacy_weekly_section(
    blocks: list[dict[str, object]],
    heading: str,
    raw_lines: list[str],
) -> None:
    content_lines = _trim_blank_weekly_lines(raw_lines)
    reference = next(
        (
            line.removeprefix("• 참고: ")
            for line in content_lines
            if line.startswith("• 참고: ")
        ),
        "",
    )
    visible_lines = [
        ("없어" if line == "• 없어" else line)
        for line in content_lines
        if line and not line.startswith("• 참고: ")
    ]
    blocks.extend(
        (
            {"type": "divider"},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\n".join((heading, *visible_lines)),
                },
            },
        )
    )
    if reference:
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": reference,
                    }
                ],
            }
        )
