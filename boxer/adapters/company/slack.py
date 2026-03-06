import logging
import re
from typing import Any

import pymysql
from anthropic import Anthropic
from botocore.exceptions import BotoCoreError, ClientError
from slack_bolt import App

from boxer.adapters.common.slack import MentionPayload, SlackReplyFn, create_slack_app
from boxer.adapters.company.fun import handle_fun_message
from boxer.company import settings as cs
from boxer.company.utils import _extract_barcode
from boxer.core import settings as s
from boxer.core.llm import _ask_claude, _ask_ollama_chat, _check_ollama_health
from boxer.core.retrieval_synthesis import _synthesize_retrieval_answer
from boxer.core.thread_context import _build_model_input, _load_thread_context
from boxer.core.utils import _validate_tokens
from boxer.routers.company.app_user import _lookup_app_user_by_barcode, _should_lookup_barcode
from boxer.routers.company.barcode_log import (
    _analyze_barcode_log_phase1_window,
    _analyze_barcode_log_errors,
    _analyze_barcode_log_scan_events,
    _build_phase2_scope_request_message,
    _extract_hospital_room_scope,
    _extract_log_date,
    _extract_log_date_with_presence,
    _is_barcode_all_recorded_dates_request,
    _is_barcode_video_info_request,
    _is_barcode_log_analysis_request,
    _is_barcode_last_recorded_at_request,
    _is_barcode_video_length_request,
    _is_barcode_video_list_request,
    _is_barcode_video_recorded_on_date_request,
    _is_barcode_video_count_request,
    _is_error_focused_request,
    _is_scan_focused_request,
)
from boxer.routers.company.db_query import _extract_db_query, _format_db_query_result
from boxer.routers.company.box_db import (
    _load_recordings_context_by_barcode,
    _lookup_device_contexts_by_hospital_room,
    _query_all_recorded_dates_by_barcode,
    _query_last_recorded_at_by_barcode,
    _query_recordings_count_by_barcode,
    _query_recordings_detail_by_barcode,
    _query_recordings_length_by_barcode,
    _query_recordings_length_on_date_by_barcode,
    _query_recordings_list_by_barcode,
    _query_recordings_on_date_by_barcode,
)
from boxer.routers.company.s3_domain import (
    _extract_s3_request,
    _query_s3_device_log,
    _query_s3_ultrasound_by_barcode,
)
from boxer.routers.common.db import _query_db, _validate_readonly_sql
from boxer.routers.common.s3 import _build_s3_client


def _split_barcode_log_reply(reply_text: str, max_chars: int = 3000) -> list[str]:
    text = (reply_text or "").strip()
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]

    def _extract_blocks(raw_text: str) -> list[str]:
        blocks: list[str] = []
        lines = raw_text.splitlines()
        index = 0
        while index < len(lines):
            while index < len(lines) and not lines[index].strip():
                index += 1
            if index >= len(lines):
                break

            if lines[index].strip() == "```":
                code_lines = [lines[index]]
                index += 1
                while index < len(lines):
                    code_lines.append(lines[index])
                    if lines[index].strip() == "```":
                        index += 1
                        break
                    index += 1
                blocks.append("\n".join(code_lines).strip())
                continue

            paragraph: list[str] = []
            while index < len(lines) and lines[index].strip() and lines[index].strip() != "```":
                paragraph.append(lines[index])
                index += 1
            blocks.append("\n".join(paragraph).strip())
        return [block for block in blocks if block]

    def _continuation_prefix(prefix: str) -> str:
        if "• error 라인:" in prefix:
            return "• error 라인 (계속)"
        if "• scanned 이벤트:" in prefix:
            return "• scanned 이벤트 (계속)"
        return ""

    def _split_lines_block(block: str, limit: int) -> list[str]:
        rows = block.splitlines()
        chunks: list[str] = []
        current_rows: list[str] = []
        for row in rows:
            candidate_rows = current_rows + [row]
            candidate = "\n".join(candidate_rows).strip()
            if current_rows and len(candidate) > limit:
                chunks.append("\n".join(current_rows).strip())
                current_rows = [row]
                continue
            current_rows = candidate_rows
        if current_rows:
            chunks.append("\n".join(current_rows).strip())
        return [chunk for chunk in chunks if chunk]

    def _render_fenced_chunk(prefix: str, code_lines: list[str]) -> str:
        fenced = "```\n" + "\n".join(code_lines) + "\n```"
        if prefix:
            return f"{prefix}\n\n{fenced}".strip()
        return fenced

    def _split_block(block: str, limit: int) -> list[str]:
        if len(block) <= limit:
            return [block]

        first_fence_index = block.find("```")
        last_fence_index = block.rfind("```")
        if first_fence_index != -1 and last_fence_index > first_fence_index:
            prefix = block[:first_fence_index].strip()
            code_body = block[first_fence_index + 3 : last_fence_index].strip("\n")
            code_lines = code_body.splitlines()
            if not code_lines:
                return [block]

            chunks: list[str] = []
            current_lines: list[str] = []
            current_prefix = prefix
            continuation = _continuation_prefix(prefix)

            for line in code_lines:
                candidate = _render_fenced_chunk(current_prefix, current_lines + [line])
                if current_lines and len(candidate) > limit:
                    chunks.append(_render_fenced_chunk(current_prefix, current_lines))
                    current_lines = [line]
                    current_prefix = continuation
                    continue
                current_lines.append(line)

            if current_lines:
                chunks.append(_render_fenced_chunk(current_prefix, current_lines))
            return chunks

        return _split_lines_block(block, limit)

    blocks = _extract_blocks(text)
    merged_blocks: list[str] = []
    index = 0
    while index < len(blocks):
        block = blocks[index]
        if index + 1 < len(blocks) and blocks[index + 1].startswith("```"):
            if "• scanned 이벤트:" in block or "• error 라인:" in block:
                merged_blocks.append(f"{block}\n\n{blocks[index + 1]}")
                index += 2
                continue
        merged_blocks.append(block)
        index += 1

    chunks: list[str] = []
    current = ""
    for block in merged_blocks:
        for piece in _split_block(block, max_chars):
            if not current:
                current = piece
                continue
            candidate = f"{current}\n\n{piece}"
            if len(candidate) <= max_chars:
                current = candidate
                continue
            chunks.append(current)
            current = piece

    if current:
        chunks.append(current)
    return chunks


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
            provider = (s.LLM_PROVIDER or "").lower().strip()
            if provider == "ollama":
                health = _check_ollama_health()
                reply(f"🏓 pong\n• llm: {health['summary']}")
                logger.info(
                    "Responded with ping health in thread_ts=%s provider=ollama ok=%s",
                    thread_ts,
                    health["ok"],
                )
                return
            if provider == "claude":
                reply("🏓 pong\n• llm: claude api 사용 중")
                logger.info("Responded with ping health in thread_ts=%s provider=claude", thread_ts)
                return

            reply("🏓 pong\n• llm: 미설정")
            logger.info("Responded with ping health in thread_ts=%s provider=none", thread_ts)
            return

        def _timeout_reply_text() -> str:
            timeout_sec = max(1, s.OLLAMA_TIMEOUT_SEC)
            return f"LLM 서버가 {timeout_sec}초 내 응답하지 않아 AI 답변 생성이 타임아웃됐어"

        def _llm_unavailable_reply_text(summary: str | None = None) -> str:
            base = "LLM 서버가 응답하지 않아 지금은 AI 답변을 생성할 수 없어"
            detail = (summary or "").strip()
            if not detail:
                return base
            return f"{base}\n• 상태: {detail}"

        def _is_timeout_error(exc: Exception) -> bool:
            lowered = str(exc).lower()
            return "timeout" in lowered or "timed out" in lowered

        def _contains_ymd(text_value: str) -> bool:
            return bool(re.search(r"\b\d{4}-\d{2}-\d{2}\b", text_value or ""))

        def _needs_barcode_log_fallback(
            synthesized: str,
            fallback_text: str,
            route_name: str,
        ) -> bool:
            if route_name != "barcode log analysis":
                return False

            normalized_synth = synthesized or ""
            normalized_fallback = fallback_text or ""
            required_labels = ("매핑 장비", "병원", "병실")
            required_bullets = ("• 바코드:", "• 날짜:", "• 매핑 장비:")

            if normalized_fallback.startswith("*바코드 로그") and not normalized_synth.startswith("*바코드 로그"):
                return True

            for bullet in required_bullets:
                if bullet in normalized_fallback and bullet not in normalized_synth:
                    return True

            for label in required_labels:
                if label in normalized_fallback and label not in normalized_synth:
                    return True

            if ("날짜" in normalized_fallback or _contains_ymd(normalized_fallback)) and (
                "날짜" not in normalized_synth and not _contains_ymd(normalized_synth)
            ):
                return True

            if "scanned 이벤트" in normalized_fallback:
                has_scan_lines = bool(re.search(r"\b\d{1,2}:\d{2}:\d{2}\b", normalized_synth))
                if "scanned 이벤트" not in normalized_synth and not has_scan_lines:
                    return True

            return False

        def _reply_with_retrieval_synthesis(
            fallback_text: str,
            evidence_payload: dict[str, Any],
            route_name: str,
        ) -> None:
            if route_name == "barcode log analysis":
                chunks = _split_barcode_log_reply(fallback_text)
                if not chunks:
                    reply(fallback_text)
                else:
                    for index, chunk in enumerate(chunks):
                        reply(chunk, mention_user=index == 0)
                logger.info(
                    "Responded with %s (direct, preserve format, chunks=%s)",
                    route_name,
                    max(1, len(chunks)),
                )
                return

            provider = (s.LLM_PROVIDER or "").lower().strip()
            if not s.LLM_SYNTHESIS_ENABLED or not question:
                reply(fallback_text)
                logger.info("Responded with %s (direct)", route_name)
                return
            if provider not in {"claude", "ollama"}:
                reply(fallback_text)
                logger.info("Responded with %s (direct, unsupported provider=%s)", route_name, provider)
                return
            if provider == "ollama":
                health = _check_ollama_health()
                if not health["ok"]:
                    reply(fallback_text)
                    logger.warning(
                        "Responded with %s (direct, ollama unavailable=%s)",
                        route_name,
                        health["summary"],
                    )
                    return
            if provider == "claude":
                if claude_client is None:
                    reply(fallback_text)
                    logger.info("Responded with %s (direct, claude client unavailable)", route_name)
                    return
                if not cs.HYUN_USER_ID or user_id != cs.HYUN_USER_ID:
                    reply(fallback_text)
                    logger.info(
                        "Responded with %s (direct, claude synthesis not allowed for user=%s)",
                        route_name,
                        user_id,
                    )
                    return

            try:
                thread_context = ""
                if s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT:
                    thread_context = _load_thread_context(
                        client,
                        logger,
                        channel_id,
                        thread_ts,
                        current_ts,
                    )
                synthesized_text = _synthesize_retrieval_answer(
                    question=question,
                    thread_context=thread_context,
                    evidence_payload=evidence_payload,
                    provider=provider,
                    claude_client=claude_client,
                    system_prompt=cs.SYSTEM_PROMPT or None,
                )
                final_text = synthesized_text or fallback_text
                if "다른 바코드" in final_text and "다른 바코드" not in fallback_text:
                    final_text = fallback_text
                if "다른 barcode" in final_text and "다른 barcode" not in fallback_text:
                    final_text = fallback_text
                if _needs_barcode_log_fallback(final_text, fallback_text, route_name):
                    final_text = fallback_text
                reply(final_text)
                logger.info(
                    "Responded with %s (%s) in thread_ts=%s",
                    route_name,
                    "synthesized" if synthesized_text else "direct_fallback",
                    thread_ts,
                )
            except TimeoutError:
                logger.warning("Retrieval synthesis timeout for route=%s", route_name)
                reply(_timeout_reply_text())
            except RuntimeError as exc:
                if _is_timeout_error(exc):
                    logger.warning("Retrieval synthesis timeout for route=%s", route_name)
                    reply(_timeout_reply_text())
                    return
                logger.exception("Retrieval synthesis failed for route=%s", route_name)
                reply(fallback_text)
            except Exception:
                logger.exception("Retrieval synthesis failed for route=%s", route_name)
                reply(fallback_text)

        def _build_barcode_log_error_summary_fallback(summary_payload: dict[str, Any]) -> str:
            summary = summary_payload.get("summary") if isinstance(summary_payload, dict) else None
            records = summary_payload.get("records") if isinstance(summary_payload, dict) else None
            if not isinstance(summary, dict) or not isinstance(records, list):
                return ""

            first_record = records[0] if records and isinstance(records[0], dict) else {}
            device_name = str(first_record.get("deviceName") or "미확인").strip() or "미확인"
            hospital_name = str(first_record.get("hospitalName") or "미확인").strip() or "미확인"
            room_name = str(first_record.get("roomName") or "미확인").strip() or "미확인"
            date_label = str(first_record.get("date") or summary_payload.get("request", {}).get("date") or "미확인").strip() or "미확인"

            restart_events = first_record.get("restartEvents") if isinstance(first_record, dict) else []
            restart_event = restart_events[0] if isinstance(restart_events, list) and restart_events else {}
            restart_time = str(restart_event.get("time") or "시간미상").strip() if isinstance(restart_event, dict) else "시간미상"
            first_session_start_time = str(first_record.get("firstSessionStartTime") or "미확인").strip() or "미확인"
            first_ffmpeg_error = first_record.get("firstFfmpegError") if isinstance(first_record, dict) else {}
            ffmpeg_error_time = (
                str(first_ffmpeg_error.get("timeLabel") or "시간미상").strip()
                if isinstance(first_ffmpeg_error, dict)
                else "시간미상"
            )
            ffmpeg_error_elapsed = (
                str(first_ffmpeg_error.get("elapsedFromSessionStart") or "").strip()
                if isinstance(first_ffmpeg_error, dict)
                else ""
            )

            error_groups = first_record.get("errorGroups") if isinstance(first_record, dict) else []
            top_group = error_groups[0] if isinstance(error_groups, list) and error_groups else {}
            top_component = str(top_group.get("component") or "미확인").strip() if isinstance(top_group, dict) else "미확인"
            top_signature = str(top_group.get("signature") or "미확인").strip() if isinstance(top_group, dict) else "미확인"
            top_count = int(top_group.get("count") or 0) if isinstance(top_group, dict) else 0
            top_signature_lower = top_signature.lower()
            top_component_lower = top_component.lower()
            is_ffmpeg_error = "ffmpeg" in top_signature_lower or "ffmpeg" in top_component_lower
            is_standby_ffmpeg_error = "standby error" in top_signature_lower
            is_ffmpeg_timestamp_error = any(
                token in top_signature_lower
                for token in ("invalid dropping", "non-monotonous dts", "dts ", "timestamp")
            )

            abnormal_count = int(summary.get("abnormalSessionCount") or 0)
            error_line_count = int(summary.get("errorLineCount") or 0)
            restart_count = int(summary.get("restartEventCount") or 0)
            all_closed_normally = bool((first_record.get("sessions") or {}).get("allClosedNormally"))

            if restart_count > 0:
                cause_line = "• 핵심 원인: 세션 중 장비 재시작과 녹화 오류가 함께 보여 정상 녹화 실패 가능성이 높아"
            elif is_standby_ffmpeg_error and all_closed_normally:
                cause_line = "• 핵심 원인: 초기 standby ffmpeg 오류가 있었지만 세션은 정상 종료돼 실제 영상 영향 확인이 필요해"
            elif is_ffmpeg_timestamp_error:
                cause_line = "• 핵심 원인: ffmpeg DTS/타임스탬프 이상이 확인돼 캡처보드 연결 불량 또는 캡처보드 고장을 우선 의심해"
            elif top_signature != "미확인" and top_count >= 2:
                cause_line = f"• 핵심 원인: `{top_component}`에서 `{top_signature}` 오류가 반복돼 녹화 실패 가능성이 높아"
            elif top_signature != "미확인" and top_count == 1:
                cause_line = f"• 핵심 원인: `{top_component}`에서 `{top_signature}` 오류가 1회 확인돼 녹화 영향 여부 점검이 필요해"
            else:
                cause_line = "• 핵심 원인: 로그상 주요 에러 1건이 확인돼 원인 점검이 필요해"

            lines = [
                "*에러 분석*",
                cause_line,
            ]
            if restart_count > 0:
                impact_line = (
                    f"• 영향: `{date_label}` `{hospital_name}` `{room_name}` 장비 `{device_name}`에서 "
                    "세션 중 재시작이 확인돼 정상 녹화 실패 가능성이 높아"
                )
            elif abnormal_count > 0:
                impact_line = (
                    f"• 영향: `{date_label}` `{hospital_name}` `{room_name}` 장비 `{device_name}`에서 "
                    "종료 스캔이 없는 세션이 있어 정상 녹화 실패 가능성이 높아"
                )
            elif is_standby_ffmpeg_error and all_closed_normally:
                impact_line = (
                    f"• 영향: `{date_label}` `{hospital_name}` `{room_name}` 장비 `{device_name}`에서 "
                    f"standby ffmpeg 오류 `{error_line_count}줄`이 있었지만 종료 스캔은 정상 확인돼 실제 영상 확인이 필요해"
                )
            elif is_ffmpeg_error and all_closed_normally:
                impact_line = (
                    f"• 영향: `{date_label}` `{hospital_name}` `{room_name}` 장비 `{device_name}`에서 "
                    f"종료 스캔은 확인됐지만 ffmpeg 오류 `{error_line_count}줄`이 있어 영상 손상 가능성이 높아"
                )
            else:
                impact_line = (
                    f"• 영향: `{date_label}` `{hospital_name}` `{room_name}` 장비 `{device_name}`에서 "
                    f"error 라인 `{error_line_count}줄`이 확인됐어"
                )
            lines.append(impact_line)

            evidence_lines: list[str] = []
            if restart_count > 0:
                evidence_lines.append(f"- `{restart_time}` 장비 재시작 감지 (`Mommybox Starting...`)")
            if is_ffmpeg_error and ffmpeg_error_time != "시간미상":
                time_parts: list[str] = [f"첫 ffmpeg 오류 `{ffmpeg_error_time}`"]
                if first_session_start_time != "미확인":
                    time_parts.append(f"세션 시작 `{first_session_start_time}`")
                if ffmpeg_error_elapsed:
                    time_parts.append(f"시작 후 `{ffmpeg_error_elapsed}`")
                evidence_lines.append(f"- {', '.join(time_parts)}")
            if top_count > 0 and top_signature != "미확인":
                evidence_lines.append(f"- `{top_component}` `{top_signature}` `{top_count}회`")
            if evidence_lines:
                lines.append("• 근거 로그:")
                lines.extend(evidence_lines)

            action_lines: list[str] = []
            if is_ffmpeg_timestamp_error:
                action_lines.append("- 캡처보드 케이블 체결 상태와 장치 교체 테스트를 가장 먼저 진행")
            elif is_ffmpeg_error:
                action_lines.append("- 캡처보드 연결 상태와 입력 신호를 가장 먼저 점검")
            if restart_count > 0:
                action_lines.append("- 전원 차단/전원 버튼 오입력 여부 확인")
            if top_signature != "미확인":
                action_lines.append(f"- `{top_component}` 관련 장치/프로세스 상태 확인")
            if is_ffmpeg_error:
                action_lines.append("- ffmpeg 프로세스 상태와 영상 입력 장치 점유 여부 확인")
            if "Device or resource busy" in top_signature:
                action_lines.append("- `/dev/video0` 점유 프로세스와 캡처보드 상태 확인")
            if not action_lines:
                action_lines.append("- 동일 시각 장비 상태와 관련 프로세스 로그 확인")
            lines.append("• 권장 조치:")
            lines.extend(action_lines[:3])
            lines.append(f"• 확실도: {'높음' if restart_count > 0 or top_count >= 2 else '중간'}")
            return "\n".join(lines)

        def _is_bad_barcode_log_error_summary(text: str) -> bool:
            normalized = (text or "").strip()
            if not normalized:
                return True

            required_markers = ("*에러 분석*", "• 핵심 원인:", "• 영향:", "• 권장 조치:")
            if any(marker not in normalized for marker in required_markers):
                return True

            lowered = normalized.lower()
            bad_patterns = (
                "</think>",
                "<think>",
                "let me",
                "wait,",
                "wait ",
                "i should",
                "the error",
                "the user",
                "now,",
                "now ",
                "therefore",
                "looking at",
                "based on",
                "i need",
                "check if",
            )
            if any(pattern in lowered for pattern in bad_patterns):
                return True

            return False

        def _reply_with_barcode_log_error_summary(summary_payload: dict[str, Any] | None) -> None:
            if not isinstance(summary_payload, dict):
                return

            summary = summary_payload.get("summary")
            if not isinstance(summary, dict):
                return

            error_line_count = int(summary.get("errorLineCount") or 0)
            if error_line_count <= 0:
                return

            provider = (s.LLM_PROVIDER or "").lower().strip()
            if not s.LLM_SYNTHESIS_ENABLED or provider not in {"claude", "ollama"}:
                return

            if provider == "ollama":
                health = _check_ollama_health()
                if not health["ok"]:
                    logger.warning(
                        "Skipped barcode log error summary synthesis because ollama is unavailable=%s",
                        health["summary"],
                    )
                    return
            if provider == "claude":
                if claude_client is None:
                    return
                if not cs.HYUN_USER_ID or user_id != cs.HYUN_USER_ID:
                    logger.info(
                        "Skipped barcode log error summary synthesis because claude is not allowed for user=%s",
                        user_id,
                    )
                    return

            try:
                thread_context = ""
                if s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT:
                    thread_context = _load_thread_context(
                        client,
                        logger,
                        channel_id,
                        thread_ts,
                        current_ts,
                    )
                synthesized_text = _synthesize_retrieval_answer(
                    question="위 바코드 로그의 에러를 운영 관점에서 분석해줘",
                    thread_context=thread_context,
                    evidence_payload=summary_payload,
                    provider=provider,
                    claude_client=claude_client,
                    system_prompt=cs.SYSTEM_PROMPT or None,
                    max_tokens=450,
                    ollama_timeout_sec=min(90, max(30, s.OLLAMA_TIMEOUT_SEC)),
                )
                final_text = (synthesized_text or "").strip()
                if _is_bad_barcode_log_error_summary(final_text):
                    logger.warning("Barcode log error summary synthesis returned empty text")
                    final_text = _build_barcode_log_error_summary_fallback(summary_payload)
                if not final_text:
                    return
                reply(final_text, mention_user=False)
                logger.info("Responded with barcode log error summary synthesis in thread_ts=%s", thread_ts)
            except TimeoutError:
                logger.warning("Barcode log error summary synthesis timed out")
                fallback_text = _build_barcode_log_error_summary_fallback(summary_payload)
                if fallback_text:
                    reply(fallback_text, mention_user=False)
            except RuntimeError as exc:
                if _is_timeout_error(exc):
                    logger.warning("Barcode log error summary synthesis timed out")
                    fallback_text = _build_barcode_log_error_summary_fallback(summary_payload)
                    if fallback_text:
                        reply(fallback_text, mention_user=False)
                    return
                logger.exception("Barcode log error summary synthesis failed")
                fallback_text = _build_barcode_log_error_summary_fallback(summary_payload)
                if fallback_text:
                    reply(fallback_text, mention_user=False)
            except Exception:
                logger.exception("Barcode log error summary synthesis failed")
                fallback_text = _build_barcode_log_error_summary_fallback(summary_payload)
                if fallback_text:
                    reply(fallback_text, mention_user=False)

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
                    evidence_payload = {
                        "route": "s3_ultrasound",
                        "source": "s3",
                        "request": {
                            "kind": "ultrasound",
                            "barcode": s3_request["barcode"],
                        },
                        "result": result_text,
                    }
                    _reply_with_retrieval_synthesis(
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
                    _reply_with_retrieval_synthesis(
                        result_text,
                        evidence_payload,
                        route_name="s3 log result",
                    )
            except (BotoCoreError, ClientError):
                logger.exception("S3 query failed")
                reply("S3 조회 중 오류가 발생했어. 버킷 권한/리전/키 경로를 확인해줘")
            except Exception:
                logger.exception("S3 query failed")
                reply("S3 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        db_query = _extract_db_query(question)
        barcode = _extract_barcode(question)
        recordings_context: dict[str, Any] | None = None
        recordings_context_prefetch_error: Exception | None = None

        if barcode:
            try:
                recordings_context = _load_recordings_context_by_barcode(barcode)
                prefetch_summary = recordings_context.get("summary") or {}
                logger.info(
                    "Prefetched recordings context in thread_ts=%s barcode=%s count=%s",
                    thread_ts,
                    barcode,
                    int(prefetch_summary.get("recordingCount") or 0),
                )
            except Exception as exc:
                recordings_context_prefetch_error = exc
                logger.warning(
                    "Failed to prefetch recordings context in thread_ts=%s barcode=%s error=%s",
                    thread_ts,
                    barcode,
                    type(exc).__name__,
                )

        def _get_recordings_context() -> dict[str, Any]:
            nonlocal recordings_context, recordings_context_prefetch_error
            if recordings_context is not None:
                return recordings_context
            if recordings_context_prefetch_error is not None:
                raise recordings_context_prefetch_error
            if not barcode:
                raise ValueError("바코드가 필요해")
            recordings_context = _load_recordings_context_by_barcode(barcode)
            return recordings_context

        def _build_recordings_rows_evidence(context: dict[str, Any]) -> list[dict[str, Any]]:
            rows = context.get("rows") or []
            return [
                {
                    "seq": row.get("seq"),
                    "hospitalSeq": row.get("hospitalSeq"),
                    "hospitalRoomSeq": row.get("hospitalRoomSeq"),
                    "hospitalName": row.get("hospitalName"),
                    "roomName": row.get("roomName"),
                    "deviceSeq": row.get("deviceSeq"),
                    "videoLength": row.get("videoLength"),
                    "streamingStatus": row.get("streamingStatus"),
                    "recordedAt": row.get("recordedAt"),
                    "createdAt": row.get("createdAt"),
                }
                for row in rows
            ]

        def _attach_recordings_context_to_evidence(
            evidence: dict[str, Any],
            context: dict[str, Any],
        ) -> None:
            evidence["recordingsSummary"] = context.get("summary")
            evidence["recordingsContextLimit"] = context.get("limit")
            evidence["recordingsHasMore"] = context.get("has_more")
            evidence["recordingsRows"] = _build_recordings_rows_evidence(context)

        def _has_recordings_device_mapping(context: dict[str, Any]) -> bool:
            rows = context.get("rows") or []
            return any(row.get("deviceSeq") is not None for row in rows)

        def _build_barcode_fallback_evidence() -> dict[str, Any] | None:
            if not barcode:
                return None

            evidence: dict[str, Any] = {
                "route": "llm_barcode_fallback",
                "source": "box_db.recordings",
                "request": {
                    "barcode": barcode,
                    "question": question,
                },
            }

            if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
                evidence["warning"] = "DB 접속 정보(DB_*)가 없어 recordings 컨텍스트를 넣지 못했어"
                return evidence

            try:
                context = _get_recordings_context()
            except Exception as exc:
                logger.exception("Failed to load recordings context for llm fallback barcode=%s", barcode)
                evidence["warning"] = f"recordings 컨텍스트 조회 실패: {type(exc).__name__}"
                return evidence

            _attach_recordings_context_to_evidence(evidence, context)
            return evidence

        if _is_barcode_log_analysis_request(question, barcode):
            if not s.S3_QUERY_ENABLED:
                reply("로그 분석 기능이 꺼져 있어. .env에서 S3_QUERY_ENABLED=true로 설정해줘")
                return

            if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
                reply("바코드 로그 분석을 위해 DB 접속 정보(DB_*)가 필요해")
                return

            try:
                log_date, has_requested_date = _extract_log_date_with_presence(question)
                analysis_mode = "phase1_window"
                context = _get_recordings_context()
                log_analysis_payload: dict[str, Any] | None = None
                summary = context.get("summary") or {}
                recording_count = int(summary.get("recordingCount") or 0)
                has_device_mapping = _has_recordings_device_mapping(context)
                phase2_hospital_name, phase2_room_name = _extract_hospital_room_scope(question)
                used_manual_scope = False

                if has_requested_date:
                    base_mode = (
                        "error"
                        if _is_error_focused_request(question) and not _is_scan_focused_request(question)
                        else "scan"
                    )

                    if recording_count <= 0 or not has_device_mapping:
                        if not phase2_hospital_name or not phase2_room_name:
                            analysis_mode = "scope_required"
                            result_text = _build_phase2_scope_request_message(
                                barcode or "",
                                "recordings 장비 매핑이 없어 2차 입력이 필요해",
                                "*바코드 로그 분석 결과 (2차 수동 범위)*",
                            )
                        else:
                            manual_device_contexts = _lookup_device_contexts_by_hospital_room(
                                phase2_hospital_name,
                                phase2_room_name,
                            )
                            if not manual_device_contexts:
                                analysis_mode = "scope_not_found"
                                result_text = _build_phase2_scope_request_message(
                                    barcode or "",
                                    "입력한 병원명/병실명으로 장비를 찾지 못했어. MDA 표시 이름과 정확히 일치하게 입력해줘",
                                    "*바코드 로그 분석 결과 (2차 수동 범위)*",
                                )
                            else:
                                used_manual_scope = True
                                analysis_mode = f"{base_mode}_manual_scope"
                                if base_mode == "error":
                                    result_text, log_analysis_payload = _analyze_barcode_log_errors(
                                        _get_s3_client(),
                                        barcode or "",
                                        log_date,
                                        recordings_context=context,
                                        device_contexts=manual_device_contexts,
                                    )
                                else:
                                    result_text, log_analysis_payload = _analyze_barcode_log_scan_events(
                                        _get_s3_client(),
                                        barcode or "",
                                        log_date,
                                        recordings_context=context,
                                        device_contexts=manual_device_contexts,
                                    )
                    else:
                        analysis_mode = base_mode
                        if base_mode == "error":
                            result_text, log_analysis_payload = _analyze_barcode_log_errors(
                                _get_s3_client(),
                                barcode or "",
                                log_date,
                                recordings_context=context,
                            )
                        else:
                            result_text, log_analysis_payload = _analyze_barcode_log_scan_events(
                                _get_s3_client(),
                                barcode or "",
                                log_date,
                                recordings_context=context,
                            )
                else:
                    result_text, log_analysis_payload = _analyze_barcode_log_phase1_window(
                        _get_s3_client(),
                        barcode or "",
                        recordings_context=context,
                        max_days=cs.LOG_PHASE1_MAX_DAYS,
                    )

                if analysis_mode in {"scope_required", "scope_not_found"}:
                    reply(result_text)
                    logger.info(
                        "Responded with barcode log scope guidance in thread_ts=%s barcode=%s mode=%s",
                        thread_ts,
                        barcode,
                        analysis_mode,
                    )
                    return

                evidence_payload = {
                    "route": "barcode_log_analysis",
                    "source": "box_db+s3",
                    "request": {
                        "barcode": barcode,
                        "date": log_date,
                        "hasRequestedDate": has_requested_date,
                        "mode": analysis_mode,
                        "phase1MaxDays": cs.LOG_PHASE1_MAX_DAYS,
                        "recordingsCount": recording_count,
                        "recordingsHasDeviceMapping": has_device_mapping,
                        "phase2HospitalName": phase2_hospital_name,
                        "phase2RoomName": phase2_room_name,
                        "usedManualScope": used_manual_scope,
                    },
                    "analysisResult": result_text,
                }
                if log_analysis_payload is not None:
                    evidence_payload["errorSummaryEvidence"] = log_analysis_payload
                _attach_recordings_context_to_evidence(evidence_payload, context)
                _reply_with_retrieval_synthesis(
                    result_text,
                    evidence_payload,
                    route_name="barcode log analysis",
                )
                _reply_with_barcode_log_error_summary(log_analysis_payload)
            except ValueError as exc:
                reply(f"로그 분석 요청 형식 오류: {exc}")
            except (BotoCoreError, ClientError, pymysql.MySQLError, RuntimeError):
                logger.exception("Barcode log analysis failed")
                reply("바코드 로그 분석 중 오류가 발생했어. DB 연결/S3 권한/로그 경로를 확인해줘")
            except Exception:
                logger.exception("Barcode log analysis failed")
                reply("바코드 로그 분석 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_barcode_video_count_request(question, barcode):
            try:
                count_result = _query_recordings_count_by_barcode(
                    barcode or "",
                    recordings_context=_get_recordings_context(),
                )
                reply(count_result)
                logger.info("Responded with barcode video count in thread_ts=%s barcode=%s", thread_ts, barcode)
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Barcode video count query failed")
                reply("영상 개수 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Barcode video count query failed")
                reply("영상 개수 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_barcode_video_info_request(question, barcode):
            try:
                result_text = _query_recordings_detail_by_barcode(
                    barcode or "",
                    recordings_context=_get_recordings_context(),
                )
                reply(result_text)
                logger.info("Responded with barcode video detail in thread_ts=%s barcode=%s", thread_ts, barcode)
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Barcode video detail query failed")
                reply("영상 정보 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Barcode video detail query failed")
                reply("영상 정보 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_barcode_video_list_request(question, barcode):
            try:
                result_text = _query_recordings_list_by_barcode(
                    barcode or "",
                    recordings_context=_get_recordings_context(),
                )
                reply(result_text)
                logger.info("Responded with barcode video list in thread_ts=%s barcode=%s", thread_ts, barcode)
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Barcode video list query failed")
                reply("영상 목록 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Barcode video list query failed")
                reply("영상 목록 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_barcode_video_length_request(question, barcode):
            try:
                context = _get_recordings_context()
                target_date, has_requested_date = _extract_log_date_with_presence(question)
                if has_requested_date:
                    result_text = _query_recordings_length_on_date_by_barcode(
                        barcode or "",
                        target_date,
                        recordings_context=context,
                    )
                else:
                    result_text = _query_recordings_length_by_barcode(
                        barcode or "",
                        recordings_context=context,
                    )
                reply(result_text)
                logger.info(
                    "Responded with barcode video length in thread_ts=%s barcode=%s has_date=%s",
                    thread_ts,
                    barcode,
                    has_requested_date,
                )
            except ValueError as exc:
                reply(f"영상 길이 조회 요청 형식 오류: {exc}")
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Barcode video length query failed")
                reply("영상 길이 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Barcode video length query failed")
                reply("영상 길이 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_barcode_all_recorded_dates_request(question, barcode):
            try:
                result_text = _query_all_recorded_dates_by_barcode(
                    barcode or "",
                    recordings_context=_get_recordings_context(),
                )
                reply(result_text)
                logger.info(
                    "Responded with barcode all recorded dates in thread_ts=%s barcode=%s",
                    thread_ts,
                    barcode,
                )
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Barcode all recorded dates query failed")
                reply("전체 녹화 날짜 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Barcode all recorded dates query failed")
                reply("전체 녹화 날짜 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_barcode_last_recorded_at_request(question, barcode):
            try:
                result_text = _query_last_recorded_at_by_barcode(
                    barcode or "",
                    recordings_context=_get_recordings_context(),
                )
                context = _get_recordings_context()
                evidence_payload = {
                    "route": "barcode_last_recorded_at",
                    "source": "box_db.recordings",
                    "request": {
                        "barcode": barcode,
                        "question": question,
                    },
                    "queryResult": result_text,
                }
                _attach_recordings_context_to_evidence(evidence_payload, context)
                _reply_with_retrieval_synthesis(
                    result_text,
                    evidence_payload,
                    route_name="barcode last recordedAt",
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
                result_text = _query_recordings_on_date_by_barcode(
                    barcode or "",
                    target_date,
                    recordings_context=_get_recordings_context(),
                )
                context = _get_recordings_context()
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
                _attach_recordings_context_to_evidence(evidence_payload, context)
                _reply_with_retrieval_synthesis(
                    result_text,
                    evidence_payload,
                    route_name="barcode recordedAt-on-date",
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
                _reply_with_retrieval_synthesis(
                    formatted_result,
                    evidence_payload,
                    route_name="db query result",
                )
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
                fallback_evidence = _build_barcode_fallback_evidence()
                if fallback_evidence is not None:
                    synthesis_thread_context = ""
                    if s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT:
                        synthesis_thread_context = _load_thread_context(
                            client,
                            logger,
                            channel_id,
                            thread_ts,
                            current_ts,
                        )
                    answer = _synthesize_retrieval_answer(
                        question=question,
                        thread_context=synthesis_thread_context,
                        evidence_payload=fallback_evidence,
                        provider="claude",
                        claude_client=claude_client,
                        system_prompt=cs.SYSTEM_PROMPT or None,
                    )
                    if answer:
                        reply(answer)
                        logger.info(
                            "Responded with claude answer using barcode evidence in thread_ts=%s barcode=%s",
                            thread_ts,
                            barcode,
                        )
                        return
                    logger.warning(
                        "Claude barcode evidence synthesis returned empty in thread_ts=%s barcode=%s",
                        thread_ts,
                        barcode,
                    )

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
            except TimeoutError:
                logger.warning("Claude API timeout")
                reply(_timeout_reply_text())
            except Exception:
                logger.exception("Claude API call failed")
                reply("AI 응답 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if s.LLM_PROVIDER == "ollama":
            if not question:
                reply("질문 내용을 같이 보내줘")
                return
            try:
                health = _check_ollama_health()
                if not health["ok"]:
                    logger.warning("Ollama unavailable before answer generation: %s", health["summary"])
                    reply(_llm_unavailable_reply_text(str(health["summary"])))
                    return
                fallback_evidence = _build_barcode_fallback_evidence()
                if fallback_evidence is not None:
                    synthesis_thread_context = ""
                    if s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT:
                        synthesis_thread_context = _load_thread_context(
                            client,
                            logger,
                            channel_id,
                            thread_ts,
                            current_ts,
                        )
                    answer = _synthesize_retrieval_answer(
                        question=question,
                        thread_context=synthesis_thread_context,
                        evidence_payload=fallback_evidence,
                        provider="ollama",
                        claude_client=None,
                        system_prompt=cs.SYSTEM_PROMPT or None,
                    )
                    if answer:
                        reply(answer)
                        logger.info(
                            "Responded with ollama answer using barcode evidence in thread_ts=%s barcode=%s",
                            thread_ts,
                            barcode,
                        )
                        return
                    logger.warning(
                        "Ollama barcode evidence synthesis returned empty in thread_ts=%s barcode=%s",
                        thread_ts,
                        barcode,
                    )

                thread_context = _load_thread_context(
                    client,
                    logger,
                    channel_id,
                    thread_ts,
                    current_ts,
                )
                model_input = _build_model_input(question, thread_context)
                answer = _ask_ollama_chat(
                    model_input,
                    system_prompt=cs.SYSTEM_PROMPT,
                    think=False,
                )
                if not answer:
                    answer = "답변을 생성하지 못했어. 다시 질문해줘"
                reply(answer)
                logger.info("Responded with ollama answer in thread_ts=%s", thread_ts)
            except TimeoutError:
                logger.warning("Ollama API timeout")
                reply(_timeout_reply_text())
            except RuntimeError as exc:
                if _is_timeout_error(exc):
                    logger.warning("Ollama API timeout")
                    reply(_timeout_reply_text())
                    return
                logger.exception("Ollama API call failed")
                reply("Ollama 응답 중 오류가 발생했어. 서버 연결 상태를 확인해줘")
            except Exception:
                logger.exception("Ollama API call failed")
                reply("Ollama 응답 중 오류가 발생했어. 서버 연결 상태를 확인해줘")
            return

        reply("현재는 ping, s3 조회, db 조회 또는 LLM 질문에 응답해")

    def _handle_company_message(
        payload: Any,
        reply: Any,
        client: Any,
        logger: logging.Logger,
    ) -> None:
        handle_fun_message(
            payload,
            reply,
            client,
            logger,
            claude_client=claude_client,
        )

    return create_slack_app(_handle_company_mention, _handle_company_message)
