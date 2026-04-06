import json
import logging
import re
from datetime import date, datetime
from typing import Any

import pymysql
from anthropic import Anthropic
from botocore.exceptions import BotoCoreError, ClientError
from slack_bolt import App

from boxer_adapter_slack.common import (
    MentionPayload,
    SlackReplyFn,
    _load_slack_user_name,
    _merge_request_log_metadata,
    _set_request_log_route,
    create_slack_app,
)
from boxer_adapter_slack.context import _load_slack_thread_context
from boxer_company_adapter_slack.fun import handle_fun_message
from boxer_company_adapter_slack.weekly_recordings_reporter import attach_weekly_recordings_reporter
from boxer_company.weekly_recordings_report import (
    _build_weekly_recordings_report_blocks,
    _build_weekly_recordings_report_summary,
    _coerce_weekly_recordings_report_now,
    _format_weekly_recordings_report,
)
from boxer_company.prompt_security import (
    build_prompt_security_refusal,
    is_prompt_exfiltration_attempt,
)
from boxer_company.team_chat_context import TEAM_MEMBER_PROFILES, build_team_freeform_context
from boxer_company.notion_links import select_company_notion_doc_links
from boxer_company.notion_playbooks import _select_notion_references
from boxer_company.retrieval_rules import (
    _build_company_retrieval_rules,
    _transform_company_retrieval_payload,
)
from boxer_company import settings as cs
from boxer_company.utils import _extract_barcode
from boxer.context.builder import _build_model_input
from boxer.core import settings as s
from boxer.core.llm import _ask_claude, _ask_ollama_chat, _check_claude_health, _check_ollama_health
from boxer.core.utils import _validate_tokens
from boxer.retrieval.connectors.db import _query_db, _validate_readonly_sql
from boxer.retrieval.connectors.notion import _is_notion_configured
from boxer.retrieval.connectors.s3 import _build_s3_client
from boxer.retrieval.synthesis import _synthesize_retrieval_answer
from boxer_company.routers.app_user import _lookup_app_user_by_barcode, _should_lookup_barcode
from boxer_company.routers.barcode_log import (
    _analyze_barcode_log_phase1_window,
    _analyze_barcode_log_errors,
    _analyze_barcode_log_scan_events,
    _build_phase2_scope_request_message,
    _extract_capture_seq_filters,
    _extract_device_flag_filters,
    _extract_device_name_scope,
    _extract_device_seq_filter,
    _extract_device_status_filter,
    _extract_hospital_room_scope,
    _extract_leading_hospital_scope,
    _extract_log_date,
    _extract_log_date_with_presence,
    _extract_year_filter,
    _is_barcode_all_recorded_dates_request,
    _is_barcode_baby_ai_list_request,
    _is_normal_video_status,
    _is_baby_ai_list_request_without_barcode,
    _is_barcode_video_info_request,
    _is_barcode_log_analysis_request,
    _is_barcode_last_recorded_at_request,
    _is_barcode_video_length_request,
    _is_barcode_video_list_request,
    _is_barcode_video_recorded_on_date_request,
    _is_barcode_video_count_request,
    _is_error_focused_request,
    _is_devices_filter_query_request,
    _is_hospitals_filter_query_request,
    _is_hospital_rooms_filter_query_request,
    _is_recordings_filter_query_request,
    _is_scan_focused_request,
    _is_ultrasound_capture_filter_query_request,
)
from boxer_company.routers.db_query import _extract_db_query, _format_db_query_result
from boxer_company.routers.device_file_probe import (
    _build_device_file_download_config_message,
    _build_device_file_probe_config_message,
    _build_device_file_recovery_config_message,
    _build_device_file_scope_request_message,
    _is_barcode_device_file_probe_request,
    _locate_barcode_file_candidates,
    _should_download_device_files,
    _should_probe_device_files,
    _should_recover_device_files,
    _should_render_compact_file_id_result,
    _should_render_compact_device_download_result,
    _should_render_compact_device_file_list,
    _should_render_compact_device_recovery_result,
)
from boxer_company.routers.device_audio_probe import (
    _build_device_audio_probe_config_message,
    _extract_device_name_for_audio_probe,
    _is_device_audio_probe_request,
    _probe_device_audio_output,
)
from boxer_company.routers.device_update import (
    _build_device_update_activity_input,
    _build_device_update_config_message,
    _extract_device_name_for_update,
    _is_device_agent_update_request,
    _is_device_box_update_request,
    _is_device_update_status_request,
    _query_device_update_status,
    _request_device_agent_update,
    _request_device_box_update,
)
from boxer_company.routers.device_status_probe import (
    _build_device_memory_patch_config_message,
    _build_device_status_probe_config_message,
    _extract_device_name_for_status_probe,
    _is_device_captureboard_probe_request,
    _is_device_led_probe_request,
    _is_device_memory_patch_request,
    _is_device_pm2_probe_request,
    _is_device_status_probe_request,
    _patch_device_pm2_memory,
    _probe_device_runtime_component,
    _probe_device_status_overview,
)
from boxer_company.routers.mda_graphql import _create_mda_activity_log
from boxer_company.routers.request_log_query import (
    _extract_request_log_query,
    _query_request_log_text,
)
from boxer_company.routers.recording_failure_analysis import (
    _build_cause_line,
    _build_recording_failure_analysis_evidence,
    _classify_record,
    _get_top_error_group,
    _has_recording_failure_analysis_hints,
    _is_recording_failure_analysis_request,
    _narrow_recording_failure_analysis_evidence,
    _render_recording_failure_analysis_fallback,
)
from boxer_company.routers.box_db import (
    _load_recordings_context_by_barcode,
    _lookup_device_contexts_by_hospital_room,
    _query_devices_by_filters,
    _query_all_recorded_dates_by_barcode,
    _query_baby_ai_list_by_barcode,
    _query_hospitals_by_filters,
    _query_hospital_rooms_by_filters,
    _query_last_recorded_at_by_barcode,
    _query_recordings_count_by_barcode,
    _query_recordings_detail_by_barcode,
    _query_recordings_by_filters,
    _query_recordings_length_by_barcode,
    _query_recordings_length_on_date_by_barcode,
    _query_recordings_list_by_barcode,
    _query_recordings_on_date_by_barcode,
    _query_ultrasound_captures_by_filters,
)
from boxer_company.routers.s3_domain import (
    _extract_s3_request,
    _query_s3_device_log,
    _query_s3_ultrasound_by_barcode,
)
from boxer_company.routers.usage_help import (
    _build_usage_help_response,
    _is_usage_help_request,
)

_NOTION_DOC_QUERY_TOKENS = (
    "마미박스",
    "mommybox",
    "박스",
    "동기화",
    "베이비매직",
    "babymagic",
    "바이오스",
    "bios",
    "초기화",
    "데스크탑 모드",
    "데스크탑",
    "네트워크 환경",
    "네트워크 설정",
    "설정 스크립트",
    "음량",
    "볼륨",
    "dvi",
    "qr 코드북",
    "qr코드",
    "커스텀 크롭",
    "크롭",
    "진단기",
    "원격 음성",
    "299버전",
    "299",
    "캡처보드",
    "바코드 스캐너",
    "바코드 동기화",
    "핑크 바코드",
    "하얀색 바코드",
    "무료 바코드",
    "유료 바코드",
    "분만 병원",
    "비분만 병원",
    "첫 촬영",
    "첫 녹화",
    "신규 바코드 구매",
    "추가 구매",
    "온라인 상태",
    "cfg1_barcode_sync_date",
    "프로비저닝",
    "오디오",
    "사운드케이블",
    "스피커",
    "노이즈",
    "잡음",
    "아티팩트",
    "지지직",
    "그라운드 루프",
    "메모리",
    "패치",
    "방화벽",
    "firewall",
    "mda",
    "모니터링",
    "종합모니터링",
    "원격 접속",
    "원격 연결",
    "ssh",
    "status none",
    "에이전트",
)
_NOTION_DOC_THREAD_MARKERS = (
    "문서 기반 답변",
    "함께 참고할 문서",
)
_NOTION_DOC_FOLLOWUP_TOKENS = (
    "다른 방법",
    "방법 있어",
    "방법 없어",
    "대안",
    "우회",
    "그럼",
    "그러면",
    "그래서",
    "이 경우",
    "이때",
    "그 뒤",
    "그 후",
    "이건",
    "이거",
    "그건",
    "그거",
    "말고",
    "추가로",
)
_NOTION_DOC_OPERATION_FOLLOWUP_TOKENS = (
    "왜",
    "원인",
    "이유",
    "어떻게",
    "어떻게 해",
    "어떻게 해야",
    "확인",
    "재부팅",
    "재시작",
    "동기화",
    "설정",
    "조치",
    "해결",
    "방법",
    "맞아",
    "맞아?",
    "맞나요",
    "어디",
)
_FREEFORM_THREAD_REFERENCE_TOKENS = (
    "직전 질문",
    "이전 질문",
    "방금 질문",
    "위 질문",
    "이전 대화",
    "직전 대화",
    "위 대화",
    "방금 대화",
    "앞 질문",
)
_FREEFORM_ANSWER_INSTRUCTION_TOKENS = (
    "답해봐",
    "대답해봐",
    "답해 줘",
    "답해줘",
    "대답해 줘",
    "대답해줘",
    "말해봐",
    "정리해봐",
    "정리해 줘",
    "정리해줘",
)
_FREEFORM_REFERENCE_INSTRUCTION_TOKENS = (
    "참고해서",
    "참고해",
    "기준으로",
    "기준 삼아",
    "기반으로",
)
_FREEFORM_SMALL_TALK_TOKENS = (
    "안녕",
    "반가",
    "하이",
    "hello",
    "hi",
    "hey",
    "굿모닝",
    "굿나잇",
    "잘자",
    "잘 자",
)
_FREEFORM_IDENTITY_TOKENS = (
    "넌누구",
    "너누구",
    "너는누구",
    "누구야",
    "정체",
    "자기소개",
    "넌나야",
    "너는나야",
    "너도나야",
)
_TEAM_MEMBER_ALIAS_TOKENS = tuple(
    sorted(
        {
            str(alias or "").strip().lower()
            for profile in TEAM_MEMBER_PROFILES
            for alias in (
                *(profile.get("aliases") or ()),
                profile.get("name"),
            )
            if str(alias or "").strip()
        }
    )
)
_FREEFORM_PROFILE_HINTS = (
    "어떤 사람",
    "어떤사람",
    "누구야",
    "누구 같",
    "성격",
    "스타일",
    "캐릭터",
    "타입",
    "mbti",
    "엠비티아이",
    "전투력",
    "상성",
    "서열",
    "누가 더 세",
    "누가 더 쎄",
    "누가 이겨",
    "누가이겨",
    "어때",
    "어때?",
)
_NOTION_DOC_EXFILTRATION_PATTERNS = (
    re.compile(
        r"(시스템\s*(정보|프롬프트|지시문)|system\s*prompt|developer\s*prompt|internal\s*prompt|hidden\s*prompt|instruction\s*prompt)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(문서\s*(원문|전문|본문)|원문|전문|본문|raw\s*text|full\s*text|complete\s*text|entire\s*text|whole\s*text|verbatim|dump|텍스트\s*전체|전체\s*텍스트)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(하나하나\s*(오픈|열)|하나씩\s*(오픈|열)|전부\s*보여|모두\s*보여|통째로\s*보여|텍스트로\s*보여|그대로\s*보여|show\s+me|open\s+each|open\s+every|one\s+by\s+one|full\s+text|all\s+text)",
        re.IGNORECASE,
    ),
    re.compile(
        r"((i\s*am|i'?m|im)\s+(super\s+admin|admin|owner|developer|maintainer)|super\s+admin|admin\s+mode|override|ignore\s+(previous|all)\s+(rules|instructions)|bypass)",
        re.IGNORECASE,
    ),
)
_NOTION_DOC_LEAK_MARKERS = (
    "system prompt",
    "developer prompt",
    "internal prompt",
    "thread context",
    "evidence(json)",
    "page_id=",
    "authorization:",
    "bearer ",
    "notion_token",
    "<think>",
    "</think>",
)
_FREEFORM_COMPARISON_HINTS = (
    " vs ",
    "누가",
    "전투력",
    "상성",
    "서열",
    "더 세",
    "더 쎄",
    "누가 이겨",
    "우위",
)
_FREEFORM_PLAYFUL_HINTS = (
    "놀려",
    "드립",
    "농담",
    "웃기",
    "한마디",
    "밈",
    "모대",
)
_FREEFORM_ADVICE_HINTS = (
    "어떻게",
    "추천",
    "골라",
    "선택",
    "판단",
    "하는 게 낫",
    "말까",
    "갈까",
)
_FREEFORM_META_LINE_PATTERNS = (
    re.compile(r"(?mi)^\s*현재 요청 적용\s*:\s*.+$"),
    re.compile(r"(?mi)^\s*(?:팀원별 컨텍스트|현재 화자 스타일|언급된 대상 반응 가이드)\s*:\s*$"),
)
_FREEFORM_META_PREFIX_REWRITES: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(
            r"^\s*(?:캐릭터|대화|채팅)\s*로그\s*기준(?:으로)?\s*(?:해석하면|보면)\s*[,:\-]?\s*",
            re.IGNORECASE,
        ),
        "",
    ),
    (
        re.compile(
            r"^\s*(?:채팅\s*밈|오늘\s*로그|캐릭터상(?:으로)?)\s*기준(?:으로)?\s*(?:해석하면|보면)\s*[,:\-]?\s*",
            re.IGNORECASE,
        ),
        "",
    ),
    (
        re.compile(
            r"\bfictional framing\b",
            re.IGNORECASE,
        ),
        "밈 프레임",
    ),
)


def _rewrite_phase2_scope_request_message(
    result_text: str,
    title: str,
    example_action: str,
) -> str:
    barcode_match = re.search(r"• 바코드: `([^`]+)`", result_text or "")
    reason_match = re.search(r"• 사유: (.+)", result_text or "")
    barcode = barcode_match.group(1).strip() if barcode_match else ""
    reason = reason_match.group(1).strip() if reason_match else "2차 입력이 필요해"
    return _build_phase2_scope_request_message(
        barcode,
        reason,
        title,
        example_action=example_action,
    )


def _extract_optional_requested_date(question: str) -> tuple[str | None, bool]:
    parsed_date, has_requested_date = _extract_log_date_with_presence(question)
    return (parsed_date if has_requested_date else None, has_requested_date)


def _is_weekly_recordings_report_request(
    question: str,
    *,
    barcode: str | None,
    target_date: str | None,
) -> bool:
    if barcode:
        return False

    text = (question or "").strip()
    if not text:
        return False
    lowered = text.lower()

    has_media_hint = any(token in text for token in ("초음파", "영상", "비디오", "동영상", "녹화")) or any(
        token in lowered for token in ("recording", "recordings")
    )
    if not has_media_hint:
        return False

    has_summary_hint = any(token in text for token in ("현황", "요약", "리포트", "보고", "집계", "통계", "정리", "병원별")) or any(
        token in lowered for token in ("summary", "report", "overview", "status")
    )
    if not has_summary_hint:
        return False

    has_week_hint = any(
        token in text
        for token in ("주간", "주별", "일주일", "한 주", "지난주", "지난 주", "저번주", "저번 주", "전주", "이번주", "이번 주")
    ) or any(token in lowered for token in ("weekly", "week"))
    if not has_week_hint:
        return False

    has_excluded_hint = any(
        token in text
        for token in (
            "바코드",
            "목록",
            "리스트",
            "상세",
            "길이",
            "재생시간",
            "다운로드",
            "복구",
            "로그",
            "캡처",
            "스냅샷",
        )
    ) or any(
        token in lowered
        for token in (
            "list",
            "detail",
            "download",
            "recover",
            "log",
            "capture",
            "captures",
            "snapshot",
            "duration",
            "fileid",
        )
    )
    return not has_excluded_hint


def _build_weekly_recordings_report_reply_payload(
    *,
    target_date: str | None = None,
    now: datetime | None = None,
) -> tuple[str, list[dict[str, Any]], str, str]:
    report_target_date = date.fromisoformat(target_date) if target_date else None
    local_now = _coerce_weekly_recordings_report_now(now)
    report_summary = _build_weekly_recordings_report_summary(
        target_date=report_target_date,
        now=local_now,
    )
    return (
        _format_weekly_recordings_report(report_summary, now=local_now),
        _build_weekly_recordings_report_blocks(report_summary, now=local_now),
        str(report_summary.get("weekStartDate") or "").strip()
        or (report_target_date.isoformat() if report_target_date is not None else ""),
        str(report_summary.get("weekEndDate") or "").strip(),
    )


def _is_generic_count_or_existence_request(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    return any(token in text for token in cs.VIDEO_COUNT_HINT_TOKENS) or any(
        token in text for token in ("있나", "있어", "있는지", "유무", "존재", "몇")
    ) or any(token in lowered for token in ("count",))


def _render_notion_playbook_section(playbooks: list[dict[str, Any]] | None) -> str:
    items = [item for item in (playbooks or []) if isinstance(item, dict)]
    if not items:
        return ""

    lines = ["*참고 플레이북*"]
    for item in items[:3]:
        title = str(item.get("title") or "").strip() or "제목 미상"
        matched_keywords = [
            str(keyword).strip()
            for keyword in (item.get("matchedKeywords") or [])
            if str(keyword).strip()
        ]
        line = f"- {title}"
        if matched_keywords:
            line += f" (`{', '.join(matched_keywords[:3])}`)"
        lines.append(line)
    return "\n".join(lines)


def _append_notion_playbook_section(
    text: str,
    playbooks: list[dict[str, Any]] | None,
) -> str:
    section = _render_notion_playbook_section(playbooks)
    normalized_text = (text or "").strip()
    if not section:
        return normalized_text
    if "참고 플레이북" in normalized_text:
        return normalized_text
    if not normalized_text:
        return section
    return f"{normalized_text}\n\n{section}"


def _render_company_notion_doc_section(docs: list[dict[str, Any]] | None) -> str:
    items = [item for item in (docs or []) if isinstance(item, dict)]
    if not items:
        return ""

    lines = ["*함께 참고할 문서*"]
    for item in items[:3]:
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()
        if not title or not url:
            continue
        lines.append(f"- <{url}|{title}>")
    return "\n".join(lines)


def _append_company_notion_doc_section(
    text: str,
    docs: list[dict[str, Any]] | None,
) -> str:
    section = _render_company_notion_doc_section(docs)
    normalized_text = (text or "").strip()
    if not section:
        return normalized_text
    if "함께 참고할 문서" in normalized_text:
        return normalized_text
    if not normalized_text:
        return section
    return f"{normalized_text}\n\n{section}"


def _looks_like_notion_doc_question(question: str) -> bool:
    text = (question or "").strip()
    if not text:
        return False
    if _looks_like_thread_answer_instruction(text):
        return False
    return any(token in text for token in _NOTION_DOC_QUERY_TOKENS)


def _thread_has_notion_doc_context(thread_context: str) -> bool:
    text = (thread_context or "").strip()
    if not text:
        return False
    return any(marker in text for marker in _NOTION_DOC_THREAD_MARKERS)


def _sanitize_notion_doc_thread_context(thread_context: str) -> str:
    text = (thread_context or "").strip()
    if not _thread_has_notion_doc_context(text):
        return ""
    return text


def _looks_like_small_talk_question(question: str) -> bool:
    text = (question or "").strip()
    if not text:
        return False

    lowered = text.lower()
    collapsed = re.sub(r"[\s?!.,~]+", "", lowered)
    if any(token in text for token in _FREEFORM_SMALL_TALK_TOKENS):
        return True
    return any(token in collapsed for token in _FREEFORM_IDENTITY_TOKENS)


def _looks_like_team_freeform_question(question: str) -> bool:
    text = (question or "").strip()
    if not text:
        return False

    lowered = text.lower()
    collapsed = re.sub(r"[\s?!.,~]+", "", lowered)
    has_member_alias = any(alias in lowered for alias in _TEAM_MEMBER_ALIAS_TOKENS)
    has_profile_hint = any(token in lowered for token in _FREEFORM_PROFILE_HINTS) or any(
        token in collapsed for token in _FREEFORM_PROFILE_HINTS
    )

    if has_member_alias and has_profile_hint:
        return True
    if any(token in lowered for token in _FREEFORM_COMPARISON_HINTS):
        return True
    return False


def _looks_like_thread_answer_instruction(question: str) -> bool:
    text = (question or "").strip()
    if not text:
        return False

    has_thread_reference = any(token in text for token in _FREEFORM_THREAD_REFERENCE_TOKENS)
    if not has_thread_reference:
        return False

    has_answer_instruction = any(token in text for token in _FREEFORM_ANSWER_INSTRUCTION_TOKENS)
    has_reference_instruction = any(token in text for token in _FREEFORM_REFERENCE_INSTRUCTION_TOKENS)
    return has_answer_instruction or has_reference_instruction


def _looks_like_notion_doc_followup(question: str, thread_context: str) -> bool:
    text = (question or "").strip()
    if not text or not _thread_has_notion_doc_context(thread_context):
        return False
    if _looks_like_small_talk_question(text):
        return False
    if _looks_like_team_freeform_question(text):
        return False
    if _looks_like_thread_answer_instruction(text):
        return False
    if _looks_like_notion_doc_question(text):
        return False

    lowered = text.lower()
    if any(token in text for token in _NOTION_DOC_FOLLOWUP_TOKENS):
        return True
    if any(token in text for token in _NOTION_DOC_OPERATION_FOLLOWUP_TOKENS):
        return True
    if any(token in lowered for token in ("alternative", "workaround", "other way", "else")):
        return True
    return False


def _build_notion_doc_query_text(question: str, thread_context: str) -> str:
    normalized_question = (question or "").strip()
    normalized_thread = _resolve_notion_doc_thread_context(question, thread_context)
    if not normalized_thread:
        return normalized_question

    thread_lines = [line.strip() for line in normalized_thread.splitlines() if line.strip()]
    relevant_thread = "\n".join(thread_lines[-6:])
    if not relevant_thread:
        return normalized_question
    return f"{relevant_thread}\n{normalized_question}".strip()


def _resolve_notion_doc_thread_context(question: str, thread_context: str) -> str:
    normalized_thread = _sanitize_notion_doc_thread_context(thread_context)
    if not normalized_thread:
        return ""
    if not _looks_like_notion_doc_followup(question, normalized_thread):
        return ""
    return normalized_thread


def _is_notion_doc_exfiltration_attempt(question: str, thread_context: str = "") -> bool:
    text = (question or "").strip()
    if not text:
        return False
    if not (
        _looks_like_notion_doc_question(text)
        or _thread_has_notion_doc_context(thread_context)
    ):
        return False
    return any(pattern.search(text) for pattern in _NOTION_DOC_EXFILTRATION_PATTERNS)


def _build_notion_doc_security_refusal() -> str:
    return "보안 위반 시도로 판단해 요청을 즉시 차단해. 문서 원문, 시스템 정보, 내부 지시문은 공개하지 않아. 같은 시도가 반복되면 관리자 검토 및 접근 제한 대상으로 처리해."


def _classify_freeform_response_mode(question: str, thread_context: str = "") -> str:
    normalized = f"{question or ''}\n{thread_context or ''}".lower()
    if any(token in normalized for token in _FREEFORM_COMPARISON_HINTS):
        return "comparison"
    if any(token in normalized for token in _FREEFORM_PLAYFUL_HINTS):
        return "playful"
    if any(token in normalized for token in _FREEFORM_ADVICE_HINTS):
        return "advice"
    return "analysis"


def _build_freeform_response_rules(question: str, thread_context: str = "") -> str | None:
    base_rules = (cs.FREEFORM_RESPONSE_RULES_PROMPT or "").strip()
    mode = _classify_freeform_response_mode(question, thread_context)
    mode_line = {
        "comparison": '- 비교/상성 질문이면 "결론 -> 이유 2~3개 -> 변수/예외 1개" 순서로 바로 답해.',
        "playful": "- 가벼운 드립 질문이면 1~3문장 안에서 임팩트 있게 답해. 마지막 한 줄만 세게 쳐.",
        "advice": '- 조언/판단 질문이면 "결론 -> 옵션/다음 액션 -> 이유" 순서로 답해.',
        "analysis": '- 해석/분석 질문이면 "결론 -> 구조적 근거 -> 리스크/예외" 순서로 답해.',
    }[mode]
    if base_rules:
        return f"{base_rules}\n{mode_line}"
    return mode_line


def _sanitize_freeform_reply(text: str) -> str:
    normalized = (text or "").strip()
    if not normalized:
        return ""

    cleaned = normalized
    for pattern in _FREEFORM_META_LINE_PATTERNS:
        cleaned = pattern.sub("", cleaned)
    for pattern, replacement in _FREEFORM_META_PREFIX_REWRITES:
        cleaned = pattern.sub(replacement, cleaned)

    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned or normalized


def _get_freeform_system_prompt(
    question: str = "",
    thread_context: str = "",
) -> str | None:
    sections = [
        (cs.FREEFORM_CORE_IDENTITY_PROMPT or "").strip(),
        _build_freeform_response_rules(question, thread_context) or "",
    ]
    prompt = "\n\n".join(section for section in sections if section).strip()
    return prompt or None


def _build_freeform_chat_system_prompt(
    question: str,
    thread_context: str,
    *,
    speaker_user_id: str = "",
) -> str | None:
    base_prompt = _get_freeform_system_prompt(question, thread_context) or ""
    team_context = build_team_freeform_context(
        question,
        thread_context,
        speaker_user_id=speaker_user_id,
    )
    if base_prompt and team_context:
        return f"{base_prompt}\n\n{team_context}"
    if base_prompt:
        return base_prompt
    return team_context or None


def _sanitize_notion_references_for_llm(references: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    items = [item for item in (references or []) if isinstance(item, dict)]
    sanitized: list[dict[str, Any]] = []
    for item in items[:3]:
        preview_lines = [
            str(line).strip()[:160]
            for line in (item.get("previewLines") or [])
            if str(line).strip()
        ][:5]
        sanitized.append(
            {
                "title": str(item.get("title") or "").strip(),
                "section": str(item.get("section") or "").strip(),
                "kind": str(item.get("kind") or "").strip(),
                "priority": str(item.get("priority") or "").strip(),
                "matchedKeywords": [
                    str(keyword).strip()
                    for keyword in (item.get("matchedKeywords") or [])
                    if str(keyword).strip()
                ][:4],
                "previewLines": preview_lines,
                "summary": " / ".join(preview_lines[:3]),
            }
        )
    return sanitized


def _needs_notion_doc_security_refusal(text: str, route_name: str) -> bool:
    if route_name != "notion playbook qa":
        return False
    normalized = (text or "").strip().lower()
    if any(marker in normalized for marker in _NOTION_DOC_LEAK_MARKERS):
        return True
    meaningful_lines = [line for line in (text or "").splitlines() if line.strip()]
    if "```" in (text or ""):
        return True
    if len(meaningful_lines) > 16:
        return True
    if len(text or "") > 1400:
        return True
    return False


_BABYMAGIC_DOC_TITLES = {"베이비매직 장애 안내", "베이비매직 CS 자동화"}
_BABYMAGIC_SEND_HINT_TOKENS = (
    "유저 번호",
    "미매칭",
    "재전송",
    "앱으로 전송",
    "앱 전송",
)
_BABYMAGIC_RETRY_ACTION = (
    "유저가 앱에서 생성한 아이에 바코드를 등록했는지 먼저 확인하고, "
    "그다음 MDA 베이비매직 관리에서 재전송을 시도해"
)
_MOMMYBOX_RECORDING_PROCESS_TITLE = "마미박스 프로세스 순서"
_PINK_BARCODE_OVERVIEW_TITLE = "핑크 바코드: 운영 개요"
_BARCODE_FIRST_RECORDING_EDGE_CASE_TITLE = "바코드 표시: 구매 병원과 첫 촬영 병원이 다른 경우"
_PINK_BARCODE_VALIDATION_POLICY_TITLE = "바코드 검증: 핑크 바코드만 예외 허용할 수 있는지"


def _build_notion_doc_fallback(question: str, references: list[dict[str, Any]] | None) -> str:
    def _clean_preview_line(text: str) -> str:
        line = re.sub(r"^#+\s*", "", str(text or "").strip())
        line = re.sub(r"^[-*•]\s*", "", line)
        line = re.sub(r"\s+", " ", line).strip()
        if ":" in line:
            prefix, rest = line.split(":", 1)
            normalized_prefix = prefix.strip()
            if normalized_prefix and (
                len(normalized_prefix) <= 24
                or normalized_prefix.endswith(("돼요", "되나", "포인트", "기준"))
                or normalized_prefix in {"정책", "전제", "운영 기준", "실제 사례"}
            ):
                line = rest.strip()
        replacements = (
            (
                "비분만 병원에서 무료 발급한 바코드(핑크 바코드)는 바코드를 유료로 판매하는 분만 병원 장비에서 스캔되지 않아야 함",
                "무료 바코드는 분만 병원에서 스캔되면 안 돼",
            ),
            (
                "이 정책은 분만 병원 마미박스가 온라인 상태에서 바코드 동기화를 받아야 반영됨",
                "온라인 바코드 동기화가 돼야 반영돼",
            ),
            (
                "장비의 마지막 바코드 동기화 일자가 오래되면 최신 제한 대상 바코드를 아직 내려받지 못해 녹화 준비로 넘어갈 수 있음",
                "동기화가 밀리면 차단 바코드가 아직 반영되지 않을 수 있어",
            ),
            (
                "오프라인이거나 동기화가 밀린 장비는 무료 바코드 차단 정책이 늦게 반영될 수 있음",
                "오프라인 장비는 차단 반영이 늦을 수 있어",
            ),
            (
                "마지막 바코드 동기화 일자와 `cfg1_barcode_sync_date` 갱신 여부를 먼저 확인",
                "마지막 동기화 일자와 `cfg1_barcode_sync_date`를 확인해",
            ),
            (
                "마지막 바코드 동기화 일자와 cfg1_barcode_sync_date 갱신 여부를 먼저 확인",
                "마지막 동기화 일자와 `cfg1_barcode_sync_date`를 확인해",
            ),
            (
                "아니고 평소에도 동기화는 계속 진행된다. 다만 재시작 직후에는 동기화가 실제로 도는지 확인하기 쉽다",
                "재부팅이 필수는 아니고 평소에도 동기화는 계속 돌아가",
            ),
            (
                "아니야. 평소에도 동기화는 계속 돌아가고, 재시작은 동기화가 실제로 진행됐는지 확인하기 쉬운 시점이야",
                "재부팅이 필수는 아니고 평소에도 동기화는 계속 돌아가",
            ),
            (
                "일반 사용 중에는 재부팅을 하지 않아도 매일 동기화가 진행된다고 보면 됨",
                "평소에도 매일 동기화가 진행돼",
            ),
        )
        for source, target in replacements:
            line = line.replace(source, target)
        return line[:90]

    def _pick_preview_line(
        lines: list[str],
        *,
        include_tokens: tuple[str, ...] = (),
        exclude_texts: set[str] | None = None,
    ) -> str:
        excluded = exclude_texts or set()
        for line in lines:
            if not line or line in excluded:
                continue
            if include_tokens and not any(token in line for token in include_tokens):
                continue
            return line
        return ""

    items = [item for item in (references or []) if isinstance(item, dict)]
    lines = ["*문서 기반 답변*"]
    if not items:
        lines.append("• 결론: 관련 문서를 못 찾았어")
        lines.append("• 확인: 증상이나 키워드를 더 구체적으로 말해줘")
        lines.append("• 조치: 문서 제목이나 장애 증상을 같이 보내줘")
        return "\n".join(lines)

    primary_title = str(items[0].get("title") or "").strip() or "제목 미상"
    preview_fragments: list[str] = []
    for item in items[:3]:
        for raw_line in item.get("previewLines") or []:
            line = _clean_preview_line(raw_line)
            if not line:
                continue
            if line == str(item.get("title") or "").strip():
                continue
            if line.startswith("- page_id="):
                continue
            if line in preview_fragments:
                continue
            preview_fragments.append(line)
            if len(preview_fragments) >= 8:
                break
        if len(preview_fragments) >= 8:
            break

    is_pink_barcode_overview_doc = primary_title == _PINK_BARCODE_OVERVIEW_TITLE
    is_mommybox_recording_process_doc = primary_title == _MOMMYBOX_RECORDING_PROCESS_TITLE
    is_barcode_sync_doc = primary_title == "바코드 동기화: 분만 병원에서 핑크 바코드가 스캔되는 경우"
    is_barcode_first_recording_edge_case_doc = primary_title == _BARCODE_FIRST_RECORDING_EDGE_CASE_TITLE
    is_pink_barcode_validation_policy_doc = primary_title == _PINK_BARCODE_VALIDATION_POLICY_TITLE
    is_firewall_doc = primary_title == "병원 방화벽으로 MDA/원격 접속이 안 될 때"
    normalized_question = (question or "").strip()
    is_babymagic_send_issue = primary_title in _BABYMAGIC_DOC_TITLES and (
        any(token in normalized_question for token in ("전송", "재전송", "이유", "원인", "왜", "안 된", "안된"))
        or any(
            any(token in fragment for token in _BABYMAGIC_SEND_HINT_TOKENS)
            for fragment in preview_fragments
        )
    )
    is_reason_question = any(token in normalized_question for token in ("왜", "원인", "이유"))
    is_restart_question = any(token in normalized_question for token in ("재부팅", "재시작", "껐다", "켜야"))
    is_meaning_question = "cfg1_barcode_sync_date" in normalized_question or any(
        token in normalized_question for token in ("뭐야", "무엇", "뜻", "의미")
    )

    conclusion = ""
    if is_restart_question:
        conclusion = _pick_preview_line(
            preview_fragments,
            include_tokens=("재부팅", "재시작"),
        )
    elif is_reason_question:
        conclusion = _pick_preview_line(
            preview_fragments,
            include_tokens=("반영되지", "스캔될 수 있어", "왜 스캔되나", "원인"),
        )
    elif is_meaning_question:
        conclusion = _pick_preview_line(
            preview_fragments,
            include_tokens=("cfg1_barcode_sync_date",),
        )
    if not conclusion:
        conclusion = _pick_preview_line(
            preview_fragments,
            include_tokens=("안 돼", "동기화", "원인"),
        ) or (preview_fragments[0] if preview_fragments else f"`{primary_title}` 기준 확인 필요")

    used_lines = {conclusion}
    confirm = _pick_preview_line(
        preview_fragments,
        include_tokens=("확인 포인트", "cfg1_barcode_sync_date", "마지막 동기화"),
        exclude_texts=used_lines,
    )
    if not confirm:
        confirm = _pick_preview_line(
            preview_fragments,
            include_tokens=("전제:", "온라인", "동기화"),
            exclude_texts=used_lines,
        )
    if not confirm:
        confirm = _pick_preview_line(preview_fragments, exclude_texts=used_lines) or f"`{primary_title}` 문서를 먼저 봐"
    used_lines.add(confirm)

    action = _pick_preview_line(
        preview_fragments,
        include_tokens=("확인 포인트", "cfg1_barcode_sync_date", "마지막 동기화"),
        exclude_texts=used_lines,
    )
    if not action:
        action = _pick_preview_line(
            preview_fragments,
            include_tokens=("온라인", "오프라인", "동기화"),
            exclude_texts=used_lines,
        )
    if not action:
        action = "문서 기준 확인 필요"
    if is_babymagic_send_issue:
        action = _BABYMAGIC_RETRY_ACTION

    if is_pink_barcode_overview_doc:
        lines.append("• 결론: 핑크 바코드 이슈는 동기화, 앱 표시, 검증 정책 3가지로 나눠 봐야 해")
        lines.append("• 확인: 지금 질문이 분만 병원에서 스캔된 건지, 앱에 핑크로 보이는 건지, 검증 해제 정책인지 먼저 구분해")
        lines.append("• 조치: 스캔 이슈면 동기화 문서, 앱 표시 이슈면 첫 촬영 병원 문서, 허용/차단 정책이면 검증 정책 문서 기준으로 이어서 보면 돼")
        return "\n".join(lines)

    if is_mommybox_recording_process_doc:
        lines.append("• 결론: 바코드 스캔 후 준비 음성이 나오고 세션이 생성된 뒤 모션 감지가 시작돼")
        lines.append("• 확인: 모션 감지 단계의 상태는 RECORDING이 아니라 SESSION이고, 모션 감지 성공 또는 타임아웃이면 그때 녹화 시작 음성 후 본 녹화가 시작돼. 모션 감지 단계에서 종료 스캔하면 본 녹화 종료와 같은 의미가 아니야")
        lines.append("• 조치: 녹화 중 종료 스캔하면 종료 음성이 나오고 파일을 마무리한 뒤 업로드를 시도한다고 안내해")
        return "\n".join(lines)

    if is_barcode_sync_doc and not is_meaning_question:
        if is_restart_question:
            barcode_sync_conclusion = "재부팅이 필수는 아니고, 마미박스는 매일 핑크 바코드 동기화를 시도해"
        else:
            barcode_sync_conclusion = "지금은 장비가 최신 핑크 바코드까지 동기화하지 못해서 분만 병원에서도 스캔된 거로 봐"
        lines.append(f"• 결론: {barcode_sync_conclusion}")
        lines.append("• 확인: 핑크 바코드 동기화가 가능한 버전인지 먼저 확인해")
        lines.append("• 조치: 마미박스를 핑크 바코드 동기화가 가능한 버전으로 업데이트해야 해. 1회당 약 10일치 바코드를 가져오고, 매일 동기화를 시도해. 현재 기본 DB에는 1월 1일부터의 핑크 바코드 목록이 있어")
        return "\n".join(lines)

    if is_barcode_first_recording_edge_case_doc:
        lines.append("• 결론: 첫 녹화가 비분만 병원에서 먼저 나가면 앱에는 핑크 바코드로 보일 수 있어")
        lines.append("• 확인: 분만 병원에서 실제 첫 촬영이 없었는지랑 첫 recording hospital이 비분만 병원인지 먼저 확인해")
        lines.append("• 조치: 이건 표시상 엣지케이스라 실제 녹화 차단이나 신규 바코드 추가 구매가 필요한 건 아니라고 안내해")
        return "\n".join(lines)

    if is_pink_barcode_validation_policy_doc:
        is_validation_disable_question = any(
            token in normalized_question
            for token in (
                "검증을 풀",
                "검증 풀",
                "검증 해제",
                "유효성 검증 해제",
                "검증없이",
                "검증 없이",
            )
        )
        if is_validation_disable_question:
            lines.append("• 결론: 맞아. 바코드 유효성 검증을 해제하면 검증 없이 녹화가 진행돼")
            lines.append("• 확인: 이건 핑크 바코드만 예외 허용하는 게 아니라 전체 검증을 푸는 설정이야")
            lines.append("• 조치: 특정 핑크 바코드만 따로 허용하는 건 현재 안 돼. 허용이 필요하면 검증 유지/전체 해제 중 운영 판단이 필요해")
            return "\n".join(lines)

        lines.append("• 결론: 핑크 바코드만 따로 녹화 허용/차단하는 설정은 없어")
        lines.append("• 확인: 분만 병원에서 차단이 걸리려면 바코드 유효성 검증이 켜져 있는지 먼저 확인해")
        lines.append("• 조치: 핑크 바코드도 녹화되게 하려면 바코드 유효성 검증 자체를 해제해야 하고, 그러면 검증 없이 녹화가 진행돼")
        return "\n".join(lines)

    if is_firewall_doc:
        lines.append("• 결론: 영상 업로드는 정상이어도 원격 접속은 별도 경로라 불가할 수 있어. 현재는 장비 원격 접근이 제한된 상태야")
        lines.append("• 확인: 병원 네트워크 또는 방화벽 설정 여부, 방화벽 정책, 장비 원격 접근 여부(SSH 연결) 확인이 필요해")
        lines.append("• 조치: 병원과 네트워크 또는 방화벽 설정을 소통 및 협의해야 해. 접속이 열리면 그 뒤에 원격 진단을 다시 진행할 수 있어")
        return "\n".join(lines)

    lines.append(f"• 결론: {conclusion}")
    lines.append(f"• 확인: {confirm}")
    lines.append(f"• 조치: {action}")
    return "\n".join(lines)


def _needs_notion_doc_fallback(text: str, route_name: str, fallback_text: str = "") -> bool:
    if route_name != "notion playbook qa":
        return False

    normalized = (text or "").strip()
    if not normalized:
        return True
    if normalized == _build_notion_doc_security_refusal():
        return False
    if not normalized.startswith("*문서 기반 답변*"):
        return True

    fallback_normalized = (fallback_text or "").strip()
    lowered = normalized.lower()
    fallback_lowered = fallback_normalized.lower()
    if "핑크 바코드 동기화가 가능한 버전" in fallback_normalized and "핑크 바코드 동기화가 가능한 버전" not in normalized:
        return True
    if "cfg1_barcode_sync_date" in lowered and "cfg1_barcode_sync_date" not in fallback_lowered:
        return True
    if _BABYMAGIC_RETRY_ACTION in fallback_normalized and _BABYMAGIC_RETRY_ACTION not in normalized:
        return True
    if "표시상 엣지케이스" in fallback_normalized and "엣지케이스" not in normalized:
        return True
    if "추가 구매" in fallback_normalized and "추가 구매" not in normalized:
        return True

    required_bullets = (
        "• 결론:",
        "• 확인:",
        "• 조치:",
    )
    return any(bullet not in normalized for bullet in required_bullets)


def _normalize_notion_doc_answer_style(text: str, route_name: str) -> str:
    if route_name != "notion playbook qa":
        return (text or "").strip()

    normalized = (text or "").strip()
    if not normalized:
        return normalized

    replacements = (
        ("소통·협의", "소통 및 협의"),
        ("원격으로 원인 확인이나 조치가 어렵다고 안내해", "원격으로 원인 확인이나 조치가 어려워"),
        ("협의가 필요하다고 안내해", "협의가 필요해"),
        ("확인이 필요하다고 안내해", "확인이 필요해"),
        ("다시 진행한다고 안내해", "다시 진행할 수 있어"),
        ("다시 진행한다고 답해", "다시 진행할 수 있어"),
        ("안내해.", ""),
        ("안내해", ""),
    )
    for source, target in replacements:
        normalized = normalized.replace(source, target)

    normalized = re.sub(r"\s+\.", ".", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()


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


def _extract_user_only_thread_text(thread_context: str, target_user_id: str) -> str:
    prefix = f"{(target_user_id or '').strip()}: "
    if not prefix.strip():
        return ""
    lines: list[str] = []
    for raw_line in (thread_context or "").splitlines():
        line = raw_line.strip()
        if not line.startswith(prefix):
            continue
        lines.append(line[len(prefix) :].strip())
    return "\n".join(part for part in lines if part)


def _extract_latest_barcode_from_thread_context(thread_context: str) -> str | None:
    lines = [line.strip() for line in (thread_context or "").splitlines() if line.strip()]
    for line in reversed(lines):
        barcode = _extract_barcode(line)
        if barcode:
            return barcode
    return None


def _collect_device_download_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for record in payload.get("records") or []:
        if not isinstance(record, dict):
            continue

        file_names: list[str] = []
        seen_files: set[str] = set()
        download_links: list[dict[str, str]] = []
        seen_links: set[str] = set()

        for session in record.get("sessions") or []:
            if not isinstance(session, dict):
                continue

            probe = session.get("probe") if isinstance(session.get("probe"), dict) else None
            if probe and probe.get("ok"):
                for found_file in probe.get("files") or []:
                    file_name = str(found_file or "").strip().split("/")[-1]
                    if file_name and file_name not in seen_files:
                        seen_files.add(file_name)
                        file_names.append(file_name)

            download = session.get("download") if isinstance(session.get("download"), dict) else None
            if not download:
                continue
            for item in download.get("downloads") or []:
                if not isinstance(item, dict) or not item.get("ok"):
                    continue
                file_name = str(item.get("fileName") or "").strip()
                url = str(item.get("url") or "").strip()
                if not file_name or not url:
                    continue
                dedupe_key = file_name
                if dedupe_key in seen_links:
                    continue
                seen_links.add(dedupe_key)
                download_links.append({"fileName": file_name, "url": url})

        if not download_links:
            continue

        records.append(
            {
                "deviceName": str(record.get("deviceName") or "").strip() or "미확인",
                "deviceSeq": record.get("deviceSeq"),
                "hospitalSeq": record.get("hospitalSeq"),
                "hospitalRoomSeq": record.get("hospitalRoomSeq"),
                "hospitalName": str(record.get("hospitalName") or "").strip() or "미확인",
                "roomName": str(record.get("roomName") or "").strip() or "미확인",
                "fileNames": file_names,
                "downloadLinks": download_links,
            }
        )

    return records


def _build_device_download_activity_input(
    *,
    record: dict[str, Any],
    barcode: str,
    log_date: str,
    question: str,
    user_id: str,
    user_name: str | None,
    channel_id: str,
    thread_ts: str,
) -> dict[str, Any]:
    device_name = str(record.get("deviceName") or "").strip() or "미확인"
    hospital_name = str(record.get("hospitalName") or "").strip() or "미확인"
    room_name = str(record.get("roomName") or "").strip() or "미확인"
    requester_name = str(user_name or "").strip()
    requester_label = requester_name or str(user_id or "").strip()
    file_names = [str(item).strip() for item in (record.get("fileNames") or []) if str(item).strip()]
    download_links = [
        item
        for item in (record.get("downloadLinks") or [])
        if isinstance(item, dict) and str(item.get("fileName") or "").strip() and str(item.get("url") or "").strip()
    ]

    detail_log = {
        "source": "boxer_slack_device_download",
        "barcode": barcode,
        "logDate": log_date,
        "question": question,
        "slackUserId": user_id,
        "slackUserName": requester_name,
        "slackChannelId": channel_id,
        "slackThreadTs": thread_ts,
        "requestedBySlackUserId": user_id,
        "requestedBySlackUserName": requester_name,
        "deviceName": device_name,
        "deviceSeq": record.get("deviceSeq"),
        "hospitalSeq": record.get("hospitalSeq"),
        "hospitalRoomSeq": record.get("hospitalRoomSeq"),
        "hospitalName": hospital_name,
        "roomName": room_name,
        "fileNames": file_names,
        "downloadFileNames": [
            str(item.get("fileName") or "").strip()
            for item in download_links
        ],
        "downloadLinkCount": len(download_links),
    }

    return {
        "activityType": "recording.download",
        "barcode": barcode or None,
        "hospitalSeq": record.get("hospitalSeq"),
        "hospitalRoomSeq": record.get("hospitalRoomSeq"),
        "deviceSeq": record.get("deviceSeq"),
        "targetEntityType": "Device" if record.get("deviceSeq") is not None else None,
        "targetEntitySeq": record.get("deviceSeq"),
        "reason": "Boxer Slack 다운로드 링크 전송 성공",
        "description": (
            f"Boxer Slack 다운로드 링크 전송 완료: 병원명 [{hospital_name}], "
            f"병실명 [{room_name}], 장비명 [{device_name}]"
            f"{f', 요청자 [{requester_label}]' if requester_label else ''}, 파일 {len(download_links)}개"
        ),
        "detailLog": json.dumps(detail_log, ensure_ascii=False, separators=(",", ":")),
    }


def _log_device_download_activity(
    *,
    records: list[dict[str, Any]],
    barcode: str,
    log_date: str,
    question: str,
    user_id: str,
    user_name: str | None,
    channel_id: str,
    thread_ts: str,
    logger: logging.Logger,
) -> int:
    if not records:
        return 0

    success_count = 0

    for record in records:
        try:
            _create_mda_activity_log(
                _build_device_download_activity_input(
                    record=record,
                    barcode=barcode,
                    log_date=log_date,
                    question=question,
                    user_id=user_id,
                    user_name=user_name,
                    channel_id=channel_id,
                    thread_ts=thread_ts,
                )
            )
            success_count += 1
        except Exception:
            logger.warning(
                "Failed to create activity log for device download barcode=%s device=%s",
                barcode,
                record.get("deviceName"),
                exc_info=True,
            )
    return success_count


def _log_device_update_activity(
    *,
    question: str,
    user_id: str,
    channel_id: str,
    thread_ts: str,
    result_payload: dict[str, Any],
    client: Any,
    logger: logging.Logger,
) -> bool:
    try:
        user_name = _load_slack_user_name(
            client=client,
            user_id=user_id,
            logger=logger,
        )
        _create_mda_activity_log(
            _build_device_update_activity_input(
                question=question,
                user_id=user_id,
                user_name=user_name,
                channel_id=channel_id,
                thread_ts=thread_ts,
                result_payload=result_payload,
            )
        )
        return True
    except Exception:
        logger.warning(
            "Failed to create activity log for device update route=%s device=%s",
            result_payload.get("route"),
            ((result_payload.get("device") or {}) if isinstance(result_payload.get("device"), dict) else {}).get(
                "deviceName"
            ),
            exc_info=True,
        )
        return False


def _render_device_download_dm_text(
    barcode: str,
    log_date: str,
    records: list[dict[str, Any]],
) -> str:
    lines = [
        "*장비 영상 다운로드 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
    ]
    for record in records:
        lines.append("")
        lines.append(f"• 장비: `{record['deviceName']}`")
        lines.append(f"• 병원: `{record['hospitalName']}`")
        lines.append(f"• 병실: `{record['roomName']}`")
        file_names = record.get("fileNames") or []
        lines.append(f"• 장비에 존재하는 영상 목록: `{len(file_names)}개`")
        for file_name in file_names:
            lines.append(f"  - `{file_name}`")
        download_links = record.get("downloadLinks") or []
        lines.append(f"• 다운로드 링크: `{len(download_links)}개` (1시간)")
        for item in download_links:
            lines.append(f"  - 🎣 <{item['url']}|{item['fileName']}>")
    return "\n".join(lines)


def _render_device_download_thread_notice(
    barcode: str,
    log_date: str,
    records: list[dict[str, Any]],
    *,
    activity_logged: bool = False,
    used_expanded_scope: bool = False,
) -> str:
    lines = [
        "*장비 영상 다운로드 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
    ]
    if used_expanded_scope:
        lines.append("• 참고: 매핑 장비 외 같은 병원 장비도 함께 검색했어")
    for record in records:
        lines.append("")
        lines.append(f"• 장비: `{record['deviceName']}`")
        lines.append(f"• 병원: `{record['hospitalName']}`")
        lines.append(f"• 병실: `{record['roomName']}`")
        file_names = record.get("fileNames") or []
        lines.append(f"• 장비에 존재하는 영상 목록: `{len(file_names)}개`")
        for file_name in file_names:
            lines.append(f"  - `{file_name}`")
        lines.append(f"• 다운로드 링크: DM으로 보냈어 (`{len(record.get('downloadLinks') or [])}개`)")
    if activity_logged:
        lines.append("")
        lines.append("• 다운로드 내역 기록되었습니다. 🎣 <https://mda.kr.mmtalkbox.com/cs|CS 처리내역 엿보기>")
    return "\n".join(lines)


def _render_device_download_dm_failure_notice(
    barcode: str,
    log_date: str,
    records: list[dict[str, Any]],
    *,
    used_expanded_scope: bool = False,
) -> str:
    lines = [
        "*장비 영상 다운로드 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
    ]
    if used_expanded_scope:
        lines.append("• 참고: 매핑 장비 외 같은 병원 장비도 함께 검색했어")
    for record in records:
        lines.append("")
        lines.append(f"• 장비: `{record['deviceName']}`")
        lines.append(f"• 병원: `{record['hospitalName']}`")
        lines.append(f"• 병실: `{record['roomName']}`")
        file_names = record.get("fileNames") or []
        lines.append(f"• 장비에 존재하는 영상 목록: `{len(file_names)}개`")
        for file_name in file_names:
            lines.append(f"  - `{file_name}`")
    lines.append("• 다운로드 링크: DM 전송 실패. 봇 DM 권한을 확인해줘")
    return "\n".join(lines)


def create_app() -> App:
    _validate_tokens(include_llm=True, include_data_sources=True)
    claude_client = (
        Anthropic(
            api_key=s.ANTHROPIC_API_KEY,
            timeout=s.ANTHROPIC_TIMEOUT_SEC,
        )
        if s.LLM_PROVIDER == "claude"
        else None
    )
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
        workspace_id = payload["workspace_id"]
        channel_id = payload["channel_id"]
        current_ts = payload["current_ts"]
        thread_ts = payload["thread_ts"]

        if "ping" in text:
            _set_request_log_route(payload, "ping")
            provider = (s.LLM_PROVIDER or "").lower().strip()
            if provider == "ollama":
                health = _check_ollama_health()
                reply(f"🏓 pong\n• llm: {_format_ping_llm_status(bool(health['ok']))}")
                logger.info(
                    "Responded with ping health in thread_ts=%s provider=ollama ok=%s",
                    thread_ts,
                    health["ok"],
                )
                return
            if provider == "claude":
                health = _check_claude_health()
                reply(f"🏓 pong\n• llm: {_format_ping_llm_status(bool(health['ok']))}")
                logger.info(
                    "Responded with ping health in thread_ts=%s provider=claude ok=%s summary=%s",
                    thread_ts,
                    health["ok"],
                    health["summary"],
                )
                return

            reply(f"🏓 pong\n• llm: {_format_ping_llm_status(None)}")
            logger.info("Responded with ping health in thread_ts=%s provider=none", thread_ts)
            return

        if _is_usage_help_request(question):
            _set_request_log_route(payload, "usage_help", route_mode="guide")
            reply(_build_usage_help_response(), mention_user=False)
            logger.info("Responded with usage help in thread_ts=%s", thread_ts)
            return

        def _is_claude_allowed_user(target_user_id: str | None) -> bool:
            if not cs.CLAUDE_ALLOWED_USER_IDS:
                return True
            return bool(target_user_id) and target_user_id in cs.CLAUDE_ALLOWED_USER_IDS

        def _timeout_reply_text() -> str:
            provider = (s.LLM_PROVIDER or "").lower().strip()
            if provider == "claude":
                timeout_sec = max(1, s.ANTHROPIC_TIMEOUT_SEC)
                return f"Claude API가 {timeout_sec}초 내 응답하지 않아 AI 답변 생성이 타임아웃됐어"
            timeout_sec = max(1, s.OLLAMA_TIMEOUT_SEC)
            return f"LLM 서버가 {timeout_sec}초 내 응답하지 않아 AI 답변 생성이 타임아웃됐어"

        def _llm_unavailable_reply_text(summary: str | None = None) -> str:
            provider = (s.LLM_PROVIDER or "").lower().strip()
            if provider == "claude":
                base = "Claude API가 응답하지 않아 지금은 AI 답변을 생성할 수 없어"
            else:
                base = "LLM 서버가 응답하지 않아 지금은 AI 답변을 생성할 수 없어"
            detail = (summary or "").strip()
            if not detail:
                return base
            return f"{base}\n• 상태: {detail}"

        def _is_timeout_error(exc: Exception) -> bool:
            lowered = str(exc).lower()
            return "timeout" in lowered or "timed out" in lowered

        def _send_dm_message(target_user_id: str | None, message_text: str) -> bool:
            if not target_user_id or not (message_text or "").strip():
                return False
            try:
                response = client.conversations_open(users=[target_user_id])
                dm_channel = ((response or {}).get("channel") or {}).get("id")
                if not dm_channel:
                    return False
                client.chat_postMessage(channel=dm_channel, text=message_text)
                return True
            except Exception:
                logger.exception("Failed to send DM to user=%s", target_user_id)
                return False

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

            if (
                normalized_fallback.startswith("*바코드 로그")
                and not normalized_synth.startswith("*바코드 로그")
            ) or (
                normalized_fallback.startswith("*로그 분석 결과")
                and not normalized_synth.startswith("*로그 분석 결과")
            ):
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

        def _needs_recording_failure_analysis_fallback(
            synthesized: str,
            fallback_text: str,
            route_name: str,
        ) -> bool:
            if route_name != "recording failure analysis":
                return False

            normalized_synth = (synthesized or "").strip()
            normalized_fallback = (fallback_text or "").strip()
            required_bullets = (
                "• 핵심 원인:",
                "• 운영 근거:",
                "• 영향:",
                "• 권장 조치:",
                "• 확실도:",
            )
            reasoning_leak_tokens = (
                "</think>",
                "<think>",
                "let me ",
                "i need to",
                "the user",
                "based on",
                "looking at",
                "now, checking",
                "wait,",
                "wait ",
                "for the ",
                "the error",
            )

            if normalized_fallback.startswith("*녹화 실패 원인 분석*") and not normalized_synth.startswith("*녹화 실패 원인 분석*"):
                return True

            lowered = normalized_synth.lower()
            if any(token in lowered for token in reasoning_leak_tokens):
                return True

            for bullet in required_bullets:
                if bullet in normalized_fallback and bullet not in normalized_synth:
                    return True

            if "캡처보드" in normalized_fallback and "캡처보드" not in normalized_synth:
                return True

            return False

        def _needs_device_audio_probe_fallback(
            synthesized: str,
            fallback_text: str,
            route_name: str,
        ) -> bool:
            if route_name != "device audio probe":
                return False

            normalized_synth = (synthesized or "").strip()
            normalized_fallback = (fallback_text or "").strip()
            required_bullets = (
                "• 장비:",
                "• 판정:",
                "• 근거:",
                "• 안내:",
            )

            if normalized_fallback.startswith("*장비 소리 출력 점검*") and not normalized_synth.startswith("*장비 소리 출력 점검*"):
                return True

            for bullet in required_bullets:
                if bullet in normalized_fallback and bullet not in normalized_synth:
                    return True

            return False

        def _attach_notion_playbooks_to_evidence(
            evidence_payload: dict[str, Any] | None,
        ) -> list[dict[str, Any]]:
            if not isinstance(evidence_payload, dict):
                return []

            existing = evidence_payload.get("notionPlaybooks")
            if isinstance(existing, list) and existing:
                return [item for item in existing if isinstance(item, dict)]
            return []

        def _reply_with_retrieval_synthesis(
            fallback_text: str,
            evidence_payload: dict[str, Any],
            route_name: str,
            *,
            max_tokens: int | None = None,
        ) -> None:
            _set_request_log_route(payload, route_name, handler_type="router")
            notion_playbooks = _attach_notion_playbooks_to_evidence(evidence_payload)
            evidence_route = str(evidence_payload.get("route") or "").strip().lower()
            company_notion_docs: list[dict[str, str]] = []
            if evidence_route == "notion_playbook_qa":
                request_payload = evidence_payload.get("request") if isinstance(evidence_payload.get("request"), dict) else {}
                notion_link_query = str(request_payload.get("contextualQuestion") or question).strip() or question
                company_notion_docs = select_company_notion_doc_links(
                    notion_link_query,
                    notion_playbooks=notion_playbooks,
                    max_results=3,
                )
                fallback_with_references = _append_company_notion_doc_section(
                    fallback_text,
                    company_notion_docs,
                )
            else:
                fallback_with_references = _append_notion_playbook_section(
                    fallback_text,
                    notion_playbooks,
                )
            prefer_fallback_on_timeout = evidence_route == "notion_playbook_qa"

            if route_name == "barcode log analysis":
                chunks = _split_barcode_log_reply(fallback_with_references)
                if not chunks:
                    reply(fallback_with_references)
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
                reply(fallback_with_references)
                logger.info("Responded with %s (direct)", route_name)
                return
            if provider not in {"claude", "ollama"}:
                reply(fallback_with_references)
                logger.info("Responded with %s (direct, unsupported provider=%s)", route_name, provider)
                return
            if provider == "ollama":
                health = _check_ollama_health()
                if not health["ok"]:
                    reply(fallback_with_references)
                    logger.warning(
                        "Responded with %s (direct, ollama unavailable=%s)",
                        route_name,
                        health["summary"],
                    )
                    return
            if provider == "claude":
                if claude_client is None:
                    reply(fallback_with_references)
                    logger.info("Responded with %s (direct, claude client unavailable)", route_name)
                    return
                if not _is_claude_allowed_user(user_id):
                    reply(fallback_with_references)
                    logger.info(
                        "Responded with %s (direct, claude synthesis not allowed for user=%s)",
                        route_name,
                        user_id,
                    )
                    return

            try:
                thread_context = ""
                if evidence_route == "notion_playbook_qa" or s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT:
                    thread_context = _load_slack_thread_context(
                        client,
                        logger,
                        channel_id,
                        thread_ts,
                        current_ts,
                    )
                if evidence_route == "notion_playbook_qa":
                    thread_context = _resolve_notion_doc_thread_context(question, thread_context)
                synthesized_text = _synthesize_retrieval_answer(
                    question=question,
                    thread_context=thread_context,
                    evidence_payload=evidence_payload,
                    provider=provider,
                    claude_client=claude_client,
                    system_prompt=cs.RETRIEVAL_SYSTEM_PROMPT or None,
                    extra_rules=_build_company_retrieval_rules(evidence_payload),
                    evidence_transform=_transform_company_retrieval_payload,
                    max_tokens=max_tokens,
                )
                synthesized_text = _normalize_notion_doc_answer_style(synthesized_text, route_name)
                final_text = synthesized_text or fallback_with_references
                if "다른 바코드" in final_text and "다른 바코드" not in fallback_text:
                    final_text = fallback_with_references
                if "다른 barcode" in final_text and "다른 barcode" not in fallback_text:
                    final_text = fallback_with_references
                if _needs_barcode_log_fallback(final_text, fallback_text, route_name):
                    final_text = fallback_with_references
                if _needs_recording_failure_analysis_fallback(final_text, fallback_text, route_name):
                    final_text = fallback_with_references
                if _needs_device_audio_probe_fallback(final_text, fallback_text, route_name):
                    final_text = fallback_with_references
                if _needs_notion_doc_fallback(final_text, route_name, fallback_text):
                    final_text = fallback_with_references
                if _needs_notion_doc_security_refusal(final_text, route_name):
                    final_text = _build_notion_doc_security_refusal()
                elif evidence_route == "notion_playbook_qa":
                    final_text = _append_company_notion_doc_section(final_text, company_notion_docs)
                else:
                    final_text = _append_notion_playbook_section(final_text, notion_playbooks)
                reply(final_text)
                logger.info(
                    "Responded with %s (%s) in thread_ts=%s",
                    route_name,
                    "synthesized" if synthesized_text else "direct_fallback",
                    thread_ts,
                )
            except TimeoutError:
                logger.warning("Retrieval synthesis timeout for route=%s", route_name)
                reply(fallback_with_references if prefer_fallback_on_timeout else _timeout_reply_text())
            except RuntimeError as exc:
                if _is_timeout_error(exc):
                    logger.warning("Retrieval synthesis timeout for route=%s", route_name)
                    reply(fallback_with_references if prefer_fallback_on_timeout else _timeout_reply_text())
                    return
                logger.exception("Retrieval synthesis failed for route=%s", route_name)
                reply(fallback_with_references)
            except Exception:
                logger.exception("Retrieval synthesis failed for route=%s", route_name)
                reply(fallback_with_references)

        def _iter_barcode_log_error_summary_sessions(summary_payload: dict[str, Any]) -> list[dict[str, Any]]:
            request = summary_payload.get("request") if isinstance(summary_payload, dict) else {}
            records = summary_payload.get("records") if isinstance(summary_payload, dict) else []
            barcode = str((request or {}).get("barcode") or "미확인").strip() or "미확인"
            session_entries: list[dict[str, Any]] = []
            if not isinstance(records, list):
                return session_entries

            for record in records:
                if not isinstance(record, dict):
                    continue
                session_details = record.get("sessionDetails")
                if not isinstance(session_details, list):
                    continue
                for detail in session_details:
                    if not isinstance(detail, dict):
                        continue
                    session_entries.append(
                        {
                            "barcode": barcode,
                            "deviceName": str(record.get("deviceName") or "미확인").strip() or "미확인",
                            "hospitalName": str(record.get("hospitalName") or "미확인").strip() or "미확인",
                            "roomName": str(record.get("roomName") or "미확인").strip() or "미확인",
                            "date": str(record.get("date") or (request or {}).get("date") or "미확인").strip() or "미확인",
                            "recordingsOnDateCount": int(record.get("recordingsOnDateCount") or 0),
                            "deviceSessionCount": int((record.get("sessions") or {}).get("sessionCount") or 0),
                            "detail": detail,
                        }
                    )
            return session_entries

        def _is_interesting_barcode_log_error_session(session_entry: dict[str, Any]) -> bool:
            detail = session_entry.get("detail") if isinstance(session_entry, dict) else {}
            if not isinstance(detail, dict):
                return False
            video_status = str(detail.get("videoStatus") or detail.get("recordingResult") or "").strip()
            return (
                bool(detail.get("restartDetected"))
                or not bool(detail.get("normalClosed"))
                or int(detail.get("errorLineCount") or 0) > 0
                or (video_status and not _is_normal_video_status(video_status))
            )

        def _build_barcode_log_error_session_record(session_entry: dict[str, Any]) -> dict[str, Any]:
            detail = session_entry.get("detail") if isinstance(session_entry, dict) else {}
            if not isinstance(detail, dict):
                return {}

            normal_closed = bool(detail.get("normalClosed"))
            session_diagnostic = (
                detail.get("sessionDiagnostic") if isinstance(detail.get("sessionDiagnostic"), dict) else {}
            )
            record = {
                "deviceName": session_entry.get("deviceName"),
                "hospitalName": session_entry.get("hospitalName"),
                "roomName": session_entry.get("roomName"),
                "date": session_entry.get("date"),
                "recordingsOnDateCount": int(session_entry.get("recordingsOnDateCount") or 0),
                "sessions": {
                    "sessionCount": 1,
                    "normalCount": 1 if normal_closed else 0,
                    "abnormalCount": 0 if normal_closed else 1,
                },
                "restartDetected": bool(detail.get("restartDetected")),
                "errorLineCount": int(detail.get("errorLineCount") or 0),
                "errorGroups": [
                    group
                    for group in (detail.get("errorGroups") or [])
                    if isinstance(group, dict)
                ],
                "firstFfmpegError": (
                    detail.get("firstFfmpegError") if isinstance(detail.get("firstFfmpegError"), dict) else {}
                ),
                "sessionDiagnostics": [session_diagnostic] if session_diagnostic else [],
            }
            record["classificationTags"] = _classify_record(record)
            return record

        def _build_barcode_log_error_session_section(session_entry: dict[str, Any]) -> list[str]:
            detail = session_entry.get("detail") if isinstance(session_entry, dict) else {}
            if not isinstance(detail, dict):
                return []

            barcode = str(session_entry.get("barcode") or "미확인").strip() or "미확인"
            hospital_name = str(session_entry.get("hospitalName") or "미확인").strip() or "미확인"
            room_name = str(session_entry.get("roomName") or "미확인").strip() or "미확인"
            date_label = str(session_entry.get("date") or "미확인").strip() or "미확인"
            recordings_on_date_count = int(session_entry.get("recordingsOnDateCount") or 0)
            start_time = str(detail.get("startTime") or "시간미상").strip() or "시간미상"
            stop_time = str(detail.get("stopTime") or "미확인").strip() or "미확인"
            stop_token = str(detail.get("stopToken") or "미확인").strip() or "미확인"
            normal_closed = bool(detail.get("normalClosed"))
            restart_detected = bool(detail.get("restartDetected"))
            termination_status = str(
                detail.get("terminationStatus")
                or ("정상 종료" if bool(detail.get("normalClosed")) else "비정상 종료")
            ).strip() or "미확인"
            recording_result = str(detail.get("videoStatus") or detail.get("recordingResult") or "추가 확인 필요").strip() or "추가 확인 필요"
            session_record = _build_barcode_log_error_session_record(session_entry)
            tags = set(session_record.get("classificationTags") or [])
            error_line_count = int(session_record.get("errorLineCount") or 0)
            error_groups = session_record.get("errorGroups") if isinstance(session_record.get("errorGroups"), list) else []
            top_group = _get_top_error_group(session_record)
            top_component = str(top_group.get("component") or "미확인").strip() or "미확인"
            top_signature = str(top_group.get("signature") or "미확인").strip() or "미확인"
            top_count = int(top_group.get("count") or 0)
            first_ffmpeg_error = (
                session_record.get("firstFfmpegError")
                if isinstance(session_record.get("firstFfmpegError"), dict)
                else {}
            )
            ffmpeg_time = str(first_ffmpeg_error.get("timeLabel") or "").strip()
            session_diagnostic = detail.get("sessionDiagnostic") if isinstance(detail.get("sessionDiagnostic"), dict) else {}
            diagnostic_severity = str(session_diagnostic.get("severity") or "").strip()

            first_ffmpeg_text = " ".join(
                str(first_ffmpeg_error.get(key) or "").strip().lower()
                for key in ("message", "raw")
            )
            is_ffmpeg_error = "ffmpeg_error" in tags
            is_standby_ffmpeg_error = "standby error" in first_ffmpeg_text or any(
                "standby error" in str(group.get("signature") or "").strip().lower()
                for group in error_groups
                if isinstance(group, dict)
            )
            is_ffmpeg_timestamp_error = "ffmpeg_timestamp_error" in tags
            is_recording_stalled = "recording_stalled" in tags
            all_network_side_effect_errors = "status_network_error" in tags
            router_cause_hint = _build_cause_line(session_record)

            if restart_detected:
                cause_line = "• 핵심 원인: 세션 중 장비 재시작이 확인돼 정상 녹화 실패로 판단해"
                impact_line = "• 영향: 세션 중 장비 재시작으로 정상 녹화 실패가 발생한 것으로 봐야 해"
            elif not normal_closed:
                cause_line = "• 핵심 원인: 종료 스캔이 없어 세션이 비정상 종료됐어"
                impact_line = "• 영향: 종료 처리가 끝나지 않아 정상 녹화 실패로 봐야 해"
            elif recordings_on_date_count <= 0 and (is_ffmpeg_error or is_recording_stalled or diagnostic_severity == "high"):
                if is_recording_stalled and is_ffmpeg_error:
                    cause_line = "• 핵심 원인: 녹화 중 파일 증가율 저하(stall)와 ffmpeg 종료가 함께 확인됐고 날짜 기준 DB 영상 기록이 없어 녹화 & 업로드 실패로 판단해. 캡처보드 이상 또는 캡처보드 연결 불량을 우선 의심해"
                elif is_recording_stalled:
                    cause_line = "• 핵심 원인: 녹화 중 파일 증가율 저하(stall)가 반복됐고 날짜 기준 DB 영상 기록이 없어 녹화 & 업로드 실패로 판단해. 캡처보드 이상 또는 캡처보드 연결 불량을 우선 의심해"
                else:
                    cause_line = f"• 핵심 원인: {router_cause_hint}"
                impact_line = f"• 영향: 날짜 기준 DB 영상 기록이 `{recordings_on_date_count}개`라 녹화 파일 저장/업로드가 실패한 상태야"
            elif all_network_side_effect_errors and normal_closed and diagnostic_severity != "high":
                if recordings_on_date_count > 0:
                    cause_line = "• 핵심 원인: JWT 갱신/상태 전송/업로드 통신 오류가 있었지만 녹화 실패 원인이라기보다 네트워크/DNS 통신 이상으로 봐야 해"
                    impact_line = f"• 영향: 날짜 기준 DB 영상 기록 `{recordings_on_date_count}개`가 있어 녹화는 성공했고 통신 오류는 별도야"
                else:
                    cause_line = "• 핵심 원인: 업로드/상태 전송 통신 오류가 반복됐고 날짜 기준 DB 영상 기록이 없어 업로드 실패 가능성이 있어"
                    impact_line = "• 영향: 녹화 흐름은 종료됐지만 업로드/상태 전송 단계 실패 가능성이 있어"
            elif diagnostic_severity == "high":
                cause_line = "• 핵심 원인: 종료 처리 지연과 종료 후 장치 오류가 이어져 실제 영상 손상 가능성이 높아"
                impact_line = f"• 영향: 종료는 됐지만 `{recording_result}` 상태로 봐야 해"
            elif is_standby_ffmpeg_error and normal_closed:
                cause_line = "• 핵심 원인: standby ffmpeg 오류가 확인돼 영상 손상 가능성을 의심해야 하고 캡처보드 이상을 우선 점검해야 해"
                impact_line = f"• 영향: 종료는 정상이어도 `{recording_result}` 상태로 봐야 해"
            elif is_ffmpeg_timestamp_error:
                cause_line = "• 핵심 원인: ffmpeg DTS/PTS 타임스탬프 이상이 확인돼 캡처보드 연결 불량 또는 캡처보드 고장을 우선 의심해"
                impact_line = f"• 영향: 종료는 됐지만 `{recording_result}` 상태로 봐야 해"
            elif top_signature != "미확인" and top_count >= 2:
                cause_line = f"• 핵심 원인: `{top_component}` 오류가 반복돼 원인 점검이 필요해"
                impact_line = f"• 영향: error 라인 `{error_line_count}줄`이 확인됐고 `{recording_result}` 상태야"
            elif top_signature != "미확인" and top_count == 1:
                cause_line = f"• 핵심 원인: `{top_component}` 오류가 1회 확인돼 영향 여부 점검이 필요해"
                impact_line = f"• 영향: 종료 상태는 `{termination_status}`인데 영상 상태는 `{recording_result}`야"
            else:
                cause_line = "• 핵심 원인: 운영 근거상 추가 확인이 필요해"
                impact_line = f"• 영향: 현재 판정은 `{recording_result}`이야"

            action_lines: list[str] = []
            if restart_detected:
                action_lines.append("전원 차단/전원 버튼 오입력 여부 확인")
            if is_recording_stalled or is_ffmpeg_timestamp_error or is_standby_ffmpeg_error or is_ffmpeg_error:
                action_lines.append("캡처보드 연결 상태와 입력 신호 점검")
            if is_recording_stalled:
                action_lines.append("저장 경로 쓰기 상태와 파일 증가율 저하 원인 확인")
            if top_signature != "미확인":
                action_lines.append(f"{top_component} 관련 장치/프로세스 상태 확인")
            if not action_lines:
                action_lines.append("동일 시각 장비 상태와 관련 프로세스 로그 확인")

            time_label = f"{start_time} ~ {stop_time}" if stop_time != "미확인" else start_time
            if ffmpeg_time:
                time_label = f"{time_label} (첫 ffmpeg 오류 {ffmpeg_time})"
            lines = [
                f"• 바코드: `{barcode}` | 병원: `{hospital_name}` | 병실: `{room_name}` | 날짜: `{date_label}` | 시간: `{time_label}`",
                cause_line,
                impact_line,
            ]
            lines.append(f"• 조치: {' / '.join(action_lines[:3])}")
            return lines

        def _build_barcode_log_error_summary_session_payload(
            summary_payload: dict[str, Any],
            session_entry: dict[str, Any],
        ) -> dict[str, Any]:
            request = summary_payload.get("request") if isinstance(summary_payload, dict) else {}
            detail = session_entry.get("detail") if isinstance(session_entry, dict) else {}
            if not isinstance(request, dict) or not isinstance(detail, dict):
                return {}

            session_record = _build_barcode_log_error_session_record(session_entry)
            error_groups = session_record.get("errorGroups") if isinstance(session_record.get("errorGroups"), list) else []
            session_diagnostic = (
                detail.get("sessionDiagnostic") if isinstance(detail.get("sessionDiagnostic"), dict) else {}
            )
            representative_error_group = _get_top_error_group(session_record)
            time_range = str(detail.get("startTime") or "시간미상").strip() or "시간미상"
            stop_time = str(detail.get("stopTime") or "미확인").strip() or "미확인"
            if stop_time != "미확인":
                time_range = f"{time_range} ~ {stop_time}"

            payload = {
                "route": "barcode_log_error_summary_session",
                "source": summary_payload.get("source"),
                "request": {
                    "mode": request.get("mode"),
                    "barcode": request.get("barcode"),
                    "date": session_entry.get("date"),
                },
                "session": {
                    "barcode": session_entry.get("barcode"),
                    "deviceName": session_entry.get("deviceName"),
                    "hospitalName": session_entry.get("hospitalName"),
                    "roomName": session_entry.get("roomName"),
                    "date": session_entry.get("date"),
                    "time": time_range,
                    "sessionIndex": detail.get("index"),
                    "stopToken": detail.get("stopToken"),
                    "normalClosed": detail.get("normalClosed"),
                    "restartDetected": detail.get("restartDetected"),
                    "terminationStatus": detail.get("terminationStatus"),
                    "videoStatus": detail.get("videoStatus"),
                    "recordingResult": detail.get("recordingResult"),
                    "recordingsOnDateCount": session_entry.get("recordingsOnDateCount"),
                    "errorLineCount": detail.get("errorLineCount"),
                    "firstFfmpegError": detail.get("firstFfmpegError"),
                    "classificationTags": session_record.get("classificationTags") or [],
                    "routerCauseHint": _build_cause_line(session_record),
                    "representativeErrorGroup": {
                        "component": representative_error_group.get("component"),
                        "signature": representative_error_group.get("signature"),
                        "count": representative_error_group.get("count"),
                        "sampleTime": representative_error_group.get("sampleTime"),
                        "sampleMessage": representative_error_group.get("sampleMessage"),
                    },
                    "errorGroups": [
                        {
                            "component": group.get("component"),
                            "signature": group.get("signature"),
                            "count": group.get("count"),
                            "sampleTime": group.get("sampleTime"),
                            "sampleMessage": group.get("sampleMessage"),
                        }
                        for group in error_groups[:6]
                        if isinstance(group, dict)
                    ],
                    "sessionDiagnostic": {
                        "severity": session_diagnostic.get("severity"),
                        "finishDelay": session_diagnostic.get("finishDelay"),
                        "postStopScanCount": session_diagnostic.get("postStopScanCount"),
                        "postStopStopCount": session_diagnostic.get("postStopStopCount"),
                        "postStopSnapCount": session_diagnostic.get("postStopSnapCount"),
                        "postStopDeviceErrorCount": session_diagnostic.get("postStopDeviceErrorCount"),
                        "displayText": session_diagnostic.get("displayText"),
                    },
                },
            }
            return payload

        def _build_barcode_log_error_summary_fallback(summary_payload: dict[str, Any]) -> str:
            summary = summary_payload.get("summary") if isinstance(summary_payload, dict) else None
            if not isinstance(summary, dict):
                return ""

            session_entries = _iter_barcode_log_error_summary_sessions(summary_payload)
            interesting_entries = [entry for entry in session_entries if _is_interesting_barcode_log_error_session(entry)]
            if not interesting_entries:
                interesting_entries = session_entries
            if not interesting_entries:
                return ""

            lines = ["*세션별 에러 분석*"]
            for session_entry in interesting_entries:
                section_lines = _build_barcode_log_error_session_section(session_entry)
                if not section_lines:
                    continue
                lines.append("")
                lines.extend(section_lines)
            return "\n".join(lines).strip()

        def _is_bad_barcode_log_error_summary_session(text: str) -> bool:
            normalized = (text or "").strip()
            if not normalized:
                return True

            required_markers = ("• 바코드:", "• 핵심 원인:", "• 영향:", "• 조치:")
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

        def _needs_barcode_log_error_summary_session_fallback(
            synthesized: str,
            session_payload: dict[str, Any],
        ) -> bool:
            if _is_bad_barcode_log_error_summary_session(synthesized):
                return True

            session = session_payload.get("session") if isinstance(session_payload, dict) else {}
            if not isinstance(session, dict):
                return False

            tags = {
                str(tag).strip()
                for tag in (session.get("classificationTags") or [])
                if str(tag).strip()
            }
            recordings_on_date_count = int(session.get("recordingsOnDateCount") or 0)
            normalized = (synthesized or "").strip()
            lowered = normalized.lower()

            if recordings_on_date_count <= 0 and tags.intersection({"ffmpeg_error", "ffmpeg_sigterm", "recording_stalled"}):
                if "녹화 & 업로드 실패" not in normalized:
                    return True
                if not any(token in normalized for token in ("ffmpeg", "SIGTERM", "sigterm", "stall", "캡처보드", "영상 입력")):
                    return True
                if "recording_stalled" in tags and "캡처보드" not in normalized:
                    return True

            representative = session.get("representativeErrorGroup")
            if isinstance(representative, dict):
                representative_text = " ".join(
                    str(representative.get(key) or "").strip().lower()
                    for key in ("component", "signature")
                )
                if any(token in representative_text for token in ("ffmpeg", "sigterm", "recording may be stalled", "stalled")):
                    if "app 오류" in normalized and not any(
                        token in lowered for token in ("ffmpeg", "sigterm", "stall")
                    ):
                        return True

            return False

        def _reply_with_barcode_log_error_summary(summary_payload: dict[str, Any] | None) -> None:
            if not isinstance(summary_payload, dict):
                return

            summary = summary_payload.get("summary")
            if not isinstance(summary, dict):
                return

            error_line_count = int(summary.get("errorLineCount") or 0)
            abnormal_session_count = int(summary.get("abnormalSessionCount") or 0)
            restart_event_count = int(summary.get("restartEventCount") or 0)
            if error_line_count <= 0 and abnormal_session_count <= 0 and restart_event_count <= 0:
                return

            session_entries = _iter_barcode_log_error_summary_sessions(summary_payload)
            interesting_entries = [entry for entry in session_entries if _is_interesting_barcode_log_error_session(entry)]
            if not interesting_entries:
                interesting_entries = session_entries
            if not interesting_entries:
                return

            fallback_text = _build_barcode_log_error_summary_fallback(summary_payload)

            def _build_rendered_fallback_sections() -> list[str]:
                sections: list[str] = []
                for session_entry in interesting_entries:
                    session_payload = _build_barcode_log_error_summary_session_payload(summary_payload, session_entry)
                    if not session_payload:
                        continue
                    fallback_section = "\n".join(_build_barcode_log_error_session_section(session_entry)).strip()
                    if not fallback_section:
                        continue
                    session_playbooks = _attach_notion_playbooks_to_evidence(session_payload)
                    sections.append(_append_notion_playbook_section(fallback_section, session_playbooks))
                return sections

            provider = (s.LLM_PROVIDER or "").lower().strip()
            if not s.LLM_SYNTHESIS_ENABLED or not question:
                rendered_sections = _build_rendered_fallback_sections()
                final_text = fallback_text
                if rendered_sections:
                    final_text = "*세션별 에러 분석*\n\n" + "\n\n".join(rendered_sections)
                reply(final_text, mention_user=False)
                logger.info("Responded with barcode log error summary (direct)")
                return
            if provider not in {"claude", "ollama"}:
                rendered_sections = _build_rendered_fallback_sections()
                final_text = fallback_text
                if rendered_sections:
                    final_text = "*세션별 에러 분석*\n\n" + "\n\n".join(rendered_sections)
                reply(final_text, mention_user=False)
                logger.info(
                    "Responded with barcode log error summary (direct, unsupported provider=%s)",
                    provider,
                )
                return
            if provider == "ollama":
                health = _check_ollama_health()
                if not health["ok"]:
                    rendered_sections = _build_rendered_fallback_sections()
                    final_text = fallback_text
                    if rendered_sections:
                        final_text = "*세션별 에러 분석*\n\n" + "\n\n".join(rendered_sections)
                    reply(final_text, mention_user=False)
                    logger.warning(
                        "Responded with barcode log error summary (direct, ollama unavailable=%s)",
                        health["summary"],
                    )
                    return
            if provider == "claude":
                if claude_client is None:
                    rendered_sections = _build_rendered_fallback_sections()
                    final_text = fallback_text
                    if rendered_sections:
                        final_text = "*세션별 에러 분석*\n\n" + "\n\n".join(rendered_sections)
                    reply(final_text, mention_user=False)
                    logger.info("Responded with barcode log error summary (direct, claude client unavailable)")
                    return
                if not _is_claude_allowed_user(user_id):
                    rendered_sections = _build_rendered_fallback_sections()
                    final_text = fallback_text
                    if rendered_sections:
                        final_text = "*세션별 에러 분석*\n\n" + "\n\n".join(rendered_sections)
                    reply(final_text, mention_user=False)
                    logger.info(
                        "Responded with barcode log error summary (direct, claude synthesis not allowed for user=%s)",
                        user_id,
                    )
                    return

            try:
                thread_context = ""
                if s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT:
                    thread_context = _load_slack_thread_context(
                        client,
                        logger,
                        channel_id,
                        thread_ts,
                        current_ts,
                    )
                rendered_sections: list[str] = []
                for session_entry in interesting_entries:
                    session_payload = _build_barcode_log_error_summary_session_payload(summary_payload, session_entry)
                    if not session_payload:
                        continue
                    fallback_section = "\n".join(_build_barcode_log_error_session_section(session_entry)).strip()
                    if not fallback_section:
                        continue
                    session_playbooks = _attach_notion_playbooks_to_evidence(session_payload)
                    fallback_section = _append_notion_playbook_section(fallback_section, session_playbooks)
                    synthesized_text = _synthesize_retrieval_answer(
                        question=question,
                        thread_context=thread_context,
                        evidence_payload=session_payload,
                        provider=provider,
                        claude_client=claude_client,
                        system_prompt=cs.RETRIEVAL_SYSTEM_PROMPT or None,
                        extra_rules=_build_company_retrieval_rules(session_payload),
                        evidence_transform=_transform_company_retrieval_payload,
                        max_tokens=cs.BARCODE_LOG_ERROR_SUMMARY_MAX_TOKENS,
                    )
                    final_section = synthesized_text or fallback_section
                    if _needs_barcode_log_error_summary_session_fallback(final_section, session_payload):
                        final_section = fallback_section
                    final_section = _append_notion_playbook_section(final_section, session_playbooks)
                    rendered_sections.append(final_section)

                final_text = "*세션별 에러 분석*"
                if rendered_sections:
                    final_text = "*세션별 에러 분석*\n\n" + "\n\n".join(rendered_sections)
                if not rendered_sections:
                    final_text = fallback_text
                reply(final_text.strip(), mention_user=False)
                logger.info(
                    "Responded with barcode log error summary (%s sections) in thread_ts=%s",
                    len(rendered_sections),
                    thread_ts,
                )
            except TimeoutError:
                logger.warning("Barcode log error summary timeout")
                rendered_sections = _build_rendered_fallback_sections()
                final_text = fallback_text
                if rendered_sections:
                    final_text = "*세션별 에러 분석*\n\n" + "\n\n".join(rendered_sections)
                reply(final_text, mention_user=False)
            except RuntimeError as exc:
                if _is_timeout_error(exc):
                    logger.warning("Barcode log error summary timeout")
                    rendered_sections = _build_rendered_fallback_sections()
                    final_text = fallback_text
                    if rendered_sections:
                        final_text = "*세션별 에러 분석*\n\n" + "\n\n".join(rendered_sections)
                    reply(final_text, mention_user=False)
                    return
                logger.exception("Barcode log error summary synthesis failed")
                rendered_sections = _build_rendered_fallback_sections()
                final_text = fallback_text
                if rendered_sections:
                    final_text = "*세션별 에러 분석*\n\n" + "\n\n".join(rendered_sections)
                reply(final_text, mention_user=False)
            except Exception:
                logger.exception("Barcode log error summary synthesis failed")
                rendered_sections = _build_rendered_fallback_sections()
                final_text = fallback_text
                if rendered_sections:
                    final_text = "*세션별 에러 분석*\n\n" + "\n\n".join(rendered_sections)
                reply(final_text, mention_user=False)

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
        request_log_query = _extract_request_log_query(question)

        def _is_request_log_query_allowed(target_user_id: str | None) -> bool:
            if not cs.REQUEST_LOG_QUERY_ALLOWED_USER_IDS:
                return True
            return bool(target_user_id) and target_user_id in cs.REQUEST_LOG_QUERY_ALLOWED_USER_IDS

        if request_log_query is not None:
            _set_request_log_route(
                payload,
                "request log query",
                route_mode=request_log_query.mode,
                requested_date=request_log_query.target_date,
                subject_type="request_log",
            )
            _merge_request_log_metadata(
                payload,
                queryMode=request_log_query.mode,
                queryScope=request_log_query.scope_label,
                queryLimit=request_log_query.limit,
            )
            if not s.REQUEST_LOG_SQLITE_ENABLED:
                reply("요청 로그 저장 기능이 꺼져 있어. .env에서 REQUEST_LOG_SQLITE_ENABLED=true로 설정해줘")
                return
            if not _is_request_log_query_allowed(user_id):
                approval_text = "요청 로그 조회는 권한이 필요해"
                if cs.DD_USER_ID:
                    approval_text = f"요청 로그 조회는 <@{cs.DD_USER_ID}> 승인이 필요해"
                reply(approval_text, mention_user=False)
                logger.info(
                    "Rejected request log query for unauthorized user=%s mode=%s date=%s",
                    user_id,
                    request_log_query.mode,
                    request_log_query.target_date,
                )
                return
            try:
                result_text = _query_request_log_text(request_log_query)
                reply(result_text)
                logger.info(
                    "Responded with request log query in thread_ts=%s user=%s mode=%s date=%s limit=%s",
                    thread_ts,
                    user_id,
                    request_log_query.mode,
                    request_log_query.target_date,
                    request_log_query.limit,
                )
            except Exception:
                logger.exception("Request log query failed")
                reply("요청 로그 조회 중 오류가 발생했어. SQLite 파일과 권한 상태를 확인해줘")
            return

        barcode = _extract_barcode(question)
        phase2_hospital_name, phase2_room_name = _extract_hospital_room_scope(question)
        has_phase2_scope = bool(phase2_hospital_name and phase2_room_name)
        phase2_has_requested_date = False
        thread_context_for_scope = ""

        if has_phase2_scope:
            try:
                _, phase2_has_requested_date = _extract_log_date_with_presence(question)
            except ValueError:
                phase2_has_requested_date = True

        if has_phase2_scope and phase2_has_requested_date:
            thread_context_for_scope = _load_slack_thread_context(
                client,
                logger,
                channel_id,
                thread_ts,
                current_ts,
            )

        if not barcode and has_phase2_scope and phase2_has_requested_date:
            recovered_barcode = _extract_latest_barcode_from_thread_context(thread_context_for_scope)
            if recovered_barcode:
                barcode = recovered_barcode
                logger.info(
                    "Recovered barcode from thread context for phase2 scope follow-up in thread_ts=%s barcode=%s",
                    thread_ts,
                    barcode,
                )
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

        is_phase2_scope_followup = bool(barcode and has_phase2_scope and phase2_has_requested_date)
        is_failure_phase2_scope_followup = bool(
            barcode
            and has_phase2_scope
            and phase2_has_requested_date
            and _has_recording_failure_analysis_hints(thread_context_for_scope)
        )

        if _is_barcode_device_file_probe_request(question, barcode):
            if not s.S3_QUERY_ENABLED:
                reply("파일 확인 대상 세션 조회를 위해 S3_QUERY_ENABLED=true가 필요해")
                return
            if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
                reply("파일 확인 대상 세션 조회를 위해 DB 접속 정보(DB_*)가 필요해")
                return

            probe_remote_files = _should_probe_device_files(question)
            download_remote_files = _should_download_device_files(question)
            recover_remote_files = _should_recover_device_files(question)
            compact_file_id = _should_render_compact_file_id_result(question)
            compact_file_list = _should_render_compact_device_file_list(question)
            compact_download = _should_render_compact_device_download_result(question)
            compact_recovery = _should_render_compact_device_recovery_result(question)
            if recover_remote_files and not cs.DEVICE_FILE_RECOVERY_ENABLED:
                reply("장비 영상 복구 기능은 현재 비활성화돼 있어")
                return
            if probe_remote_files and (
                not cs.MDA_GRAPHQL_URL
                or not cs.MDA_ADMIN_USER_PASSWORD
                or not cs.DEVICE_SSH_PASSWORD
            ):
                reply(_build_device_file_probe_config_message())
                return
            if download_remote_files and (
                not cs.DEVICE_FILE_DOWNLOAD_BUCKET
                or not cs.MDA_GRAPHQL_URL
                or not cs.MDA_ADMIN_USER_PASSWORD
                or not cs.DEVICE_SSH_PASSWORD
            ):
                reply(_build_device_file_download_config_message())
                return
            if recover_remote_files and (
                not cs.BOX_UPLOADER_BASE_URL
                or not cs.MDA_GRAPHQL_URL
                or not cs.MDA_ADMIN_USER_PASSWORD
                or not cs.DEVICE_SSH_PASSWORD
                or not cs.UPLOADER_JWT_SECRET
            ):
                reply(_build_device_file_recovery_config_message())
                return

            try:
                log_date, has_requested_date = _extract_log_date_with_presence(question)
                if not has_requested_date:
                    reply("파일 확인 대상 세션 조회는 날짜가 필요해. 예: `48194663047 2026-03-06 파일 있나`")
                    return

                context = _get_recordings_context()
                summary = context.get("summary") or {}
                recording_count = int(summary.get("recordingCount") or 0)
                has_device_mapping = _has_recordings_device_mapping(context)
                manual_device_contexts = None

                if phase2_hospital_name and phase2_room_name:
                    manual_device_contexts = _lookup_device_contexts_by_hospital_room(
                        phase2_hospital_name,
                        phase2_room_name,
                    )
                    if not manual_device_contexts:
                        reply(_build_device_file_scope_request_message(
                            barcode or "",
                            "입력한 병원명/병실명으로 장비를 찾지 못했어. MDA 표시 이름과 정확히 일치하게 입력해줘",
                        ))
                        return
                elif recording_count <= 0 or not has_device_mapping:
                    reply(_build_device_file_scope_request_message(
                        barcode or "",
                        "recordings 장비 매핑이 없어 2차 입력이 필요해",
                    ))
                    return

                result_text, probe_payload = _locate_barcode_file_candidates(
                    _get_s3_client(),
                    barcode or "",
                    log_date,
                    recordings_context=context,
                    device_contexts=manual_device_contexts,
                    probe_remote_files=probe_remote_files,
                    download_remote_files=download_remote_files,
                    recover_remote_files=recover_remote_files,
                    compact_file_list=compact_file_list,
                    compact_file_id=compact_file_id,
                    compact_download=compact_download,
                    compact_recovery=compact_recovery,
                )
                if download_remote_files:
                    download_records = _collect_device_download_records(probe_payload)
                    if download_records:
                        dm_text = _render_device_download_dm_text(
                            barcode or "",
                            log_date,
                            download_records,
                        )
                        if _send_dm_message(user_id, dm_text):
                            requester_name = _load_slack_user_name(
                                client,
                                workspace_id,
                                user_id,
                                logger,
                            )
                            logged_count = _log_device_download_activity(
                                records=download_records,
                                barcode=barcode or "",
                                log_date=log_date,
                                question=question,
                                user_id=user_id,
                                user_name=requester_name,
                                channel_id=channel_id,
                                thread_ts=thread_ts,
                                logger=logger,
                            )
                            thread_notice = _render_device_download_thread_notice(
                                barcode or "",
                                log_date,
                                download_records,
                                activity_logged=logged_count > 0,
                                used_expanded_scope=bool(
                                    ((probe_payload.get("request") or {}).get("usedExpandedScope"))
                                ),
                            )
                            reply(thread_notice)
                        else:
                            failure_notice = _render_device_download_dm_failure_notice(
                                barcode or "",
                                log_date,
                                download_records,
                                used_expanded_scope=bool(
                                    ((probe_payload.get("request") or {}).get("usedExpandedScope"))
                                ),
                            )
                            reply(failure_notice)
                    else:
                        reply(result_text)
                else:
                    reply(result_text)
                logger.info(
                    "Responded with device file candidate lookup in thread_ts=%s barcode=%s records=%s",
                    thread_ts,
                    barcode,
                    int(((probe_payload.get("summary") or {}).get("recordCount") or 0)),
                )
            except ValueError as exc:
                reply(f"파일 확인 대상 세션 조회 요청 형식 오류: {exc}")
            except (BotoCoreError, ClientError, pymysql.MySQLError, RuntimeError):
                logger.exception("Device file candidate lookup failed")
                reply("파일 확인 대상 세션 조회 중 오류가 발생했어. S3/DB 설정을 확인해줘")
            except Exception:
                logger.exception("Device file candidate lookup failed")
                reply("파일 확인 대상 세션 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_recording_failure_analysis_request(question, barcode) or is_failure_phase2_scope_followup:
            if not s.S3_QUERY_ENABLED:
                reply("녹화 실패 원인 분석을 위해 S3_QUERY_ENABLED=true가 필요해")
                return

            if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
                reply("녹화 실패 원인 분석을 위해 DB 접속 정보(DB_*)가 필요해")
                return

            try:
                log_date, has_requested_date = _extract_log_date_with_presence(question)
                context = _get_recordings_context()
                summary = context.get("summary") or {}
                recording_count = int(summary.get("recordingCount") or 0)
                has_device_mapping = _has_recordings_device_mapping(context)
                used_manual_scope = False
                analysis_mode = "phase1_window"
                result_text = ""
                log_analysis_payload: dict[str, Any] | None = None

                if has_requested_date:
                    if recording_count <= 0 or not has_device_mapping:
                        if not phase2_hospital_name or not phase2_room_name:
                            reply(
                                _build_phase2_scope_request_message(
                                    barcode or "",
                                    "recordings 장비 매핑이 없어 2차 입력이 필요해",
                                    "*녹화 실패 원인 분석*",
                                    example_action="녹화 실패 원인 분석",
                                )
                            )
                            logger.info(
                                "Responded with recording failure scope guidance in thread_ts=%s barcode=%s mode=scope_required",
                                thread_ts,
                                barcode,
                            )
                            return

                        manual_device_contexts = _lookup_device_contexts_by_hospital_room(
                            phase2_hospital_name,
                            phase2_room_name,
                        )
                        if not manual_device_contexts:
                            reply(
                                _build_phase2_scope_request_message(
                                    barcode or "",
                                    "입력한 병원명/병실명으로 장비를 찾지 못했어. MDA 표시 이름과 정확히 일치하게 입력해줘",
                                    "*녹화 실패 원인 분석*",
                                    example_action="녹화 실패 원인 분석",
                                )
                            )
                            logger.info(
                                "Responded with recording failure scope guidance in thread_ts=%s barcode=%s mode=scope_not_found",
                                thread_ts,
                                barcode,
                            )
                            return

                        used_manual_scope = True
                        analysis_mode = "error_manual_scope"
                        result_text, log_analysis_payload = _analyze_barcode_log_errors(
                            _get_s3_client(),
                            barcode or "",
                            log_date,
                            recordings_context=context,
                            device_contexts=manual_device_contexts,
                        )
                    else:
                        analysis_mode = "error"
                        result_text, log_analysis_payload = _analyze_barcode_log_errors(
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
                    if "• 2차 조회를 위해 아래 3가지를 같이 입력해줘:" in result_text:
                        reply(
                            _rewrite_phase2_scope_request_message(
                                result_text,
                                "*녹화 실패 원인 분석*",
                                "녹화 실패 원인 분석",
                            )
                        )
                        logger.info(
                            "Responded with recording failure scope guidance in thread_ts=%s barcode=%s mode=phase1_scope_required",
                            thread_ts,
                            barcode,
                        )
                        return

                failure_evidence = _build_recording_failure_analysis_evidence(
                    question=question,
                    summary_payload=log_analysis_payload,
                )
                failure_thread_context = thread_context_for_scope or _load_slack_thread_context(
                    client,
                    logger,
                    channel_id,
                    thread_ts,
                    current_ts,
                )
                failure_user_thread_text = _extract_user_only_thread_text(failure_thread_context, user_id)
                selector_text = "\n".join(
                    part for part in (failure_user_thread_text, question) if (part or "").strip()
                ).strip()
                failure_evidence, session_scope_message = _narrow_recording_failure_analysis_evidence(
                    failure_evidence,
                    selector_text,
                )
                if session_scope_message:
                    reply(session_scope_message)
                    logger.info(
                        "Responded with recording failure session scope guidance in thread_ts=%s barcode=%s",
                        thread_ts,
                        barcode,
                    )
                    return
                request_payload = failure_evidence.get("request") if isinstance(failure_evidence, dict) else None
                if isinstance(request_payload, dict):
                    request_payload["mode"] = analysis_mode
                    request_payload["phase2HospitalName"] = phase2_hospital_name
                    request_payload["phase2RoomName"] = phase2_room_name
                    request_payload["usedManualScope"] = used_manual_scope
                _attach_recordings_context_to_evidence(failure_evidence, context)
                fallback_text = _render_recording_failure_analysis_fallback(failure_evidence)
                _reply_with_retrieval_synthesis(
                    fallback_text,
                    failure_evidence,
                    route_name="recording failure analysis",
                    max_tokens=cs.RECORDING_FAILURE_ANALYSIS_MAX_TOKENS,
                )
            except ValueError as exc:
                reply(f"녹화 실패 원인 분석 요청 형식 오류: {exc}")
            except (BotoCoreError, ClientError, pymysql.MySQLError, RuntimeError) as exc:
                logger.exception("Recording failure analysis failed")
                reply(_build_dependency_failure_reply("녹화 실패 원인 분석", exc))
            except Exception:
                logger.exception("Recording failure analysis failed")
                reply("녹화 실패 원인 분석 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_barcode_log_analysis_request(question, barcode) or is_phase2_scope_followup:
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
                                "*로그 분석 결과 (2차 수동 범위)*",
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
                                    "*로그 분석 결과 (2차 수동 범위)*",
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
            except (BotoCoreError, ClientError, pymysql.MySQLError, RuntimeError) as exc:
                logger.exception("Barcode log analysis failed")
                reply(_build_dependency_failure_reply("바코드 로그 분석", exc))
            except Exception:
                logger.exception("Barcode log analysis failed")
                reply("바코드 로그 분석 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        try:
            structured_target_date, _ = _extract_optional_requested_date(question)
        except ValueError as exc:
            structured_target_date = None
            structured_date_error = exc
        else:
            structured_date_error = None

        structured_target_year = _extract_year_filter(question)
        if structured_target_year is not None and structured_target_date is None:
            structured_date_error = None
        structured_hospital_name, structured_room_name = _extract_hospital_room_scope(question)
        if not structured_hospital_name:
            structured_hospital_name = _extract_leading_hospital_scope(question)
        structured_hospital_seq, structured_hospital_room_seq = _extract_capture_seq_filters(question)
        structured_device_name = _extract_device_name_scope(question)
        update_device_name = _extract_device_name_for_update(question) or structured_device_name
        audio_probe_device_name = _extract_device_name_for_audio_probe(question) or structured_device_name
        status_probe_device_name = _extract_device_name_for_status_probe(question) or structured_device_name
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
                reply(result_text)
                logger.info(
                    "Responded with hospitals filters in thread_ts=%s date=%s year=%s hospital=%s hospitalSeq=%s",
                    thread_ts,
                    structured_target_date,
                    structured_target_year,
                    structured_hospital_name,
                    structured_hospital_seq,
                )
            except ValueError as exc:
                reply(f"병원 조회 요청 형식 오류: {exc}")
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Hospitals filters query failed")
                reply("병원 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Hospitals filters query failed")
                reply("병원 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

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
                reply(result_text)
                logger.info(
                    "Responded with hospital rooms filters in thread_ts=%s hospital=%s room=%s hospitalSeq=%s hospitalRoomSeq=%s",
                    thread_ts,
                    structured_hospital_name,
                    structured_room_name,
                    structured_hospital_seq,
                    structured_hospital_room_seq,
                )
            except ValueError as exc:
                reply(f"병실 조회 요청 형식 오류: {exc}")
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Hospital rooms filters query failed")
                reply("병실 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Hospital rooms filters query failed")
                reply("병실 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_device_update_status_request(question, device_name=update_device_name):
            if (
                not cs.MDA_GRAPHQL_URL
                or not cs.MDA_ADMIN_USER_PASSWORD
                or not cs.DEVICE_SSH_PASSWORD
            ):
                reply(_build_device_update_config_message())
                return

            try:
                _set_request_log_route(payload, "device update status", handler_type="router")
                result_text, _ = _query_device_update_status(update_device_name or "")
                reply(result_text)
                logger.info(
                    "Responded with device update status in thread_ts=%s deviceName=%s",
                    thread_ts,
                    update_device_name,
                )
            except ValueError as exc:
                reply(f"장비 업데이트 상태 요청 형식 오류: {exc}")
            except RuntimeError as exc:
                logger.exception("Device update status failed")
                reply(_build_dependency_failure_reply("장비 업데이트 상태 확인", exc))
            except Exception:
                logger.exception("Device update status failed")
                reply("장비 업데이트 상태 확인 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_device_box_update_request(question, device_name=update_device_name):
            if (
                not cs.MDA_GRAPHQL_URL
                or not cs.MDA_ADMIN_USER_PASSWORD
                or not cs.DEVICE_SSH_PASSWORD
            ):
                reply(_build_device_update_config_message())
                return

            try:
                _set_request_log_route(payload, "device box update", handler_type="router")
                def _reply_device_update_notice(notice_text: str) -> None:
                    try:
                        reply(notice_text, mention_user=False)
                    except Exception:
                        logger.exception(
                            "Failed to send device box update progress notice in thread_ts=%s deviceName=%s",
                            thread_ts,
                            update_device_name,
                        )

                result_text, result_payload = _request_device_box_update(
                    question,
                    device_name=update_device_name,
                    on_dispatched=_reply_device_update_notice,
                )
                reply(result_text, mention_user=False)
                if bool(((result_payload.get("dispatch") or {}) if isinstance(result_payload, dict) else {}).get("status")):
                    _log_device_update_activity(
                        question=question,
                        user_id=user_id,
                        channel_id=channel_id,
                        thread_ts=thread_ts,
                        result_payload=result_payload,
                        client=client,
                        logger=logger,
                    )
                logger.info(
                    "Responded with device box update in thread_ts=%s deviceName=%s",
                    thread_ts,
                    update_device_name,
                )
            except ValueError as exc:
                reply(f"장비 박스 업데이트 요청 형식 오류: {exc}")
            except RuntimeError as exc:
                logger.exception("Device box update failed")
                reply(_build_dependency_failure_reply("장비 박스 업데이트", exc))
            except Exception:
                logger.exception("Device box update failed")
                reply("장비 박스 업데이트 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_device_agent_update_request(question, device_name=update_device_name):
            if (
                not cs.MDA_GRAPHQL_URL
                or not cs.MDA_ADMIN_USER_PASSWORD
                or not cs.DEVICE_SSH_PASSWORD
            ):
                reply(_build_device_update_config_message())
                return

            try:
                _set_request_log_route(payload, "device agent update", handler_type="router")
                def _reply_device_update_notice(notice_text: str) -> None:
                    try:
                        reply(notice_text, mention_user=False)
                    except Exception:
                        logger.exception(
                            "Failed to send device agent update progress notice in thread_ts=%s deviceName=%s",
                            thread_ts,
                            update_device_name,
                        )

                result_text, result_payload = _request_device_agent_update(
                    question,
                    device_name=update_device_name,
                    on_dispatched=_reply_device_update_notice,
                )
                reply(result_text, mention_user=False)
                if bool(((result_payload.get("dispatch") or {}) if isinstance(result_payload, dict) else {}).get("status")):
                    _log_device_update_activity(
                        question=question,
                        user_id=user_id,
                        channel_id=channel_id,
                        thread_ts=thread_ts,
                        result_payload=result_payload,
                        client=client,
                        logger=logger,
                    )
                logger.info(
                    "Responded with device agent update in thread_ts=%s deviceName=%s",
                    thread_ts,
                    update_device_name,
                )
            except ValueError as exc:
                reply(f"장비 에이전트 업데이트 요청 형식 오류: {exc}")
            except RuntimeError as exc:
                logger.exception("Device agent update failed")
                reply(_build_dependency_failure_reply("장비 에이전트 업데이트", exc))
            except Exception:
                logger.exception("Device agent update failed")
                reply("장비 에이전트 업데이트 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_device_audio_probe_request(question, device_name=audio_probe_device_name):
            if (
                not cs.MDA_GRAPHQL_URL
                or not cs.MDA_ADMIN_USER_PASSWORD
                or not cs.DEVICE_SSH_PASSWORD
            ):
                reply(_build_device_audio_probe_config_message())
                return

            try:
                result_text, evidence_payload = _probe_device_audio_output(
                    audio_probe_device_name or "",
                )
                _reply_with_retrieval_synthesis(
                    result_text,
                    evidence_payload,
                    route_name="device audio probe",
                    max_tokens=280,
                )
                logger.info(
                    "Responded with device audio probe in thread_ts=%s deviceName=%s",
                    thread_ts,
                    audio_probe_device_name,
                )
            except ValueError as exc:
                reply(f"장비 소리 출력 점검 요청 형식 오류: {exc}")
            except RuntimeError as exc:
                logger.exception("Device audio probe failed")
                reply(_build_dependency_failure_reply("장비 소리 출력 점검", exc))
            except Exception:
                logger.exception("Device audio probe failed")
                reply("장비 소리 출력 점검 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_device_memory_patch_request(question, device_name=status_probe_device_name):
            if (
                not cs.MDA_GRAPHQL_URL
                or not cs.MDA_ADMIN_USER_PASSWORD
                or not cs.DEVICE_SSH_PASSWORD
            ):
                reply(_build_device_memory_patch_config_message())
                return

            try:
                _set_request_log_route(payload, "device memory patch", handler_type="router")
                result_text, _ = _patch_device_pm2_memory(
                    status_probe_device_name or "",
                )
                reply(result_text)
                logger.info(
                    "Responded with device memory patch in thread_ts=%s deviceName=%s",
                    thread_ts,
                    status_probe_device_name,
                )
            except ValueError as exc:
                reply(f"장비 메모리 패치 요청 형식 오류: {exc}")
            except RuntimeError as exc:
                logger.exception("Device memory patch failed")
                reply(_build_dependency_failure_reply("장비 메모리 패치", exc))
            except Exception:
                logger.exception("Device memory patch failed")
                reply("장비 메모리 패치 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_device_pm2_probe_request(question, device_name=status_probe_device_name):
            if (
                not cs.MDA_GRAPHQL_URL
                or not cs.MDA_ADMIN_USER_PASSWORD
                or not cs.DEVICE_SSH_PASSWORD
            ):
                reply(_build_device_status_probe_config_message())
                return

            try:
                _set_request_log_route(payload, "device pm2 probe", handler_type="router")
                result_text, _ = _probe_device_runtime_component(
                    status_probe_device_name or "",
                    component="pm2",
                )
                reply(result_text)
                logger.info(
                    "Responded with device pm2 probe in thread_ts=%s deviceName=%s",
                    thread_ts,
                    status_probe_device_name,
                )
            except ValueError as exc:
                reply(f"장비 PM2 상태 점검 요청 형식 오류: {exc}")
            except RuntimeError as exc:
                logger.exception("Device pm2 probe failed")
                reply(_build_dependency_failure_reply("장비 PM2 상태 점검", exc))
            except Exception:
                logger.exception("Device pm2 probe failed")
                reply("장비 PM2 상태 점검 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_device_captureboard_probe_request(question, device_name=status_probe_device_name):
            if (
                not cs.MDA_GRAPHQL_URL
                or not cs.MDA_ADMIN_USER_PASSWORD
                or not cs.DEVICE_SSH_PASSWORD
            ):
                reply(_build_device_status_probe_config_message())
                return

            try:
                _set_request_log_route(payload, "device captureboard probe", handler_type="router")
                result_text, _ = _probe_device_runtime_component(
                    status_probe_device_name or "",
                    component="captureboard",
                )
                reply(result_text)
                logger.info(
                    "Responded with device captureboard probe in thread_ts=%s deviceName=%s",
                    thread_ts,
                    status_probe_device_name,
                )
            except ValueError as exc:
                reply(f"장비 캡처보드 점검 요청 형식 오류: {exc}")
            except RuntimeError as exc:
                logger.exception("Device captureboard probe failed")
                reply(_build_dependency_failure_reply("장비 캡처보드 점검", exc))
            except Exception:
                logger.exception("Device captureboard probe failed")
                reply("장비 캡처보드 점검 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_device_led_probe_request(question, device_name=status_probe_device_name):
            if (
                not cs.MDA_GRAPHQL_URL
                or not cs.MDA_ADMIN_USER_PASSWORD
                or not cs.DEVICE_SSH_PASSWORD
            ):
                reply(_build_device_status_probe_config_message())
                return

            try:
                _set_request_log_route(payload, "device led probe", handler_type="router")
                result_text, _ = _probe_device_runtime_component(
                    status_probe_device_name or "",
                    component="led",
                )
                reply(result_text)
                logger.info(
                    "Responded with device led probe in thread_ts=%s deviceName=%s",
                    thread_ts,
                    status_probe_device_name,
                )
            except ValueError as exc:
                reply(f"장비 LED 점검 요청 형식 오류: {exc}")
            except RuntimeError as exc:
                logger.exception("Device led probe failed")
                reply(_build_dependency_failure_reply("장비 LED 점검", exc))
            except Exception:
                logger.exception("Device led probe failed")
                reply("장비 LED 점검 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_device_status_probe_request(question, device_name=status_probe_device_name):
            if (
                not cs.MDA_GRAPHQL_URL
                or not cs.MDA_ADMIN_USER_PASSWORD
                or not cs.DEVICE_SSH_PASSWORD
            ):
                reply(_build_device_status_probe_config_message())
                return

            try:
                _set_request_log_route(payload, "device status probe", handler_type="router")
                result_text, _ = _probe_device_status_overview(
                    status_probe_device_name or "",
                )
                reply(result_text)
                logger.info(
                    "Responded with device status probe in thread_ts=%s deviceName=%s",
                    thread_ts,
                    status_probe_device_name,
                )
            except ValueError as exc:
                reply(f"장비 상태 점검 요청 형식 오류: {exc}")
            except RuntimeError as exc:
                logger.exception("Device status probe failed")
                reply(_build_dependency_failure_reply("장비 상태 점검", exc))
            except Exception:
                logger.exception("Device status probe failed")
                reply("장비 상태 점검 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

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
                reply(result_text)
                logger.info(
                    "Responded with devices filters in thread_ts=%s deviceName=%s deviceSeq=%s hospital=%s room=%s hospitalSeq=%s hospitalRoomSeq=%s status=%s activeFlag=%s installFlag=%s",
                    thread_ts,
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
                reply(f"장비 조회 요청 형식 오류: {exc}")
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Devices filters query failed")
                reply("장비 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Devices filters query failed")
                reply("장비 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

        if _is_weekly_recordings_report_request(
            question,
            barcode=barcode,
            target_date=structured_target_date,
        ):
            try:
                if structured_date_error is not None:
                    raise structured_date_error
                _set_request_log_route(
                    payload,
                    "weekly recordings report",
                    route_mode="summary",
                    handler_type="router",
                    requested_date=structured_target_date,
                )
                (
                    result_text,
                    result_blocks,
                    resolved_week_start_date,
                    resolved_week_end_date,
                ) = _build_weekly_recordings_report_reply_payload(
                    target_date=structured_target_date
                )
                if resolved_week_start_date:
                    _set_request_log_route(
                        payload,
                        "weekly recordings report",
                        route_mode="summary",
                        handler_type="router",
                        requested_date=resolved_week_start_date,
                    )
                reply(
                    result_text,
                    mention_user=False,
                    blocks=result_blocks,
                )
                logger.info(
                    "Responded with weekly recordings report in thread_ts=%s week_start=%s week_end=%s",
                    thread_ts,
                    resolved_week_start_date,
                    resolved_week_end_date,
                )
            except ValueError as exc:
                reply(f"주간 영상 현황 요청 형식 오류: {exc}")
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Weekly recordings report query failed")
                reply("주간 영상 현황 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Weekly recordings report query failed")
                reply("주간 영상 현황 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

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
                reply(result_text)
                logger.info(
                    "Responded with ultrasound capture filters in thread_ts=%s barcode=%s date=%s year=%s hospital=%s room=%s hospitalSeq=%s hospitalRoomSeq=%s",
                    thread_ts,
                    barcode,
                    structured_target_date,
                    structured_target_year,
                    structured_hospital_name,
                    structured_room_name,
                    structured_hospital_seq,
                    structured_hospital_room_seq,
                )
            except ValueError as exc:
                reply(f"캡처 조회 요청 형식 오류: {exc}")
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Ultrasound captures query failed")
                reply("캡처 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Ultrasound captures query failed")
                reply("캡처 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
            return

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
                reply(result_text)
                logger.info(
                    "Responded with recordings filters in thread_ts=%s barcode=%s date=%s year=%s hospital=%s room=%s hospitalSeq=%s hospitalRoomSeq=%s",
                    thread_ts,
                    barcode,
                    structured_target_date,
                    structured_target_year,
                    structured_hospital_name,
                    structured_room_name,
                    structured_hospital_seq,
                    structured_hospital_room_seq,
                )
            except ValueError as exc:
                reply(f"영상 조회 요청 형식 오류: {exc}")
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Recordings filters query failed")
                reply("영상 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Recordings filters query failed")
                reply("영상 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
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

        if _is_baby_ai_list_request_without_barcode(question, barcode):
            reply("베이비매직 조회는 바코드가 필요해. 예: `12345678910 베이비매직 목록`")
            logger.info(
                "Responded with baby_ai barcode guidance in thread_ts=%s question=%s",
                thread_ts,
                question,
            )
            return

        if _is_barcode_baby_ai_list_request(question, barcode):
            try:
                target_date, has_requested_date = _extract_log_date_with_presence(question)
                result_text = _query_baby_ai_list_by_barcode(
                    barcode or "",
                    target_date if has_requested_date else None,
                )
                reply(result_text)
                logger.info(
                    "Responded with barcode baby_ai list in thread_ts=%s barcode=%s has_date=%s",
                    thread_ts,
                    barcode,
                    has_requested_date,
                )
            except ValueError as exc:
                reply(f"베이비매직 목록 조회 요청 형식 오류: {exc}")
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Barcode baby_ai list query failed")
                reply("베이비매직 목록 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘")
            except Exception:
                logger.exception("Barcode baby_ai list query failed")
                reply("베이비매직 목록 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘")
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

        notion_thread_context = ""
        is_notion_doc_question = _looks_like_notion_doc_question(question)
        if not is_notion_doc_question and thread_ts:
            notion_thread_context = _load_slack_thread_context(
                client,
                logger,
                channel_id,
                thread_ts,
                current_ts,
            )
            is_notion_doc_question = _looks_like_notion_doc_followup(question, notion_thread_context)

        if is_notion_doc_question:
            _set_request_log_route(payload, "notion playbook qa", handler_type="router")
            try:
                if _is_notion_doc_exfiltration_attempt(question, notion_thread_context):
                    logger.warning(
                        "Blocked notion doc exfiltration attempt in thread_ts=%s question=%s",
                        thread_ts,
                        question,
                    )
                    reply(_build_notion_doc_security_refusal())
                    return
                evidence_payload = {
                    "route": "notion_playbook_qa",
                    "source": "notion",
                    "request": {
                        "question": question,
                    },
                }
                if not notion_thread_context and thread_ts:
                    notion_thread_context = _load_slack_thread_context(
                        client,
                        logger,
                        channel_id,
                        thread_ts,
                        current_ts,
                    )
                notion_query_text = _build_notion_doc_query_text(question, notion_thread_context)
                if notion_query_text and notion_query_text != question:
                    evidence_payload["request"]["contextualQuestion"] = notion_query_text
                notion_references = _select_notion_references(
                    notion_query_text or question,
                    evidence_payload=evidence_payload,
                    max_results=3,
                )
                if notion_references:
                    sanitized_references = _sanitize_notion_references_for_llm(notion_references)
                    evidence_payload["notionPlaybooks"] = sanitized_references
                    evidence_payload["notionReferences"] = sanitized_references
                    fallback_text = _build_notion_doc_fallback(question, sanitized_references)
                    _reply_with_retrieval_synthesis(
                        fallback_text,
                        evidence_payload,
                        route_name="notion playbook qa",
                    )
                    logger.info(
                        "Responded with notion doc answer in thread_ts=%s refs=%s",
                        thread_ts,
                        len(notion_references),
                    )
                    return
                if not _is_notion_configured():
                    logger.warning("Notion doc query had no local match and notion is not configured in runtime")
                reply("관련 운영 문서를 찾지 못했어. 증상이나 키워드를 조금 더 구체적으로 말해줘")
                logger.info("No notion references matched in thread_ts=%s question=%s", thread_ts, question)
                return
            except TimeoutError:
                logger.warning("Notion doc answer timeout")
                reply(_timeout_reply_text())
                return
            except Exception:
                logger.exception("Notion doc answer failed")
                reply("문서 기반 답변 중 오류가 발생했어. 잠시 후 다시 시도해줘")
                return

        if s.LLM_PROVIDER == "claude" and claude_client:
            _set_request_log_route(
                payload,
                "llm_freeform",
                route_mode="claude",
                handler_type="llm_freeform",
            )
            if not question:
                reply("질문 내용을 같이 보내줘. 지원 기능이 궁금하면 `사용법`이라고 보내줘")
                return
            if not _is_claude_allowed_user(user_id):
                reply("Claude 질문은 현재 지정된 사용자만 사용할 수 있어")
                logger.info("Rejected claude call for user=%s", user_id)
                return
            try:
                thread_context = _load_slack_thread_context(
                    client,
                    logger,
                    channel_id,
                    thread_ts,
                    current_ts,
                )
                if is_prompt_exfiltration_attempt(question, thread_context):
                    logger.warning(
                        "Blocked freeform prompt exfiltration attempt in thread_ts=%s question=%s",
                        thread_ts,
                        question,
                    )
                    reply(build_prompt_security_refusal())
                    return
                fallback_evidence = _build_barcode_fallback_evidence()
                if fallback_evidence is not None:
                    synthesis_thread_context = ""
                    if s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT:
                        synthesis_thread_context = _load_slack_thread_context(
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
                        system_prompt=_get_freeform_system_prompt(question, synthesis_thread_context),
                        extra_rules=_build_company_retrieval_rules(fallback_evidence),
                        evidence_transform=_transform_company_retrieval_payload,
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
                model_input = _build_model_input(question, thread_context)
                answer = _ask_claude(
                    claude_client,
                    model_input,
                    system_prompt=_build_freeform_chat_system_prompt(
                        question,
                        thread_context,
                        speaker_user_id=user_id,
                    ),
                )
                answer = _sanitize_freeform_reply(answer)
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
            _set_request_log_route(
                payload,
                "llm_freeform",
                route_mode="ollama",
                handler_type="llm_freeform",
            )
            if not question:
                reply("질문 내용을 같이 보내줘. 지원 기능이 궁금하면 `사용법`이라고 보내줘")
                return
            try:
                thread_context = _load_slack_thread_context(
                    client,
                    logger,
                    channel_id,
                    thread_ts,
                    current_ts,
                )
                if is_prompt_exfiltration_attempt(question, thread_context):
                    logger.warning(
                        "Blocked freeform prompt exfiltration attempt in thread_ts=%s question=%s",
                        thread_ts,
                        question,
                    )
                    reply(build_prompt_security_refusal())
                    return
                health = _check_ollama_health()
                if not health["ok"]:
                    logger.warning("Ollama unavailable before answer generation: %s", health["summary"])
                    reply(_llm_unavailable_reply_text(str(health["summary"])))
                    return
                fallback_evidence = _build_barcode_fallback_evidence()
                if fallback_evidence is not None:
                    synthesis_thread_context = ""
                    if s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT:
                        synthesis_thread_context = _load_slack_thread_context(
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
                        system_prompt=_get_freeform_system_prompt(question, synthesis_thread_context),
                        extra_rules=_build_company_retrieval_rules(fallback_evidence),
                        evidence_transform=_transform_company_retrieval_payload,
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
                model_input = _build_model_input(question, thread_context)
                answer = _ask_ollama_chat(
                    model_input,
                    system_prompt=_build_freeform_chat_system_prompt(
                        question,
                        thread_context,
                        speaker_user_id=user_id,
                    ),
                    think=False,
                )
                answer = _sanitize_freeform_reply(answer)
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

        reply("지원 기능이 궁금하면 `사용법`이라고 보내줘", mention_user=False)

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

    app = create_slack_app(_handle_company_mention, _handle_company_message)
    attach_weekly_recordings_reporter(app, logger=logging.getLogger(__name__))
    return app
