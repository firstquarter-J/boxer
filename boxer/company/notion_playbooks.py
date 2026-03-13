import re
import time
from typing import Any

from boxer.core import settings as s
from boxer.routers.common.notion import (
    _extract_block_text,
    _fetch_all_notion_blocks,
    _is_notion_configured,
    _load_notion_page_content_cached,
    _normalize_notion_id,
)

_RAG_INDEX_HEADING = "RAG 인덱스"
_RAG_INDEX_LINE_PATTERN = re.compile(
    r"page_id=(?P<page_id>[0-9a-fA-F-]{32,36})\s*\|\s*"
    r"section=(?P<section>[^|]+?)\s*\|\s*"
    r"kind=(?P<kind>[^|]+?)\s*\|\s*"
    r"priority=(?P<priority>[^|]+?)\s*\|\s*"
    r"title=(?P<title>[^|]+?)\s*\|\s*"
    r"keywords=(?P<keywords>.+)$"
)
_NOTION_CACHE_TTL_SEC = 300
_NOTION_INDEX_CACHE: dict[str, Any] = {
    "root_page_id": "",
    "expires_at": 0.0,
    "entries": [],
}
_NOTION_OVERVIEW_QUERY_TOKENS = ("설명", "소개", "뭐야", "무엇", "개요", "알려줘")
_NOTION_OVERVIEW_SECTION_TITLES = (
    "마미박스 장애 대응",
    "베이비매직",
    "마미박스 가이드",
    "마미박스 설치",
    "마미박스 설정",
    "마미박스 장비 구성",
)
_LOW_SIGNAL_NOTION_TERMS = {
    "가이드",
    "기록",
    "로그",
    "녹화",
    "마미박스",
    "문제",
    "반복",
    "분석",
    "실패",
    "업로드",
    "영상",
    "이슈",
    "장비",
    "조치",
    "초음파",
    "확인",
}
_NOTION_QUERY_EXPANSIONS = (
    {
        "tokens": (
            "핑크 바코드",
            "무료 바코드",
            "유료 바코드",
            "바코드 동기화",
            "cfg1_barcode_sync_date",
        ),
        "aliases": (
            "핑크 바코드",
            "무료 바코드",
            "유료 바코드",
            "바코드 동기화",
            "분만 병원",
            "비분만 병원",
            "온라인 상태",
        ),
    },
    {
        "tokens": ("restart_detected", "재시작", "restart", "reboot"),
        "aliases": ("재시작", "재부팅", "restart", "reboot", "멈춤", "비정상 재부팅"),
    },
    {
        "tokens": ("ffmpeg_sigterm", "sigterm"),
        "aliases": ("ffmpeg", "sigterm", "녹화 실패", "업로드 실패"),
    },
    {
        "tokens": ("stalled", "stall"),
        "aliases": ("stalled", "stall", "recording may be stalled", "녹화 지연"),
    },
    {
        "tokens": ("timestamp", "dts", "pts", "invalid dropping"),
        "aliases": ("timestamp", "dts", "pts", "invalid dropping", "캡처보드", "영상 입력"),
    },
    {
        "tokens": ("eai_again", "jwt", "uploader", "endpoint", "네트워크"),
        "aliases": ("네트워크", "통신", "업로드", "dns", "jwt", "eai_again"),
    },
    {
        "tokens": (
            "방화벽",
            "firewall",
            "mda",
            "status none",
            "ssh",
            "원격 접속",
            "원격 연결",
            "모니터링",
            "에이전트",
        ),
        "aliases": (
            "병원 방화벽",
            "방화벽",
            "MDA",
            "원격 접속",
            "원격 연결",
            "SSH",
            "status NONE",
            "모니터링",
            "영상 업로드 정상",
            "네트워크",
        ),
    },
    {
        "tokens": ("노이즈", "잡음", "아티팩트", "지지직", "울림", "웅"),
        "aliases": (
            "노이즈",
            "잡음",
            "소리 잡음",
            "화면 잡음",
            "전기적 아티팩트",
            "그라운드 루프",
            "ei 코어",
            "페라이트",
        ),
    },
)
_NOTION_PLAYBOOK_TOPIC_RULES = (
    {
        "tokens": ("설명", "소개", "개요", "뭐야", "무엇", "대해"),
        "titles": (
            "마미박스 프로세스 순서",
            "마미박스 버전별 운용 장비 목록",
            "마미박스 장비 캡처보드",
        ),
    },
    {
        "tokens": ("ffmpeg", "sigterm", "stall", "stalled", "thumbnail", "recording", "녹화", "업로드"),
        "titles": (
            "초음파 영상 업로드 이슈 분석 가이드",
            "초음파 영상 업로드 반복 실패",
            "초음파 영상 녹화불가(화면 신호 없음)",
            "초음파 영상 확인",
            "로그 패턴 분석 가이드",
        ),
    },
    {
        "tokens": (
            "방화벽",
            "firewall",
            "mda",
            "status none",
            "ssh",
            "원격 접속",
            "원격 연결",
            "모니터링",
            "에이전트",
        ),
        "titles": (
            "병원 방화벽으로 MDA/원격 접속이 안 될 때",
            "초음파 영상 업로드 안됨(네트워크 이슈)",
            "네트워크 환경 가이드라인",
        ),
    },
    {
        "tokens": ("network", "dns", "jwt", "eai_again", "업로드", "네트워크", "통신"),
        "titles": (
            "초음파 영상 업로드 안됨(네트워크 이슈)",
            "초음파 영상 업로드 반복 실패",
            "초음파 영상 업로드 이슈 분석 가이드",
        ),
    },
    {
        "tokens": ("/dev/video", "video device", "timestamp", "dts", "pts", "화면 신호 없음", "캡처보드", "영상 입력"),
        "titles": (
            "초음파 영상 녹화불가(화면 신호 없음)",
            "초음파 영상 확인",
            "로그 패턴 분석 가이드",
            "마미박스 장비 캡처보드",
        ),
    },
    {
        "tokens": ("noise", "audio", "소리", "오디오", "노이즈", "잡음"),
        "titles": (
            "초음파 영상 소리 잡음(노이즈)",
            "마미박스 소리 없음",
            "마미박스 장비 스피커",
            "마미박스 장비 사운드케이블(2RCA or 3.5mm to 3.5mm)",
        ),
    },
    {
        "tokens": ("artifact", "아티팩트", "전기적", "화면 잡음"),
        "titles": (
            "초음파 화면 잡음(전기적 아티팩트)",
            "마미박스 장비 그라운드 루프 아이솔레이터",
            "마미박스 장비 RGB 케이블 및 RGB to HDMI 컨버터",
        ),
    },
    {
        "tokens": (
            "reboot",
            "restart",
            "restart_detected",
            "memory",
            "메모리",
            "재부팅",
            "재시작",
            "멈춤",
            "비정상 종료",
        ),
        "titles": (
            "299버전 메모리 문제 확인 및 조치",
            "마미박스 멈춤 & 비정상 재부팅",
            "마미박스 부팅 불가(파일 시스템 손상)",
        ),
    },
    {
        "tokens": ("capture", "captured", "이미지 캡처", "캡처 불가"),
        "titles": (
            "마미박스 초음파 이미지 캡처 불가",
            "초음파 영상 확인",
        ),
    },
    {
        "tokens": (
            "핑크 바코드",
            "무료 바코드",
            "유료 바코드",
            "바코드 동기화",
            "cfg1_barcode_sync_date",
            "분만 병원",
            "비분만 병원",
            "동기화",
        ),
        "titles": (
            "바코드 동기화: 분만 병원에서 핑크 바코드가 스캔되는 경우",
        ),
    },
    {
        "tokens": ("scanner", "barcode scanner", "바코드 스캐너"),
        "titles": (
            "바코드 스캐너 작동 문제",
        ),
    },
)
_PLAYBOOK_PRIORITY_WEIGHT = {
    "high": 3,
    "medium": 2,
    "low": 1,
}


def _normalize_notion_lookup_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip().lower())


def _extract_notion_lookup_terms(text: str) -> list[str]:
    parts = re.split(r"[^0-9A-Za-z가-힣._+-]+", text or "")
    return [part for part in parts if len(part.strip()) >= 2]


def _build_notion_preview_lines(lines: list[str] | None, query_text: str, *, max_lines: int = 8) -> list[str]:
    query_terms = {
        _normalize_notion_lookup_text(term)
        for term in _extract_notion_lookup_terms(query_text)
        if _normalize_notion_lookup_text(term)
    }
    scored: list[tuple[int, int, str]] = []
    seen: set[str] = set()

    for index, raw_line in enumerate(lines or []):
        stripped = str(raw_line or "").strip()
        if not stripped:
            continue
        normalized = _normalize_notion_lookup_text(stripped)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)

        score = 0
        if ":" in stripped:
            score += 4
        if stripped.startswith("- "):
            score += 2
        if stripped.startswith("#"):
            score -= 2
        if len(stripped) <= 18 and ":" not in stripped:
            score -= 2
        if any(term and term in normalized for term in query_terms):
            score += 7
        if any(
            token in stripped
            for token in (
                "정책:",
                "전제:",
                "확인 포인트:",
                "운영 기준",
                "재부팅",
                "재시작",
                "동기화",
                "원인",
                "조치",
                "실제 사례",
            )
        ):
            score += 4

        scored.append((score, index, stripped[:160]))

    scored.sort(key=lambda item: (item[0], -item[1]), reverse=True)
    return [line for _, _, line in scored[: max(1, max_lines)]]


def _parse_notion_rag_index_line(text: str) -> dict[str, Any] | None:
    matched = _RAG_INDEX_LINE_PATTERN.match((text or "").strip())
    if not matched:
        return None

    raw_keywords = [keyword.strip() for keyword in matched.group("keywords").split(",") if keyword.strip()]
    return {
        "pageId": _normalize_notion_id(matched.group("page_id")),
        "section": matched.group("section").strip(),
        "kind": matched.group("kind").strip(),
        "priority": matched.group("priority").strip().lower(),
        "title": matched.group("title").strip(),
        "keywords": raw_keywords,
    }


def _build_fallback_notion_rag_index(root_page_id: str) -> list[dict[str, Any]]:
    blocks = _fetch_all_notion_blocks(root_page_id)
    current_section = ""
    entries: list[dict[str, Any]] = []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        block_type = block.get("type")
        if block_type == "heading_1":
            current_section = _extract_block_text(block)
            continue
        if block_type != "child_page":
            continue
        title = _extract_block_text(block)
        if not title:
            continue
        section = current_section or "기타"
        entries.append(
            {
                "pageId": _normalize_notion_id(str(block.get("id") or "")),
                "section": section,
                "kind": "runbook" if "장애" in section or "문제" in title or "불가" in title else "guide",
                "priority": "high" if "장애" in section else "medium",
                "title": title,
                "keywords": _extract_notion_lookup_terms(title)[:8],
            }
        )
    return entries


def _load_notion_rag_index(root_page_id: str) -> list[dict[str, Any]]:
    normalized_root_id = _normalize_notion_id(root_page_id)
    now = time.time()
    if (
        _NOTION_INDEX_CACHE.get("root_page_id") == normalized_root_id
        and float(_NOTION_INDEX_CACHE.get("expires_at") or 0) > now
    ):
        cached_entries = _NOTION_INDEX_CACHE.get("entries")
        if isinstance(cached_entries, list):
            return cached_entries

    blocks = _fetch_all_notion_blocks(normalized_root_id)
    entries: list[dict[str, Any]] = []
    in_index = False
    for block in blocks:
        if not isinstance(block, dict):
            continue
        block_type = block.get("type")
        text = _extract_block_text(block)
        if block_type == "heading_2" and text == _RAG_INDEX_HEADING:
            in_index = True
            continue
        if not in_index:
            continue
        if block_type == "heading_1":
            break
        if block_type != "bulleted_list_item":
            continue
        entry = _parse_notion_rag_index_line(text)
        if entry is not None:
            entries.append(entry)

    fallback_entries = _build_fallback_notion_rag_index(normalized_root_id)
    seen_page_ids = {
        _normalize_notion_id(str(entry.get("pageId") or ""))
        for entry in entries
        if isinstance(entry, dict) and str(entry.get("pageId") or "").strip()
    }
    for entry in fallback_entries:
        if not isinstance(entry, dict):
            continue
        page_id = _normalize_notion_id(str(entry.get("pageId") or ""))
        if page_id in seen_page_ids:
            continue
        seen_page_ids.add(page_id)
        entries.append(entry)

    _NOTION_INDEX_CACHE.update(
        {
            "root_page_id": normalized_root_id,
            "expires_at": now + _NOTION_CACHE_TTL_SEC,
            "entries": entries,
        }
    )
    return entries


def _build_notion_lookup_query(question: str, evidence_payload: dict[str, Any] | None = None) -> str:
    parts = [(question or "").strip()]
    if not isinstance(evidence_payload, dict):
        return _normalize_notion_lookup_text(" ".join(part for part in parts if part))

    route = str(evidence_payload.get("route") or "").strip()
    if route:
        parts.append(route)

    if isinstance(evidence_payload.get("analysisResult"), str):
        parts.append(str(evidence_payload.get("analysisResult") or "")[:2000])

    request = evidence_payload.get("request") if isinstance(evidence_payload.get("request"), dict) else {}
    for key in ("mode", "question", "date"):
        value = request.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())

    values = evidence_payload.get("classificationTags")
    if isinstance(values, list):
        parts.extend(str(value).strip() for value in values if str(value).strip())

    for record in (evidence_payload.get("records") or [])[:2]:
        if not isinstance(record, dict):
            continue
        if isinstance(record.get("classificationTags"), list):
            parts.extend(
                str(value).strip()
                for value in record.get("classificationTags") or []
                if str(value).strip()
            )
        for field in ("recordingResult", "topErrorMessage", "firstFfmpegError", "causeHint"):
            value = record.get(field)
            if isinstance(value, str) and value.strip():
                parts.append(value.strip())
        for group in (record.get("topErrorGroups") or [])[:3]:
            if not isinstance(group, dict):
                continue
            for field in ("component", "signature", "sampleMessage"):
                value = group.get(field)
                if isinstance(value, str) and value.strip():
                    parts.append(value.strip())

    session = evidence_payload.get("session") if isinstance(evidence_payload.get("session"), dict) else {}
    if isinstance(session, dict):
        if isinstance(session.get("classificationTags"), list):
            parts.extend(
                str(value).strip()
                for value in session.get("classificationTags") or []
                if str(value).strip()
            )
        for field in ("routerCauseHint", "firstFfmpegError", "recordingResult"):
            value = session.get(field)
            if isinstance(value, str) and value.strip():
                parts.append(value.strip())
        representative = session.get("representativeErrorGroup")
        if isinstance(representative, dict):
            for field in ("component", "signature", "sampleMessage"):
                value = representative.get(field)
                if isinstance(value, str) and value.strip():
                    parts.append(value.strip())

    raw_query = " ".join(part for part in parts if part)
    normalized_query = _normalize_notion_lookup_text(raw_query)
    expansion_terms: list[str] = []
    for rule in _NOTION_QUERY_EXPANSIONS:
        tokens = tuple(_normalize_notion_lookup_text(token) for token in (rule.get("tokens") or ()))
        if not any(token and token in normalized_query for token in tokens):
            continue
        expansion_terms.extend(str(alias).strip() for alias in (rule.get("aliases") or ()) if str(alias).strip())

    if expansion_terms:
        normalized_query = _normalize_notion_lookup_text(f"{raw_query} {' '.join(expansion_terms)}")
    return normalized_query


def _score_notion_playbook_entry(entry: dict[str, Any], query_text: str, route: str) -> tuple[int, list[str]]:
    score = 0
    matched_terms: list[str] = []
    seen_tokens: set[str] = set()
    normalized_title = _normalize_notion_lookup_text(str(entry.get("title") or ""))
    normalized_section = _normalize_notion_lookup_text(str(entry.get("section") or ""))
    normalized_priority = _normalize_notion_lookup_text(str(entry.get("priority") or "medium"))

    if not query_text:
        return 0, matched_terms

    if route in {"barcode_log_analysis", "barcode_log_error_summary_session", "recording_failure_analysis"}:
        if normalized_section == "마미박스 장애 대응":
            score += 6
        if _normalize_notion_lookup_text(str(entry.get("kind") or "")) == "runbook":
            score += 4

    title_terms = _extract_notion_lookup_terms(str(entry.get("title") or ""))
    keywords = [str(keyword).strip() for keyword in (entry.get("keywords") or []) if str(keyword).strip()]
    if normalized_title and normalized_title in query_text:
        score += 12
        matched_terms.append(str(entry.get("title") or ""))

    for token in [*keywords, *title_terms]:
        normalized_token = _normalize_notion_lookup_text(token)
        if not normalized_token or normalized_token in seen_tokens or normalized_token not in query_text:
            continue
        seen_tokens.add(normalized_token)
        if token not in matched_terms:
            matched_terms.append(token)
        base_weight = 5 if token in keywords else 3
        if normalized_token in _LOW_SIGNAL_NOTION_TERMS:
            base_weight = 1
        score += base_weight

    for rule in _NOTION_PLAYBOOK_TOPIC_RULES:
        tokens = tuple(_normalize_notion_lookup_text(token) for token in rule.get("tokens") or [])
        if not any(token and token in query_text for token in tokens):
            continue
        title_matches = {_normalize_notion_lookup_text(title) for title in (rule.get("titles") or [])}
        if normalized_title in title_matches:
            score += 20

    score += _PLAYBOOK_PRIORITY_WEIGHT.get(normalized_priority, 0)
    return score, matched_terms[:6]


def _is_notion_overview_query(question: str) -> bool:
    text = (question or "").strip()
    if not text:
        return False
    if not any(token in text for token in ("마미박스", "베이비매직")):
        return False
    return any(token in text for token in _NOTION_OVERVIEW_QUERY_TOKENS)


def _build_notion_overview_reference(root_page_id: str) -> dict[str, Any]:
    payload = _load_notion_page_content_cached(root_page_id)
    lines = [str(line or "").strip() for line in (payload.get("lines") or [])]
    preview_lines = [str(payload.get("title") or "").strip() or "마미박스 운영 문서"]
    seen_sections: set[str] = set()

    for index, raw_line in enumerate(lines):
        line = raw_line.strip()
        if line not in _NOTION_OVERVIEW_SECTION_TITLES or line in seen_sections:
            continue
        seen_sections.add(line)
        summary = ""
        for next_line in lines[index + 1 : index + 6]:
            candidate = next_line.strip()
            if not candidate or candidate in _NOTION_OVERVIEW_SECTION_TITLES:
                if candidate in _NOTION_OVERVIEW_SECTION_TITLES:
                    break
                continue
            if candidate in {"문서 사용 순서", "RAG 인덱스"}:
                continue
            if candidate.startswith("- page_id=") or candidate.startswith("- ") or candidate.startswith("1. "):
                continue
            summary = candidate
            break
        preview_lines.append(f"{line}: {summary}" if summary else line)
        if len(preview_lines) >= 6:
            break

    return {
        "pageId": _normalize_notion_id(root_page_id),
        "title": str(payload.get("title") or "").strip() or "마미박스 운영 문서",
        "section": "루트",
        "kind": "overview",
        "priority": "high",
        "keywords": ["마미박스", "운영", "문서", "개요"],
        "matchedKeywords": ["마미박스", "개요"],
        "score": 100,
        "url": payload.get("url") or "",
        "previewLines": preview_lines,
        "plainText": "\n".join(preview_lines[:8]).strip(),
    }


def _select_notion_playbooks(
    question: str,
    *,
    evidence_payload: dict[str, Any] | None = None,
    root_page_id: str | None = None,
    max_results: int = 3,
) -> list[dict[str, Any]]:
    if not _is_notion_configured():
        return []

    target_root_page_id = root_page_id or s.NOTION_TEST_PAGE_ID
    if not target_root_page_id:
        return []

    route = ""
    if isinstance(evidence_payload, dict):
        route = str(evidence_payload.get("route") or "").strip().lower()

    query_text = _build_notion_lookup_query(question, evidence_payload)
    if not query_text:
        return []

    scored_entries: list[tuple[int, dict[str, Any], list[str]]] = []
    for entry in _load_notion_rag_index(target_root_page_id):
        if not isinstance(entry, dict):
            continue
        score, matched_terms = _score_notion_playbook_entry(entry, query_text, route)
        if score <= 0:
            continue
        scored_entries.append((score, entry, matched_terms))

    scored_entries.sort(
        key=lambda item: (
            item[0],
            _PLAYBOOK_PRIORITY_WEIGHT.get(str((item[1] or {}).get("priority") or "medium").lower(), 0),
            str((item[1] or {}).get("title") or ""),
        ),
        reverse=True,
    )

    selected: list[dict[str, Any]] = []
    seen_page_ids: set[str] = set()
    for score, entry, matched_terms in scored_entries:
        page_id = _normalize_notion_id(str(entry.get("pageId") or ""))
        if page_id in seen_page_ids:
            continue
        seen_page_ids.add(page_id)
        page_content = _load_notion_page_content_cached(page_id)
        preview_lines = _build_notion_preview_lines(
            page_content.get("lines") or [],
            query_text,
            max_lines=8,
        )
        selected.append(
            {
                "pageId": page_id,
                "title": entry.get("title"),
                "section": entry.get("section"),
                "kind": entry.get("kind"),
                "priority": entry.get("priority"),
                "keywords": entry.get("keywords") or [],
                "matchedKeywords": matched_terms,
                "score": score,
                "url": page_content.get("url") or "",
                "previewLines": preview_lines,
                "plainText": str(page_content.get("plainText") or "")[:2000],
            }
        )
        if len(selected) >= max(1, max_results):
            break

    return selected


def _select_notion_references(
    question: str,
    *,
    evidence_payload: dict[str, Any] | None = None,
    root_page_id: str | None = None,
    max_results: int = 3,
) -> list[dict[str, Any]]:
    if not _is_notion_configured():
        return []

    target_root_page_id = root_page_id or s.NOTION_TEST_PAGE_ID
    if not target_root_page_id:
        return []

    selected: list[dict[str, Any]] = []
    seen_page_ids: set[str] = set()

    if _is_notion_overview_query(question):
        overview_reference = _build_notion_overview_reference(target_root_page_id)
        overview_page_id = _normalize_notion_id(str(overview_reference.get("pageId") or ""))
        seen_page_ids.add(overview_page_id)
        selected.append(overview_reference)

    playbooks = _select_notion_playbooks(
        question,
        evidence_payload=evidence_payload,
        root_page_id=target_root_page_id,
        max_results=max(1, max_results),
    )
    for item in playbooks:
        if not isinstance(item, dict):
            continue
        page_id = _normalize_notion_id(str(item.get("pageId") or ""))
        if page_id in seen_page_ids:
            continue
        seen_page_ids.add(page_id)
        selected.append(item)
        if len(selected) >= max(1, max_results):
            break

    return selected[: max(1, max_results)]
