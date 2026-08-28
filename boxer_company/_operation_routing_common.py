"""Operation matcher의 공통 scope·date·context 정규화 정본이다."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from boxer.context.entries import ContextEntry
from boxer.context.windowing import window_context_entries
from boxer.core import settings as core_settings
from boxer_company import settings as company_settings


class CompanyOperationRequestContract(Protocol):
    """순수 matcher가 Slack/API request에서 읽는 최소 구조다."""

    question: str
    metadata: Mapping[str, Any]
    context_entries: tuple[Mapping[str, Any], ...]
    actor_id: str | None
    channel: str


# 이동 전 private annotation 이름도 get_type_hints에서 해석되게 유지한다.
CompanyAssistantRequest = CompanyOperationRequestContract


# 이동 전 모듈들이 사용하던 settings alias를 같은 정본 객체에 묶는다.
cs = company_settings
s = core_settings


def _extract_barcode(text: str) -> str | None:
    match = cs.BARCODE_PATTERN.search(text)
    if not match:
        return None
    return match.group(1)


def _normalize_spaces(text: str) -> str:
    return " ".join((text or "").strip().split())


_NUMERIC_YMD_PATTERN = re.compile(r"(?<!\d)(\d{2,4})\s*[-./]\s*(\d{1,2})\s*[-./]\s*(\d{1,2})(?!\d)")


_KOREAN_YMD_PATTERN = re.compile(
    r"(?<!\d)(\d{2,4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일(?!\d)"
)


_NUMERIC_MD_PATTERN = re.compile(r"(?<!\d)(\d{1,2})\s*[./]\s*(\d{1,2})(?!\d)")


_NUMERIC_MD_DASH_PATTERN = re.compile(r"(?<!\d)(\d{1,2})\s*-\s*(\d{1,2})(?!\d)")


_KOREAN_MD_PATTERN = re.compile(r"(?<!\d)(\d{1,2})\s*월\s*(\d{1,2})\s*일(?!\d)")


_COMPACT_YYYYMMDD_PATTERN = re.compile(r"(?<!\d)(20\d{2}|19\d{2})(\d{2})(\d{2})(?!\d)")


_COMPACT_YYMMDD_PATTERN = re.compile(r"(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)")


_COMPACT_MMDD_PATTERN = re.compile(r"(?<!\d)(\d{2})(\d{2})(?!\d)")


_YEAR_ONLY_PATTERN = re.compile(r"(?<!\d)(20\d{2}|\d{2})\s*년(?:도)?(?!\d)")


_HOSPITAL_SCOPE_PATTERN = re.compile(
    r"(?:^|\s)병원(?:명)?\s*[:=]?\s*(.*?)(?=\s*(?:병실(?:명)?|진료실명|장비명|devicename|날짜|로그|분석|(?:초음파\s*)?영상|비디오|동영상|녹화|캡처|스냅샷|개수|갯수|수|몇\s*개|있나|있는지|있어|유무|존재|조회|목록|다운로드|다운)\s*[:=]?|$)",
    re.IGNORECASE,
)


_ROOM_SCOPE_PATTERN = re.compile(
    r"(?:^|[\s)])(?:병실(?:명)?|진료실명)\s*[:=]?\s*(.*?)(?=\s*(?:장비명|devicename|날짜|로그|분석|(?:초음파\s*)?영상|비디오|동영상|녹화|캡처|스냅샷|개수|갯수|수|몇\s*개|있나|있는지|있어|유무|존재|조회|목록|파일|file|download|다운로드|다운)\s*[:=]?|$)",
    re.IGNORECASE,
)


_ROOM_TOKEN_PATTERN = re.compile(
    r"("
    r"[^\s`'\",]*(?:초음파실|진료실|병실|분만실|수술실|원장실)[^\s`'\",]*"
    r"|(?<!\d)\d+\s*호(?![A-Za-z0-9가-힣])"
    r")"
)


_ROOM_PREFIX_PATTERN = re.compile(r"((?:(?:\d+\s*층|[A-Za-z0-9가-힣]+동|\d+\s*-\s*\d+)\s*)+)$")


_LEADING_HOSPITAL_KEYWORD_SCOPE_PATTERN = re.compile(
    r"^\s*(.+?)\s+병원\s+(?:(?:초음파\s*)?영상|비디오|동영상|녹화|캡처|스냅샷|개수|갯수|수|몇\s*개|있나|있는지|있어|유무|존재|조회|목록|다운로드|다운)(?:\s|$)",
    re.IGNORECASE,
)


_SCOPE_COUNT_QUERY_SUFFIX_PATTERN = re.compile(
    r"(?:^|\s)(?:총\s*)?(?:몇\s*개(?:나)?|개수|갯수|수)"
    r"(?:\s*(?:야|냐|니|인가(?:요)?|예요|에요|입니까|인지|"
    r"있어(?:요)?|있나(?:요)?|있는지|일까(?:요)?|죠|"
    r"되나(?:요)?|되는지|될까(?:요)?))?"
    r"\s*[?？.!~]*\s*$",
    re.IGNORECASE,
)


_SCOPE_COUNT_GRAMMAR_RESIDUES = frozenset(
    {"총", "은", "는", "이", "가", "들", "들이", "들은"}
)


_DEVICE_NAME_SCOPE_PATTERN = re.compile(
    r"(?:^|[\s)])(?:장비명|devicename)\s*[:=]?\s*([A-Za-z0-9._-]+)",
    re.IGNORECASE,
)


_DEVICE_NAME_TOKEN_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])([A-Za-z][A-Za-z0-9]*-[A-Za-z0-9-]*\d[A-Za-z0-9-]*)(?![A-Za-z0-9])",
    re.IGNORECASE,
)


_LEADING_DEVICE_NAME_SCOPE_PATTERN = re.compile(
    r"^\s*([A-Za-z0-9]+-[A-Za-z0-9-]+)\s+"
    r"(?:장비(?:상태|정보|상세|세부)?|devices?|device|정보|상태|온라인|ssh)\b",
    re.IGNORECASE,
)


_TODAY_HINTS = ("오늘", "금일", "today")


_DAY_BEFORE_YESTERDAY_HINTS = ("그제", "엊그제", "day before yesterday")


_TOMORROW_HINTS = ("내일", "tomorrow")


def _current_local_date() -> datetime.date:
    tz_name = os.getenv("TZ", "Asia/Seoul")
    try:
        return datetime.now(ZoneInfo(tz_name)).date()
    except Exception:
        try:
            return datetime.now(ZoneInfo("Asia/Seoul")).date()
        except Exception:
            return datetime.utcnow().date()


def _normalize_year(raw_year: int) -> int:
    if raw_year < 100:
        return 2000 + raw_year
    return raw_year


def _try_format_date(year: int, month: int, day: int) -> str | None:
    try:
        parsed = datetime(year=year, month=month, day=day)
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%d")


def _is_word_char_for_date_boundary(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z가-힣]", value or ""))


def _is_embedded_numeric_date_match(text: str, matched: re.Match[str], kind: str) -> bool:
    if kind in {"korean_ymd", "korean_md"}:
        return False

    before = text[matched.start() - 1] if matched.start() > 0 else ""
    after = text[matched.end()] if matched.end() < len(text) else ""
    before_is_word = _is_word_char_for_date_boundary(before)
    after_is_word = _is_word_char_for_date_boundary(after)
    room_prefix = text[max(0, matched.start() - 12) : matched.start()]
    room_suffix = text[matched.end() : matched.end() + 12]

    if re.search(r"(?:\d+\s*층|[A-Za-z0-9가-힣]+동)\s*$", room_prefix) and re.match(
        r"\s*(?:초음파실|진료실|병실|분만실|수술실|원장실)",
        room_suffix,
    ):
        return True
    if not before_is_word and not after_is_word:
        return False

    prefix = text[max(0, matched.start() - 4) : matched.start()]
    suffix = text[matched.end() : matched.end() + 4]

    # 병실명 안의 "2층1-1진료실" 같은 토큰은 날짜보다 위치 정보로 봐야 한다.
    if re.search(r"(?:날짜|일자)$", prefix) or suffix.startswith(("일", "로그", "영상", "파일")):
        return False
    return True


def _sub_non_embedded_date_matches(text: str, pattern: re.Pattern[str], kind: str) -> str:
    return pattern.sub(
        lambda matched: " "
        if not _is_embedded_numeric_date_match(text, matched, kind)
        else matched.group(0),
        text,
    )


def _parse_explicit_date_expression(text: str) -> tuple[bool, str | None]:
    candidates: list[tuple[int, str, re.Match[str]]] = []
    for kind, pattern in (
        ("korean_ymd", _KOREAN_YMD_PATTERN),
        ("numeric_ymd", _NUMERIC_YMD_PATTERN),
        ("korean_md", _KOREAN_MD_PATTERN),
        ("numeric_md", _NUMERIC_MD_PATTERN),
        ("numeric_md_dash", _NUMERIC_MD_DASH_PATTERN),
        ("compact_yyyymmdd", _COMPACT_YYYYMMDD_PATTERN),
        ("compact_yymmdd", _COMPACT_YYMMDD_PATTERN),
        ("compact_mmdd", _COMPACT_MMDD_PATTERN),
    ):
        for matched in pattern.finditer(text):
            if _is_embedded_numeric_date_match(text, matched, kind):
                continue
            candidates.append((matched.start(), kind, matched))

    if not candidates:
        return False, None

    invalid_date_seen = False
    for _, kind, matched in sorted(candidates, key=lambda item: item[0]):
        if kind in {"korean_ymd", "numeric_ymd"}:
            year = _normalize_year(int(matched.group(1)))
            month = int(matched.group(2))
            day = int(matched.group(3))
            parsed = _try_format_date(year, month, day)
        elif kind == "compact_yyyymmdd":
            year = int(matched.group(1))
            month = int(matched.group(2))
            day = int(matched.group(3))
            parsed = _try_format_date(year, month, day)
        elif kind == "compact_yymmdd":
            year = _normalize_year(int(matched.group(1)))
            month = int(matched.group(2))
            day = int(matched.group(3))
            parsed = _try_format_date(year, month, day)
        else:
            local_year = _current_local_date().year
            month = int(matched.group(1))
            day = int(matched.group(2))
            parsed = _try_format_date(local_year, month, day)

        if parsed is not None:
            return True, parsed
        invalid_date_seen = True

    return invalid_date_seen, None


def _looks_like_unparsed_date_token(text: str) -> bool:
    stripped = (text or "").strip()
    if not stripped:
        return False

    patterns = (
        r"(?<!\d)\d{1,2}\s*-\s*\d{1,2}(?!\d)",
        r"(?<!\d)\d{4}(?!\d)",
        r"(?<!\d)\d{6}(?!\d)",
        r"(?<!\d)\d{8}(?!\d)",
        r"\d{1,2}\s*월",
        r"\d{1,2}\s*[./]\s*\d{1,2}",
    )
    return any(re.search(pattern, stripped) for pattern in patterns)


def _extract_relative_day_offset(text: str, lowered: str) -> int | None:
    if any(token in text or token in lowered for token in _DAY_BEFORE_YESTERDAY_HINTS):
        return -2
    if any(token in text or token in lowered for token in cs.YESTERDAY_HINTS):
        return -1
    if any(token in text or token in lowered for token in _TOMORROW_HINTS):
        return 1
    if any(token in text or token in lowered for token in _TODAY_HINTS):
        return 0
    return None


def _extract_log_date_with_presence(question: str) -> tuple[str, bool]:
    text = (question or "").strip()
    lowered = text.lower()

    has_explicit_date, parsed_explicit_date = _parse_explicit_date_expression(text)
    if has_explicit_date:
        if parsed_explicit_date is None:
            raise ValueError("날짜 형식을 확인해줘. 예: 2026-03-03, 26.03.03, 3/3, 3월 3일, 02-20, 260220")
        return parsed_explicit_date, True

    base_date = _current_local_date()
    relative_offset = _extract_relative_day_offset(text, lowered)
    if relative_offset is not None:
        base_date = base_date + timedelta(days=relative_offset)
        return base_date.strftime("%Y-%m-%d"), True

    if _looks_like_unparsed_date_token(text):
        raise ValueError("날짜 형식을 확인해줘. 예: 2026-03-03, 26.03.03, 3/3, 3월 3일, 02-20, 260220")
    return base_date.strftime("%Y-%m-%d"), False


def _extract_device_name_scope(question: str) -> str | None:
    text = re.sub(r"<@[^>]+>", " ", str(question or "")).strip()
    sanitized_text = re.sub(r"[`'\"“”‘’]+", "", text)
    matched = _DEVICE_NAME_SCOPE_PATTERN.search(sanitized_text)
    if not matched:
        matched = _LEADING_DEVICE_NAME_SCOPE_PATTERN.search(sanitized_text)
    if not matched:
        # 현장 2차 입력은 "장비명" 라벨 없이 MB2-A00313만 붙는 경우가 많다.
        matched = _DEVICE_NAME_TOKEN_PATTERN.search(sanitized_text)
    if not matched:
        return None

    candidate = " ".join(str(matched.group(1) or "").split()).strip().strip("`'\"")
    if not candidate:
        return None
    if re.fullmatch(r"(?:조회|목록|개수|수|정보|상태)", candidate, flags=re.IGNORECASE):
        return None
    return candidate


def _extract_hospital_room_scope(question: str) -> tuple[str | None, str | None]:
    text = (question or "").strip()
    hospital_match = _HOSPITAL_SCOPE_PATTERN.search(text)
    room_match = _ROOM_SCOPE_PATTERN.search(text)
    has_count_query = bool(_SCOPE_COUNT_QUERY_SUFFIX_PATTERN.search(text))

    def _clean(value: str) -> str:
        normalized = re.sub(r"<@[^>]+>", " ", str(value or ""))
        normalized = re.sub(r"(?<!\S)@\S+", " ", normalized)
        normalized = " ".join(normalized.split()).strip().strip("`'\"[]")
        # 이름 뒤의 자연어 수량 질문만 제거해 `개나리병원` 같은 실제 이름은 보존한다.
        normalized = _SCOPE_COUNT_QUERY_SUFFIX_PATTERN.sub("", normalized).strip()
        normalized = re.sub(
            r"((?:병원|의원|클리닉|센터))(?:들)?(?:은|는|이|가)\s*$",
            r"\1",
            normalized,
        )
        normalized = re.sub(r"(?<!\d)\d{11}(?!\d)", "", normalized)
        normalized = re.sub(r"\s+\d{2,4}[./-]\d{1,2}[./-]\d{1,2}\s*$", "", normalized)
        normalized = re.sub(r"\s+\d{1,2}\s*월\s*\d{1,2}\s*일\s*$", "", normalized)
        normalized = re.sub(r"\s+\d{2,4}\s*년(?:도)?\s*$", "", normalized)
        normalized = re.sub(r"(?<!\d)\d{4}(?!\d)\s*$", "", normalized)
        normalized = re.sub(r"^\s*병원(?:명)?\s*[:=]?\s*", "", normalized)
        normalized = re.sub(r"^\s*(?:병실(?:명)?|진료실명)\s*[:=]?\s*", "", normalized)
        normalized = re.sub(r"^\s*날짜\s*[:=]?\s*", "", normalized)
        normalized = re.sub(
            r"\s+(?:장비명|devicename)\s*[:=]?\s*[A-Za-z0-9._-]+\s*$",
            "",
            normalized,
            flags=re.IGNORECASE,
        )
        normalized = re.sub(
            r"\s+[A-Za-z][A-Za-z0-9]*-[A-Za-z0-9-]*\d[A-Za-z0-9-]*\s*$",
            "",
            normalized,
        )
        # 병실 라벨 뒤에 액션 단어가 연달아 붙어도 병실값에 섞이지 않게 끝에서 반복 제거한다.
        while True:
            cleaned = re.sub(r"\s*(?:로그|분석)\s*$", "", normalized)
            cleaned = re.sub(
                r"\s*(?:(?:초음파\s*)?영상|비디오|동영상|녹화|캡처|스냅샷|개수|갯수|수|몇\s*개|있나|있는지|있어|유무|존재|조회|목록|파일|file|download|다운로드|다운)\s*$",
                "",
                cleaned,
                flags=re.IGNORECASE,
            ).strip()
            if cleaned == normalized:
                break
            normalized = cleaned
        if re.search(r"(?:병원|의원|클리닉|센터).+\s+병원$", normalized):
            normalized = re.sub(r"\s+병원$", "", normalized)
        return normalized.strip()

    def _is_scope_noise(value: str) -> bool:
        cleaned = " ".join(str(value or "").split()).strip()
        cleaned = _SCOPE_COUNT_QUERY_SUFFIX_PATTERN.sub("", cleaned).strip()
        if not cleaned:
            return True
        cleaned = re.sub(r"[?!.~]+\s*$", "", cleaned).strip()
        return bool(
            re.fullmatch(
                r"(?:(?:초음파\s*)?영상|비디오|동영상|녹화|캡처|스냅샷|개수|갯수|수|몇\s*개|있나|있는지|있어|유무|존재|조회|목록|다운로드|다운|\d{2,4}\s*년(?:도)?|(?:\d{2,4}\s*년(?:도)?\s+)?(?:병원|병실|진료실))(?:\s+(?:개수|갯수|수|조회|목록))?",
                cleaned,
                flags=re.IGNORECASE,
            )
        )

    hospital_name = _clean(hospital_match.group(1)) if hospital_match else ""
    room_name = _clean(room_match.group(1)) if room_match else ""
    # 조사·총계 표현은 수량 질문에서 캡처된 경우에만 버려 한 글자 실제 scope를 보존한다.
    if has_count_query and hospital_name in _SCOPE_COUNT_GRAMMAR_RESIDUES:
        hospital_name = ""
    if has_count_query and room_name in _SCOPE_COUNT_GRAMMAR_RESIDUES:
        room_name = ""
    if _is_scope_noise(hospital_name):
        hospital_name = ""
    if _is_scope_noise(room_name):
        room_name = ""

    if hospital_name and room_name:
        return (hospital_name or None, room_name or None)

    fallback_text = re.sub(r"<@[^>]+>", " ", text)
    fallback_text = re.sub(r"(?<!\S)@\S+", " ", fallback_text)
    for pattern in (
        ("korean_ymd", _KOREAN_YMD_PATTERN),
        ("numeric_ymd", _NUMERIC_YMD_PATTERN),
        ("korean_md", _KOREAN_MD_PATTERN),
        ("numeric_md", _NUMERIC_MD_PATTERN),
        ("numeric_md_dash", _NUMERIC_MD_DASH_PATTERN),
        ("compact_yyyymmdd", _COMPACT_YYYYMMDD_PATTERN),
        ("compact_yymmdd", _COMPACT_YYMMDD_PATTERN),
        ("compact_mmdd", _COMPACT_MMDD_PATTERN),
    ):
        kind, date_pattern = pattern
        fallback_text = _sub_non_embedded_date_matches(fallback_text, date_pattern, kind)
    fallback_text = _YEAR_ONLY_PATTERN.sub(" ", fallback_text)
    fallback_text = re.sub(r"(?<!\d)\d{11}(?!\d)", " ", fallback_text)
    fallback_text = re.sub(r"\b(?:로그|분석)\b", " ", fallback_text)
    fallback_text = re.sub(
        r"\b(?:영상|비디오|동영상|recording|recordings|캡처|capture|captures|스냅샷|snapshot|조회|목록|개수|갯수|count|있는지|있나|있어|유무|존재|전체|파일|file|download|다운로드|다운)\b",
        " ",
        fallback_text,
        flags=re.IGNORECASE,
    )
    fallback_text = " ".join(fallback_text.split()).strip()

    room_token_match = _ROOM_TOKEN_PATTERN.search(fallback_text)
    if room_token_match:
        room_prefix = ""
        hospital_source = fallback_text[: room_token_match.start()].rstrip()

        # 무라벨 입력은 "5층 4진료실"처럼 위치 prefix가 병실 앞에 붙는 경우가 많아서 같이 묶어준다.
        room_prefix_match = _ROOM_PREFIX_PATTERN.search(hospital_source)
        if room_prefix_match:
            room_prefix = _clean(room_prefix_match.group(1))
            hospital_source = hospital_source[: room_prefix_match.start()].rstrip()

        derived_room_name = _clean(
            " ".join(part for part in (room_prefix, room_token_match.group(1)) if part)
        )
        if not room_name:
            room_name = derived_room_name

        if not hospital_name:
            hospital_name = _clean(hospital_source)

    if not hospital_name and not room_name:
        leading_hospital_keyword_match = _LEADING_HOSPITAL_KEYWORD_SCOPE_PATTERN.search(text)
        if leading_hospital_keyword_match:
            hospital_name = _clean(leading_hospital_keyword_match.group(1))

    if not hospital_name and not room_name:
        cleaned_fallback = _clean(fallback_text)
        if any(token in cleaned_fallback for token in ("병원", "의원", "클리닉", "센터")):
            hospital_name = cleaned_fallback

    # fallback에서 날짜나 질의어만 다시 scope로 유도될 수 있어 최종 후보에도
    # 동일한 noise 판정을 적용한다.
    if _is_scope_noise(hospital_name):
        hospital_name = ""
    if _is_scope_noise(room_name):
        room_name = ""

    return (hospital_name or None, room_name or None)


class AssistantRequestScopeMismatch(ValueError):
    def __init__(self, dimension: str) -> None:
        super().__init__(f"{dimension} scope mismatch")
        self.dimension = dimension


@dataclass(frozen=True, slots=True)
class AssistantRequestScope:
    barcode: str | None
    hospital_name: str | None
    room_name: str | None
    device_name: str | None


def resolve_assistant_request_scope(
    request: CompanyAssistantRequest,
) -> AssistantRequestScope:
    """질문과 adapter scope가 함께 있으면 일치할 때만 조회 범위로 확정한다."""
    question_barcode = _extract_barcode(request.question)
    question_hospital, question_room = _extract_hospital_room_scope(
        request.question
    )
    question_device = _extract_device_name_scope(request.question)

    metadata_barcode = _metadata_text(request, "barcode")
    metadata_hospital = _metadata_text(
        request,
        "hospital_name",
        "hospitalName",
        "phase2_hospital_name",
        "phase2HospitalName",
    )
    metadata_room = _metadata_text(
        request,
        "room_name",
        "roomName",
        "phase2_room_name",
        "phase2RoomName",
    )
    metadata_device = _metadata_text(
        request,
        "device_name",
        "deviceName",
    )

    _raise_if_mismatch("barcode", metadata_barcode, question_barcode)
    _raise_if_mismatch(
        "hospital_room",
        metadata_hospital,
        question_hospital,
    )
    _raise_if_mismatch("hospital_room", metadata_room, question_room)
    _raise_if_mismatch("device", metadata_device, question_device)
    return AssistantRequestScope(
        barcode=metadata_barcode or question_barcode,
        hospital_name=metadata_hospital or question_hospital,
        room_name=metadata_room or question_room,
        device_name=metadata_device or question_device,
    )


def window_assistant_context_entries(
    request: CompanyAssistantRequest,
) -> tuple[ContextEntry, ...]:
    return tuple(
        window_context_entries(
            list(request.context_entries),
            max_chars=max(1, s.THREAD_CONTEXT_MAX_CHARS),
        )
    )


def _metadata_text(
    request: CompanyAssistantRequest,
    *keys: str,
) -> str | None:
    for key in keys:
        value: Any = request.metadata.get(key)
        if isinstance(value, str):
            normalized = " ".join(value.split()).strip()
            if normalized:
                return normalized
    return None


def _raise_if_mismatch(
    dimension: str,
    metadata_value: str | None,
    question_value: str | None,
) -> None:
    if (
        metadata_value
        and question_value
        and metadata_value != question_value
    ):
        # 실제 scope 값은 exception이나 응답에 넣지 않아 교차 조회 정보를 숨긴다.
        raise AssistantRequestScopeMismatch(dimension)
