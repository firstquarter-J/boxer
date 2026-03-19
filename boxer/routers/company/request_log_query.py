from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from boxer.core import settings as s
from boxer.routers.common.request_log import (
    _list_request_log_recent,
    _summarize_request_log_by_route,
    _summarize_request_log_by_user,
    _summarize_request_log_overview,
)

_REQUEST_LOG_PREFIXES = (
    "요청 로그",
    "요청로그",
    "request log",
    "requestlog",
)
_REQUEST_LOG_OVERVIEW_PREFIXES = (
    "요청 통계",
    "요청통계",
)
_REQUEST_LOG_DATE_PATTERN = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
_REQUEST_LOG_LIMIT_PATTERN = re.compile(r"(?<![-\d])([1-9]\d?)(?![-\d])")


@dataclass(frozen=True)
class RequestLogQuerySpec:
    mode: str
    target_date: str | None
    scope_label: str
    limit: int
    user_query: str | None = None


def _request_log_timezone() -> ZoneInfo:
    return ZoneInfo(s.REQUEST_LOG_TIMEZONE)


def _request_log_today() -> str:
    return datetime.now(_request_log_timezone()).date().isoformat()


def _request_log_yesterday() -> str:
    return (datetime.now(_request_log_timezone()).date() - timedelta(days=1)).isoformat()


def _extract_request_log_query(question: str) -> RequestLogQuerySpec | None:
    normalized = str(question or "").strip()
    if not normalized:
        return None

    remainder = ""
    matched_prefix = ""
    for prefix in (*_REQUEST_LOG_PREFIXES, *_REQUEST_LOG_OVERVIEW_PREFIXES):
        if normalized.lower().startswith(prefix.lower()):
            matched_prefix = prefix
            remainder = normalized[len(prefix):].strip()
            break
    else:
        return None

    lowered_remainder = remainder.lower()
    if remainder.startswith("최근") or lowered_remainder.startswith("recent"):
        target_date, scope_label = _extract_request_log_scope(
            remainder,
            default_scope="today",
        )
        return RequestLogQuerySpec(
            mode="recent",
            target_date=target_date,
            scope_label=scope_label,
            limit=_extract_request_log_limit(remainder, default=100, max_limit=100),
        )

    if (
        remainder.startswith("사용자")
        or remainder.startswith("유저")
        or lowered_remainder.startswith("user")
    ):
        target_date, scope_label = _extract_request_log_scope(
            remainder,
            default_scope="today",
        )
        return RequestLogQuerySpec(
            mode="users",
            target_date=target_date,
            scope_label=scope_label,
            limit=_extract_request_log_limit(remainder, default=10, max_limit=20),
        )

    if (
        remainder.startswith("라우트")
        or remainder.startswith("경로")
        or lowered_remainder.startswith("route")
    ):
        target_date, scope_label = _extract_request_log_scope(
            remainder,
            default_scope="today",
        )
        return RequestLogQuerySpec(
            mode="routes",
            target_date=target_date,
            scope_label=scope_label,
            limit=_extract_request_log_limit(remainder, default=10, max_limit=20),
        )

    user_query = _extract_request_log_user_query(remainder)
    if matched_prefix in _REQUEST_LOG_OVERVIEW_PREFIXES:
        target_date, scope_label = _extract_request_log_scope(
            remainder,
            default_scope="today",
        )
        return RequestLogQuerySpec(
            mode="overview",
            target_date=target_date,
            scope_label=scope_label,
            limit=_extract_request_log_limit(remainder, default=5, max_limit=10),
        )

    target_date, scope_label = _extract_request_log_scope(
        remainder,
        default_scope="today",
    )
    return RequestLogQuerySpec(
        mode="recent",
        target_date=target_date,
        scope_label=scope_label,
        limit=_extract_request_log_limit(remainder, default=100, max_limit=100),
        user_query=user_query,
    )


def _extract_request_log_user_query(text: str) -> str | None:
    normalized = str(text or "").strip()
    if not normalized:
        return None

    working = _REQUEST_LOG_DATE_PATTERN.sub(" ", normalized)
    working = re.sub(
        r"\b(today|yesterday|all|recent|user|users|route|routes|summary|overview)\b",
        " ",
        working,
        flags=re.IGNORECASE,
    )
    working = re.sub(r"(?<![-\d])[1-9]\d?(?![-\d])", " ", working)
    for token in (
        "오늘",
        "어제",
        "전체",
        "누적",
        "최근",
        "사용자",
        "유저",
        "라우트",
        "경로",
        "요약",
        "통계",
    ):
        working = working.replace(token, " ")
    compact = " ".join(working.split()).strip(" ,")
    return compact or None


def _extract_request_log_scope(
    text: str,
    *,
    default_scope: str,
) -> tuple[str | None, str]:
    normalized = str(text or "").strip()
    lowered = normalized.lower()

    date_match = _REQUEST_LOG_DATE_PATTERN.search(normalized)
    if date_match:
        target_date = date_match.group(1)
        return target_date, f"`{target_date}`"

    if "어제" in normalized or "yesterday" in lowered:
        target_date = _request_log_yesterday()
        return target_date, f"어제 (`{target_date}`)"

    if "오늘" in normalized or "today" in lowered:
        target_date = _request_log_today()
        return target_date, f"오늘 (`{target_date}`)"

    if "전체" in normalized or "누적" in normalized or "all" in lowered:
        return None, "전체 누적"

    if default_scope == "today":
        target_date = _request_log_today()
        return target_date, f"오늘 (`{target_date}`)"
    return None, "전체 누적"


def _extract_request_log_limit(
    text: str,
    *,
    default: int,
    max_limit: int,
) -> int:
    match = _REQUEST_LOG_LIMIT_PATTERN.search(text or "")
    if not match:
        return default
    return min(max(1, int(match.group(1))), max_limit)


def _query_request_log_text(
    spec: RequestLogQuerySpec,
    *,
    db_path: str | None = None,
) -> str:
    if spec.mode == "recent":
        result = _list_request_log_recent(
            target_date=spec.target_date,
            user_query=spec.user_query,
            limit=spec.limit,
            db_path=db_path,
        )
        return _format_request_log_recent(result, spec)
    if spec.mode == "users":
        result = _summarize_request_log_by_user(
            target_date=spec.target_date,
            limit=spec.limit,
            db_path=db_path,
        )
        return _format_request_log_users(result, spec)
    if spec.mode == "routes":
        result = _summarize_request_log_by_route(
            target_date=spec.target_date,
            limit=spec.limit,
            db_path=db_path,
        )
        return _format_request_log_routes(result, spec)
    result = _summarize_request_log_overview(
        target_date=spec.target_date,
        top_limit=spec.limit,
        db_path=db_path,
    )
    return _format_request_log_overview(result, spec)


def _format_request_log_overview(result: dict[str, Any], spec: RequestLogQuerySpec) -> str:
    total_count = int(result.get("totalCount") or 0)
    lines = [
        "*요청 로그 조회 결과*",
        f"• 기준: {spec.scope_label}",
        f"• 전체 요청: `{total_count}건`",
        f"• 고유 사용자: `{int(result.get('uniqueUserCount') or 0)}명`",
        f"• 오류 요청: `{int(result.get('errorCount') or 0)}건`",
    ]
    if total_count <= 0:
        lines.append("• 결과: 저장된 요청 로그가 없어")
        lines.append("• 예시: `요청 로그`, `요청 로그 Hyun`, `요청 로그 2026-03-13`, `요청 통계`")
        return "\n".join(lines)

    top_users = [
        row for row in (result.get("topUsers") or [])
        if isinstance(row, dict)
    ]
    top_routes = [
        row for row in (result.get("topRoutes") or [])
        if isinstance(row, dict)
    ]

    if top_users:
        lines.append("")
        lines.append("*상위 사용자*")
        for index, row in enumerate(top_users, start=1):
            lines.append(
                f"{index}. `{_user_label(row)}` - `{int(row.get('requestCount') or 0)}건`"
            )

    if top_routes:
        lines.append("")
        lines.append("*상위 라우트*")
        for index, row in enumerate(top_routes, start=1):
            lines.append(
                f"{index}. `{_route_label(row)}` - `{int(row.get('requestCount') or 0)}건`"
            )

    lines.append("")
    lines.append("• 예시: `요청 로그`, `요청 로그 Hyun`, `요청 로그 2026-03-13`, `요청 로그 사용자`, `요청 통계`")
    return "\n".join(lines)


def _format_request_log_recent(result: dict[str, Any], spec: RequestLogQuerySpec) -> str:
    rows = [row for row in (result.get("rows") or []) if isinstance(row, dict)]
    lines = [
        "*요청 로그 최근 조회 결과*",
        f"• 기준: {spec.scope_label}",
    ]
    if spec.user_query:
        lines.append(f"• 사용자: `{spec.user_query}`")
    lines.extend(
        [
            f"• 표시 건수: 최근 `{spec.limit}건`",
            f"• 전체 요청: `{int(result.get('totalCount') or 0)}건`",
        ]
    )
    if not rows:
        lines.append("• 결과: 조건에 맞는 요청 로그가 없어")
        return "\n".join(lines)

    for index, row in enumerate(rows, start=1):
        permalink = str(row.get("permalink") or row.get("threadPermalink") or "").strip()
        line = (
            f"{index}. `{_time_label(row.get('createdAtLocal'))}`"
            f" | `{_user_label(row)}`"
            f" | `{_route_label(row)}`"
            f" | `{_handler_type_label(row.get('handlerType'))}`"
            f" | `{_status_label(row.get('status'))}`"
        )
        if permalink:
            line += f" | <{permalink}|링크>"
        lines.append(line)
        lines.append(f"   {_compact_request_text(row)}")
    return "\n".join(lines)


def _format_request_log_users(result: dict[str, Any], spec: RequestLogQuerySpec) -> str:
    rows = [row for row in (result.get("rows") or []) if isinstance(row, dict)]
    lines = [
        "*요청 로그 사용자 통계*",
        f"• 기준: {spec.scope_label}",
        f"• 전체 요청: `{int(result.get('totalCount') or 0)}건`",
        f"• 고유 사용자: `{int(result.get('uniqueUserCount') or 0)}명`",
        f"• 표시 사용자: 상위 `{spec.limit}명`",
    ]
    if not rows:
        lines.append("• 결과: 조건에 맞는 요청 로그가 없어")
        return "\n".join(lines)

    for index, row in enumerate(rows, start=1):
        error_count = int(row.get("errorCount") or 0)
        line = f"{index}. `{_user_label(row)}` - `{int(row.get('requestCount') or 0)}건`"
        if error_count > 0:
            line += f" (`오류 {error_count}건`)"
        lines.append(line)
    return "\n".join(lines)


def _format_request_log_routes(result: dict[str, Any], spec: RequestLogQuerySpec) -> str:
    rows = [row for row in (result.get("rows") or []) if isinstance(row, dict)]
    lines = [
        "*요청 로그 라우트 통계*",
        f"• 기준: {spec.scope_label}",
        f"• 전체 요청: `{int(result.get('totalCount') or 0)}건`",
        f"• 고유 라우트: `{int(result.get('uniqueRouteCount') or 0)}개`",
        f"• 표시 라우트: 상위 `{spec.limit}개`",
    ]
    if not rows:
        lines.append("• 결과: 조건에 맞는 요청 로그가 없어")
        return "\n".join(lines)

    for index, row in enumerate(rows, start=1):
        error_count = int(row.get("errorCount") or 0)
        line = f"{index}. `{_route_label(row)}` - `{int(row.get('requestCount') or 0)}건`"
        if error_count > 0:
            line += f" (`오류 {error_count}건`)"
        lines.append(line)
    return "\n".join(lines)


def _time_label(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "시간 미상"
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return text


def _user_label(row: dict[str, Any]) -> str:
    return str(row.get("userLabel") or row.get("userName") or row.get("userId") or "unknown").strip()


def _route_label(row: dict[str, Any]) -> str:
    route_name = str(row.get("routeName") or "").strip() or "unknown"
    route_mode = str(row.get("routeMode") or "").strip()
    if route_mode:
        return f"{route_name} / {route_mode}"
    return route_name


def _status_label(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "handled"
    if text == "error":
        return "error"
    return text


def _handler_type_label(value: Any) -> str:
    return str(value or "").strip() or "unknown"


def _compact_request_text(row: dict[str, Any]) -> str:
    raw_text = str(row.get("normalizedQuestion") or row.get("requestText") or "").strip()
    compact = " ".join(raw_text.replace("`", "'").split())
    if not compact:
        return "(질문 없음)"
    if len(compact) > 90:
        return f"{compact[:87]}..."
    return compact
