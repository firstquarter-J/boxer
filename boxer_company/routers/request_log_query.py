from __future__ import annotations

from boxer_company._operation_routing_private import (
    RequestLogQuerySpec,
)

from datetime import datetime
from typing import Any

from boxer.observability.request_log import (
    _list_request_log_recent,
    _summarize_request_log_by_route,
    _summarize_request_log_by_user,
    _summarize_request_log_overview,
)


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
