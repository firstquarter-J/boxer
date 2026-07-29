import json
from datetime import datetime
from typing import Any
from urllib import error, parse, request

from boxer_company import settings as cs
from boxer.core.utils import _display_value


_BABY_SELECTION_CONTEXT_KEYWORDS = (
    "유저 조회",
    "유저조회",
    "산모 조회",
    "산모조회",
    "람다",
    "lambda",
)
_BABY_SELECTION_ISSUE_KEYWORDS = (
    "안 나",
    "안나",
    "누락",
    "한 명만",
    "한명만",
    "하나만",
    "선택",
)
_BABY_SELECTION_ANALYSIS_KEYWORDS = (
    "원인",
    "왜",
    "분석",
)
_BABY_SELECTION_EXPLANATION = (
    "임신 중인 태아는 한 명만(다태아가 아닌 이상) 존재해야 하는데, "
    "태아 상태 아이가 두 명이라 출산예정일이 가장 먼 아이가 선택된거야."
)


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _should_analyze_app_user_baby_selection(
    question: str,
    barcode: str,
) -> bool:
    normalized = (question or "").strip().lower()
    if not normalized or barcode not in normalized:
        return False
    return _contains_any(
        normalized,
        _BABY_SELECTION_CONTEXT_KEYWORDS,
    ) and _contains_any(
        normalized,
        _BABY_SELECTION_ISSUE_KEYWORDS,
    ) and _contains_any(
        normalized,
        _BABY_SELECTION_ANALYSIS_KEYWORDS,
    )


def _should_lookup_barcode(question: str, barcode: str) -> bool:
    normalized = (question or "").strip()
    lookup_keywords = ("유저 조회", "유저조회", "산모 조회", "산모조회")

    non_profile_hints_ko = ("영상", "녹화", "촬영", "로그", "개수", "갯수", "최신", "마지막")
    has_non_profile_hint = _contains_any(normalized, non_profile_hints_ko)
    has_lookup_keyword = _contains_any(normalized, lookup_keywords)
    if has_non_profile_hint and not has_lookup_keyword:
        return False

    if normalized.startswith(barcode):
        suffix = normalized[len(barcode) :].strip()
        return suffix in lookup_keywords

    return has_lookup_keyword


def _request_app_users_by_barcode(barcode: str) -> list[dict[str, Any]]:
    if not cs.APP_USER_API_URL:
        raise RuntimeError("APP_USER_API_URL is empty")

    timeout_sec = max(1, cs.APP_USER_API_TIMEOUT_SEC)
    query = parse.urlencode({"barcode": barcode})
    delimiter = "&" if "?" in cs.APP_USER_API_URL else "?"
    endpoint = f"{cs.APP_USER_API_URL}{delimiter}{query}"
    req = request.Request(url=endpoint, method="GET")
    try:
        with request.urlopen(req, timeout=timeout_sec) as response:
            body = response.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"app-user API HTTP {exc.code}: {detail[:200]}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"app-user API connection failed: {exc.reason}") from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("app-user API returned invalid JSON") from exc

    if not isinstance(payload, dict):
        raise RuntimeError("app-user API returned invalid payload")
    users = payload.get("data")
    if users is None:
        return []
    if not isinstance(users, list) or not all(
        isinstance(user, dict) for user in users
    ):
        raise RuntimeError("app-user API returned invalid users")
    return users


def _parse_birth_date(value: object) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None


def _format_embryo_count(count: int) -> str:
    return "두 명" if count == 2 else f"{count}명"


def _analyze_app_user_baby_selection_by_barcode(barcode: str) -> str:
    users = _request_app_users_by_barcode(barcode)
    if not users:
        return f"바코드 {barcode}로 조회된 유저가 없어"

    babies = users[0].get("babies")
    if not isinstance(babies, list) or not all(
        isinstance(baby, dict) for baby in babies
    ):
        raise RuntimeError("app-user API returned invalid babies")
    if len(babies) <= 1:
        return (
            "Lambda 조회 결과에서 태아 상태 아이가 한 명이라 "
            "출산예정일로 한 명을 선택하는 로직이 원인이라고 볼 수 없어."
        )

    # Lambda는 babyStatus=EMBRYO만 반환한다. HPA와 동일하게 유효한
    # 다태아 묶음이 있으면 그 묶음을 유지하고, 없을 때만 예정일을 비교한다.
    twin_baby = next(
        (baby for baby in babies if baby.get("twinFlag") == 1),
        None,
    )
    twin_key = twin_baby.get("twinKey") if twin_baby else None
    twin_babies = (
        [
            baby
            for baby in babies
            if baby.get("twinFlag") == 1
            and baby.get("twinKey") == twin_key
        ]
        if twin_key
        else []
    )
    if twin_babies:
        return (
            "Lambda 조회 결과에서 다태아로 식별된 아이들이라 "
            "출산예정일이 가장 먼 한 명만 선택하는 경우가 아니야."
        )

    birth_dates = [_parse_birth_date(baby.get("birthDate")) for baby in babies]
    if any(birth_date is None for birth_date in birth_dates):
        return (
            "태아 상태 아이가 여러 명이지만 출산예정일이 없는 아이가 있어서 "
            "가장 먼 예정일 선택 로직이 원인인지 확정할 수 없어."
        )
    if len(set(birth_dates)) != len(birth_dates):
        return (
            "태아 상태 아이가 여러 명이지만 출산예정일이 같아서 "
            "가장 먼 예정일 선택 로직이 원인인지 확정할 수 없어."
        )

    count_label = _format_embryo_count(len(babies))
    if len(babies) == 2:
        return _BABY_SELECTION_EXPLANATION
    return (
        "임신 중인 태아는 한 명만(다태아가 아닌 이상) 존재해야 하는데, "
        f"태아 상태 아이가 {count_label}이라 "
        "출산예정일이 가장 먼 아이가 선택된거야."
    )


def _lookup_app_user_by_barcode(barcode: str) -> str:
    users = _request_app_users_by_barcode(barcode)
    if not users:
        return f"바코드 {barcode}로 조회된 유저가 없어"

    lines = [
        f"*바코드 조회 결과* :barcode: `{barcode}`",
        f"• 조회 건수: *{len(users)}건*",
    ]
    for user_index, user in enumerate(users, start=1):
        user_phone = _display_value(user.get("userPhoneNumber"), default="null")
        user_seq = _display_value(user.get("userSeq"), default="null")
        user_real_name = _display_value(user.get("userRealName"), default="null")
        lines.append("")
        lines.append(f"*user {user_index}*")
        lines.append(f"• `userPhoneNumber`: `{user_phone}`")
        lines.append(f"• `userSeq`: `{user_seq}`")
        lines.append(f"• `userRealName`: `{user_real_name}`")

        babies = user.get("babies")
        if not isinstance(babies, list) or not babies:
            lines.append("• `babies`: `[]`")
            continue

        for baby_index, baby in enumerate(babies, start=1):
            baby_seq = _display_value(baby.get("babySeq"), default="null")
            twin_key = _display_value(baby.get("twinKey"), default="null")
            twin_flag = _display_value(baby.get("twinFlag"), default="null")
            birth_date = _display_value(baby.get("birthDate"), default="null")
            baby_nickname = _display_value(baby.get("babyNickname"), default="null")
            lines.append(f"• `babies[{baby_index - 1}]`")
            lines.append(f"  - `babySeq`: `{baby_seq}`")
            lines.append(f"  - `twinKey`: `{twin_key}`")
            lines.append(f"  - `twinFlag`: `{twin_flag}`")
            lines.append(f"  - `birthDate`: `{birth_date}`")
            lines.append(f"  - `babyNickname`: `{baby_nickname}`")

    return "\n".join(lines)
