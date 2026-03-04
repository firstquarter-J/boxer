import json
from urllib import error, parse, request

from boxer.company import settings as cs
from boxer.core.utils import _display_value


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _should_lookup_barcode(question: str, barcode: str) -> bool:
    normalized = (question or "").strip()
    lowered = normalized.lower()
    if normalized == barcode:
        return True

    if normalized.startswith(barcode):
        suffix = normalized[len(barcode) :].strip()
        if suffix in {
            "",
            "조회",
            "조회해줘",
            "확인",
            "확인해줘",
            "정보",
            "정보줘",
            "정보 알려줘",
            "유저 정보",
            "사용자 정보",
        }:
            return True

    if "바코드" in normalized or "barcode" in lowered:
        return True

    user_target_hints_ko = (
        "유저",
        "사용자",
        "회원",
        "고객",
        "산모",
        "아이",
        "아기",
        "앱유저",
        "앱 유저",
        "프로필",
    )
    user_target_hints_en = ("user", "app-user", "app user", "baby", "profile")
    lookup_action_hints_ko = ("조회", "확인", "알려", "보여", "찾아", "검색", "정보")
    lookup_action_hints_en = ("lookup", "find", "search", "info", "profile", "details")

    has_user_target = _contains_any(normalized, user_target_hints_ko) or _contains_any(
        lowered, user_target_hints_en
    )
    has_lookup_action = _contains_any(normalized, lookup_action_hints_ko) or _contains_any(
        lowered, lookup_action_hints_en
    )
    return has_user_target and has_lookup_action


def _lookup_app_user_by_barcode(barcode: str) -> str:
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

    users = payload.get("data")
    if not isinstance(users, list) or not users:
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
