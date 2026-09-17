"""자동 업데이트 명령을 외부 조회 없이 분류하는 Slack/API 공통 계약이다."""

from __future__ import annotations

import re
from dataclasses import dataclass

DAILY_AUTO_UPDATE_ROUTE = "daily_auto_update_control"
AUTO_UPDATE_OPTION_KEYS = {
    "agent": "autoUpdateAgent",
    "box_free": "autoUpdateBoxFree",
    "box_paid": "autoUpdateBoxPaid",
}
AUTO_UPDATE_LABELS = {
    "agent": "에이전트",
    "box_free": "마미박스 무료병원",
    "box_paid": "마미박스 유료병원",
}


@dataclass(frozen=True, slots=True)
class DailyAutoUpdateCommand:
    action: str
    target: str | None = None


def parse_daily_auto_update_command(question: str) -> DailyAutoUpdateCommand | None:
    # 전체 문장이 짧은 명령과 일치할 때만 변경한다. 부정·조건·복합 명령이나
    # 인용된 명령의 설명 요청을 부분 문자열만 보고 실행하지 않는다.
    text = re.sub(r"^(?:\s*<@[^>]+>)+", "", str(question or ""))
    compact = re.sub(r"[\s`]+", "", text.lower()).strip(".!~")
    if "자동업데이트" not in compact or not any(
        token in compact
        for token in (
            "마미박스",
            "박스",
            "box",
            "에이전트",
            "agent",
            "데일리",
            "일일",
            "순회",
        )
    ):
        return None

    prefix, _, suffix = compact.partition("자동업데이트")
    targets = {"에이전트": "agent", "agent": "agent"}
    for box in ("마미박스", "박스", "mommybox", "box"):
        targets[box] = "box_qualifier_required"
        for qualifier, target in (("무료", "box_free"), ("유료", "box_paid")):
            for hospital in ("", "병원"):
                targets[box + qualifier + hospital] = target
                targets[qualifier + hospital + box] = target
    target = targets.get(prefix)
    daily = prefix in {"데일리", "일일", "순회", "데일리순회", "일일순회"}
    if target is None and not daily:
        return DailyAutoUpdateCommand("needs_input")

    if re.fullmatch(
        r"(?:상태|상태확인|상태조회|확인|조회|여부)(?:해|해줘|해요|해주세요|알려줘|보여줘)?[?？]?"
        r"|(?:켜져|꺼져)(?:있어|있나|있니|있나요|있는지)(?:알려줘|확인해줘)?[?？]?",
        suffix,
    ):
        return DailyAutoUpdateCommand("status")
    if re.fullmatch(
        r"(?:꺼|끄)(?:줘|주세요|줘요|세요|라|자|기)?|off|disable|"
        r"(?:비활성(?:화)?|중단|정지)(?:해|해줘|해주세요)?|멈춰(?:줘)?",
        suffix,
    ):
        return DailyAutoUpdateCommand("disable", target)
    if re.fullmatch(
        r"켜(?:줘|주세요|줘요|세요|라|자|기)?|on|enable|"
        r"(?:활성(?:화)?|재개|시작)(?:해|해줘|해주세요)?",
        suffix,
    ):
        return DailyAutoUpdateCommand("enable", target)
    return DailyAutoUpdateCommand("needs_input")
