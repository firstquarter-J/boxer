"""질문의 상대 날짜를 해석할 기준 시각을 답변 모델에 전달한다."""

from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.deployment import get_deployment_profile, get_local_zone
from boxer_company.memory.live_query import (
    live_query_period_error,
    resolve_live_query_period,
)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_SLACK_TS = re.compile(r"([0-9]{1,20})\.([0-9]{1,9})")


def _question_time(request: CompanyAssistantRequest) -> datetime | None:
    memory = request.memory_context or {}
    # 질문 본문·임의 metadata가 날짜를 바꾸지 않게 실제 Slack 요청과 맞는 식별자만 쓴다.
    if (request.channel != "slack" or not request.actor_id
            or memory.get("workspaceId") != request.tenant_id
            or memory.get("actorId") != request.actor_id
            or not memory.get("channelId")
            or memory.get("channelId") != request.metadata.get("channel_id")
            or memory.get("threadTs") != request.conversation_id
            or memory.get("triggerKind") != "app_mention"):
        return None
    value = memory.get("currentMessageTs")
    match = _SLACK_TS.fullmatch(value) if isinstance(value, str) else None
    if match is None:
        return None
    try:
        # 조회 helper와 같은 입력 범위를 받되 모델 표시용 시각만 microsecond로 자른다.
        return (_EPOCH + timedelta(seconds=int(match.group(1)),
            microseconds=int(match.group(2)[:6].ljust(6, "0")))).astimezone(get_local_zone())
    except (OverflowError, ValueError):
        return None


def build_request_reference_context(
    request: CompanyAssistantRequest, *, now: datetime | None = None,
) -> str:
    question_time = _question_time(request)
    reference = question_time or now or datetime.now(timezone.utc)
    # 실제 질문 시각은 UTC로 보존하고 상대 날짜에만 서버의 배포 시간대를 적용한다.
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    profile = get_deployment_profile()
    reference = reference.astimezone(profile.timezone)
    elapsed = reference.astimezone(timezone.utc) - _EPOCH
    stamp = f"{elapsed.days * 86400 + elapsed.seconds}.{elapsed.microseconds:06d}"
    facts: dict[str, object] = {
        "timeZone": profile.timezone_name,
        "referenceTimeKst" if profile.country == "kr" else "referenceTime": reference.isoformat(timespec="microseconds"),
        "referenceDateKst" if profile.country == "kr" else "referenceDate": reference.date().isoformat(),
        "timeBasis": "slack_question" if question_time is not None else "server_clock",
    }
    # 기억의 승인 기간과 원문 정책은 한국 전용이다. 미국 일반 질문에 KST 기억 기간을 덧붙이지 않는다.
    period = resolve_live_query_period(request.question, stamp) if profile.country == "kr" else None
    if period is not None:
        facts["requestedPeriod"] = {
            **{key: period[key] for key in ("kind", "startAtKst", "endAtKst")},
            "endExclusive": True,
            "cutoffAtKst": reference.isoformat(timespec="microseconds"),
        }
    period_error = live_query_period_error(request.question, stamp) if profile.country == "kr" else None
    if period_error is not None:
        # 조회하지 않은 기간을 모델이 임의 보정하거나 최근 발언으로 채우지 않게 사유만 전달한다.
        facts["requestedPeriodError"] = period_error
    # 발언의 상대 날짜와 질문의 상대 날짜를 같은 기준일로 섞지 않도록 따로 설명한다.
    memory_time_instruction = (
        "발언 본문 안의 오늘·어제는 그 발언의 sourceTimes.spokenAtKst를 기준으로 해석해. "
        if profile.country == "kr" else ""
    )
    return (
        "요청 기준 정보(서버 확인):\n"
        + json.dumps(facts, ensure_ascii=False, separators=(",", ":"))
        + f"\n질문의 상대 날짜와 연도 없는 날짜는 위 질문 기준 {profile.timezone_label} 날짜로 해석해. "
        "요청 기간이 있으면 그 기간과 질문 시각까지의 확인된 근거만 사용해. "
        "대화 기록 조회에서 기간 오류가 있으면 날짜가 잘못됐는지, 여러 기간인지, 미래인지 설명하고 기간을 다시 확인해. "
        "기간 오류를 최근 발언 조회로 바꾸지 마. 일부 근거만 확인했다면 그 기간 전체를 집계했다고 말하지 마. "
        + memory_time_instruction
        + "기준일이 제공됐으므로 현재 날짜를 모른다고 하거나 날짜 확인을 다시 요청하지 마. "
        "기준 정보의 내부 필드명은 답변에 노출하지 마."
    )
