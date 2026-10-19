"""질문의 상대 날짜를 해석할 기준 시각을 답변 모델에 전달한다."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import re
from zoneinfo import ZoneInfo

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.memory.live_query import resolve_live_query_period


_KST = ZoneInfo("Asia/Seoul")
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
            microseconds=int(match.group(2)[:6].ljust(6, "0")))).astimezone(_KST)
    except (OverflowError, ValueError):
        return None


def build_request_reference_context(
    request: CompanyAssistantRequest, *, now: datetime | None = None,
) -> str:
    question_time = _question_time(request)
    reference = question_time or now or datetime.now(timezone.utc)
    # 서버 시계는 timezone-aware UTC/KST만 사용하고 테스트의 naive 값도 UTC로 명시한다.
    if reference.tzinfo is None:
        reference = reference.replace(tzinfo=timezone.utc)
    reference = reference.astimezone(_KST)
    elapsed = reference.astimezone(timezone.utc) - _EPOCH
    stamp = f"{elapsed.days * 86400 + elapsed.seconds}.{elapsed.microseconds:06d}"
    facts: dict[str, object] = {
        "timeZone": "Asia/Seoul",
        "referenceTimeKst": reference.isoformat(timespec="microseconds"),
        "referenceDateKst": reference.date().isoformat(),
        "timeBasis": "slack_question" if question_time is not None else "server_clock",
    }
    period = resolve_live_query_period(request.question, stamp)
    if period is not None:
        facts["requestedPeriod"] = {
            **{key: period[key] for key in ("kind", "startAtKst", "endAtKst")},
            "endExclusive": True,
            "cutoffAtKst": reference.isoformat(timespec="microseconds"),
        }
    # 발언의 상대 날짜와 질문의 상대 날짜를 같은 기준일로 섞지 않도록 따로 설명한다.
    return (
        "요청 기준 정보(서버 확인):\n"
        + json.dumps(facts, ensure_ascii=False, separators=(",", ":"))
        + "\n질문의 오늘·어제는 위 질문 기준 KST 날짜로 해석해. "
        "발언 본문 안의 오늘·어제는 그 발언의 sourceTimes.spokenAtKst를 기준으로 해석해. "
        "기준일이 제공됐으므로 현재 날짜를 모른다고 하거나 날짜 확인을 다시 요청하지 마. "
        "기준 정보의 내부 필드명은 답변에 노출하지 마."
    )
