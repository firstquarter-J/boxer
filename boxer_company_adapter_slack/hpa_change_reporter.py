from __future__ import annotations

import logging
import re
import threading
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from typing import Any

from boxer_company.hpa_change_workflow import (
    HpaChangeJob,
    HpaChangePollResult,
    HpaChangePollState,
    HpaChangeStatus,
    redact_sensitive_text,
)
from boxer_company_adapter_slack.hpa_change_runtime import HpaChangeRuntime


_REPORTABLE_STATUSES = (
    HpaChangeStatus.RECEIVED,
    HpaChangeStatus.DISPATCHING,
    HpaChangeStatus.DISPATCHED,
    HpaChangeStatus.RUNNING,
    HpaChangeStatus.WORKFLOW_SUCCEEDED,
    HpaChangeStatus.RESULT_READY,
    HpaChangeStatus.NEEDS_CLARIFICATION,
    HpaChangeStatus.PR_CREATED,
    HpaChangeStatus.FAILED,
    HpaChangeStatus.CANCELED,
)
_ACTIVE_STATUSES = frozenset(
    {
        HpaChangeStatus.RECEIVED,
        HpaChangeStatus.DISPATCHING,
        HpaChangeStatus.DISPATCHED,
        HpaChangeStatus.RUNNING,
        HpaChangeStatus.WORKFLOW_SUCCEEDED,
        HpaChangeStatus.RESULT_READY,
    }
)
_SAFE_PR_URL_RE = re.compile(
    r"^https://github\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/pull/[0-9]+/?$"
)
_SAFE_SLACK_USER_ID_RE = re.compile(r"^[UW][A-Z0-9]{5,20}$")
_REPORTER_THREAD: threading.Thread | None = None
_REPORTER_THREAD_LOCK = threading.Lock()


def _safe_review_line(value: Any, *, max_chars: int = 240) -> str:
    text = redact_sensitive_text(str(value or ""))
    text = re.sub(r"[\x00-\x1f\x7f]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    # worker 결과가 Slack mention/link 문법을 만들더라도
    # 알림 부작용이 생기지 않게 한다.
    text = text.replace("<", "‹").replace(">", "›")
    return text[:max_chars]


def _item_text(
    item: Any,
    preferred_keys: Sequence[str],
    *,
    max_chars: int = 240,
) -> str:
    if isinstance(item, Mapping):
        for key in preferred_keys:
            value = item.get(key)
            if value is not None and str(value).strip():
                return _safe_review_line(value, max_chars=max_chars)
        return ""
    return _safe_review_line(item, max_chars=max_chars)


def _collect_review_items(
    result: Mapping[str, Any],
    *,
    keys: Sequence[str],
    preferred_keys: Sequence[str],
    limit: int = 3,
    item_max_chars: int = 240,
) -> list[str]:
    review = result.get("review")
    containers = [result]
    if isinstance(review, Mapping):
        containers.append(review)

    rendered: list[str] = []
    for container in containers:
        for key in keys:
            raw_items = container.get(key)
            if raw_items is None:
                continue
            candidates = raw_items if isinstance(raw_items, (list, tuple)) else [raw_items]
            for item in candidates:
                text = _item_text(item, preferred_keys, max_chars=item_max_chars)
                if text and text not in rendered:
                    rendered.append(text)
                if len(rendered) >= limit:
                    return rendered
    return rendered


def _collect_review_entries(
    result: Mapping[str, Any],
    *,
    keys: Sequence[str],
    limit: int = 3,
) -> list[Any]:
    """구조화된 정정 항목의 claim/correction/evidence를 보존해 표시한다."""

    review = result.get("review")
    containers = [result]
    if isinstance(review, Mapping):
        containers.append(review)

    rendered: list[Any] = []
    seen: set[str] = set()
    for container in containers:
        for key in keys:
            raw_items = container.get(key)
            if raw_items is None:
                continue
            candidates = raw_items if isinstance(raw_items, (list, tuple)) else [raw_items]
            for item in candidates:
                fingerprint = repr(item)
                if fingerprint in seen:
                    continue
                seen.add(fingerprint)
                rendered.append(item)
                if len(rendered) >= limit:
                    return rendered
    return rendered


def _format_correction(item: Any) -> str:
    if not isinstance(item, Mapping):
        return _safe_review_line(item)
    claim = _safe_review_line(item.get("claim"), max_chars=160)
    correction = _safe_review_line(
        item.get("correction") or item.get("summary") or item.get("message"),
        max_chars=240,
    )
    evidence = _safe_review_line(item.get("evidence"), max_chars=180)
    parts: list[str] = []
    if claim:
        parts.append(f"요청 전제: {claim}")
    if correction:
        parts.append(f"HPA 적용: {correction}")
    if evidence:
        parts.append(f"근거: {evidence}")
    return " | ".join(parts) or _safe_review_line(item)


def _review_summary(result: Mapping[str, Any]) -> str:
    values = _collect_review_items(
        result,
        keys=("summary",),
        preferred_keys=("summary", "message", "title"),
        limit=1,
    )
    return values[0] if values else ""


def _review_decision(result: Mapping[str, Any]) -> str:
    values = _collect_review_items(
        result,
        keys=("hpaDecision", "hpa_decision", "recommendedApproach", "recommended_approach"),
        preferred_keys=("decision", "summary", "message", "title"),
        limit=1,
        item_max_chars=500,
    )
    return values[0] if values else ""


def _review_requester_guidance(result: Mapping[str, Any]) -> str:
    values = _collect_review_items(
        result,
        keys=("requesterGuidance", "requester_guidance", "migrationGuidance"),
        preferred_keys=("guidance", "summary", "message", "title"),
        limit=1,
        item_max_chars=500,
    )
    return values[0] if values else (
        "CR Web 코드는 HPA에 그대로 사용할 수 없어. "
        "HPA의 실제 구조에 맞춰 의도만 변환해서 적용할게."
    )


def _safe_pr_urls(values: Sequence[Any]) -> list[str]:
    urls: list[str] = []
    for value in values:
        url = str(value or "").strip()
        if _SAFE_PR_URL_RE.fullmatch(url) and url not in urls:
            urls.append(url)
        if len(urls) >= 4:
            break
    return urls


def _requester_mention(job: HpaChangeJob) -> str:
    """결과·정정·추가 질문을 원 요청자에게 직접 전달하되 Slack ID를 검증한다."""

    requester_id = str(job.requested_by or "").strip()
    if not _SAFE_SLACK_USER_ID_RE.fullmatch(requester_id):
        return ""
    return f"<@{requester_id}>"


def _format_hpa_change_poll_messages(poll: HpaChangePollResult) -> list[str]:
    task_id = _safe_review_line(poll.task_id, max_chars=80)
    base_lines = [f"• 요청 ID: `{task_id}`"]

    # 요청 원문·첨부·오류 원문은 출력하지 않고
    # worker가 구조화한 review 항목만 짧게 보여준다.
    result = poll.result if isinstance(poll.result, Mapping) else {}
    corrections = _collect_review_entries(
        result,
        keys=("corrections",),
    )
    adaptations = _collect_review_items(
        result,
        keys=(
            "hpaAdaptations",
            "hpa_adaptations",
            "productizationNotes",
            "productization_notes",
        ),
        preferred_keys=("adaptation", "summary", "message", "title"),
        limit=5,
    )
    questions = _collect_review_items(
        result,
        keys=("blocking_questions", "blockingQuestions", "questions"),
        preferred_keys=("question", "summary", "message", "title"),
        limit=5,
        item_max_chars=600,
    )
    summary = _review_summary(result)
    decision = _review_decision(result)
    requester_guidance = _review_requester_guidance(result)

    if poll.state is HpaChangePollState.RUNNING:
        return ["\n".join(["*HPA 코드 변경 작업 진행 중*", *base_lines])]

    if poll.state is HpaChangePollState.NEEDS_CLARIFICATION:
        review_lines = ["*HPA 코드 변경 검토 결과*", "• 상태: 추가 확인 필요", *base_lines]
        review_lines.append(f"• 요청자 안내: {requester_guidance}")
        if summary:
            review_lines.append(f"• 검토 요약: {summary}")
        if decision:
            review_lines.append(f"• HPA 최종 적용안: {decision}")
        review_lines.extend(
            f"• HPA 기준 정정: {_format_correction(item)}" for item in corrections
        )
        review_lines.extend(f"• HPA 적용 방식: {item}" for item in adaptations)
        if not summary and not corrections and not adaptations:
            review_lines.append("• 검토 내용: HPA 실제 코드 확인이 더 필요해")

        question_lines = ["*HPA 추가 확인 질문*", *base_lines]
        if questions:
            question_lines.extend(
                f"• 질문 {index}: {item}" for index, item in enumerate(questions, 1)
            )
        else:
            question_lines.append(
                "• 질문 1: 구현 전에 결정할 내용이 있어. 요청 담당자가 결과를 확인해줘"
            )
        return ["\n".join(review_lines), "\n".join(question_lines)]

    if poll.state is HpaChangePollState.PR_OPENED:
        lines = ["*HPA 코드 변경 PR 준비 완료*", *base_lines]
        lines.append(f"• 요청자 안내: {requester_guidance}")
        if summary:
            lines.append(f"• 검토 요약: {summary}")
        if decision:
            lines.append(f"• HPA 최종 적용안: {decision}")
        lines.extend(
            f"• HPA 기준 정정: {_format_correction(item)}" for item in corrections
        )
        lines.extend(f"• HPA 적용 방식: {item}" for item in adaptations)
        pr_urls = _safe_pr_urls(poll.pr_urls)
        lines.extend(f"• PR: {url}" for url in pr_urls)
        if not pr_urls:
            lines.append("• PR: 유효한 PR 링크를 확인하지 못했어")
        lines.append("• 다음 단계: 현 승인 후 머지·배포")
        return ["\n".join(lines)]

    if poll.state is HpaChangePollState.FAILED:
        return [
            "\n".join(
                [
                "*HPA 코드 변경 자동화가 완료되지 못했어*",
                *base_lines,
                (
                    "• 안내: 내부 오류 원문은 노출하지 않았어. "
                    "운영 로그와 worker 상태를 확인해줘"
                ),
                ]
            )
        ]

    return []


def _format_hpa_change_poll_message(poll: HpaChangePollResult) -> str:
    """호출부 호환을 위해 첫 번째(검토 요약) 댓글만 반환한다."""

    messages = _format_hpa_change_poll_messages(poll)
    return messages[0] if messages else ""


def _is_timed_out(job: HpaChangeJob, runtime: HpaChangeRuntime, now: datetime) -> bool:
    if job.status not in _ACTIVE_STATUSES:
        return False
    created_at = job.created_at
    if created_at.tzinfo is None:
        created_at = created_at.replace(tzinfo=timezone.utc)
    elapsed_seconds = (
        now.astimezone(timezone.utc) - created_at.astimezone(timezone.utc)
    ).total_seconds()
    return elapsed_seconds > max(1, int(runtime.run_timeout_sec))


def _post_hpa_change_message(client: Any, job: HpaChangeJob, text: str) -> None:
    # 자동 결과가 thread에서 묻히지 않도록 요청자를 먼저 멘션한다.
    mention = _requester_mention(job)
    message = f"{mention} {text}" if mention else text
    client.chat_postMessage(
        channel=job.channel_id,
        thread_ts=job.thread_ts,
        text=message,
        unfurl_links=False,
        unfurl_media=False,
    )


def run_hpa_change_reporter_once(
    runtime: HpaChangeRuntime,
    client: Any,
    *,
    logger: logging.Logger | None = None,
    now: datetime | None = None,
) -> int:
    """SQLite 작업을 한 번 전진시키고 발송 성공 상태만 기록한다."""

    if not runtime.enabled or runtime.store is None or runtime.workflow is None:
        return 0
    actual_logger = logger or logging.getLogger(__name__)
    actual_now = now or datetime.now(timezone.utc)
    sent_count = 0

    list_reportable_jobs = getattr(runtime.store, "list_reportable_jobs", None)
    if callable(list_reportable_jobs):
        # 이미 알린 오래된 terminal row를 SQL에서 제외해
        # 누적 작업이 새 job을 가리지 않게 한다.
        jobs = list_reportable_jobs(limit=500)
    else:
        jobs = runtime.store.list_jobs(statuses=_REPORTABLE_STATUSES, limit=500)
    for listed_job in jobs:
        try:
            # terminal 알림을 이미 보낸 작업은 GitHub를 다시 조회하지 않는다.
            listed_terminal_state = {
                HpaChangeStatus.NEEDS_CLARIFICATION: HpaChangePollState.NEEDS_CLARIFICATION,
                HpaChangeStatus.PR_CREATED: HpaChangePollState.PR_OPENED,
                HpaChangeStatus.FAILED: HpaChangePollState.FAILED,
                HpaChangeStatus.CANCELED: HpaChangePollState.FAILED,
            }.get(listed_job.status)
            if (
                listed_terminal_state is not None
                and listed_job.notified_status == listed_terminal_state.value
            ):
                continue

            if _is_timed_out(listed_job, runtime, actual_now):
                # timeout도 merge/deploy 같은 복구 동작 없이
                # 실패 상태와 알림만 남긴다.
                runtime.store.mark_failed(
                    listed_job.task_id,
                    "HPA 변경 worker 실행 제한 시간 초과",
                )
            poll = runtime.workflow.poll_job(listed_job.task_id)
            if poll.state is HpaChangePollState.QUEUED:
                continue
            if poll.job.notified_status == poll.state.value:
                continue

            messages = _format_hpa_change_poll_messages(poll)
            if not messages:
                continue
            # 검토 요약을 먼저 올리고, 추가 질문은 다음 thread 댓글로 분리한다.
            # 어느 한 댓글이라도 실패하면 notified를 남기지 않아 다음 poll에서 재시도한다.
            for message in messages:
                _post_hpa_change_message(client, poll.job, message)
            # Slack 발송이 성공한 뒤에만 표시해서
            # 재시작·일시 오류 시 알림이 유실되지 않게 한다.
            runtime.store.mark_notified(poll.task_id, poll.state)
            sent_count += 1
        except Exception as exc:
            # request/result 원문과 GitHub 응답을 로그에 흘리지 않는다.
            actual_logger.warning(
                "Failed to poll or report HPA change task_id=%s error_type=%s",
                listed_job.task_id,
                type(exc).__name__,
            )
    return sent_count


def _hpa_change_reporter_loop(
    runtime: HpaChangeRuntime,
    client: Any,
    logger: logging.Logger,
) -> None:
    interval = max(1, int(runtime.poll_interval_sec))
    while True:
        run_hpa_change_reporter_once(runtime, client, logger=logger)
        threading.Event().wait(interval)


def attach_hpa_change_reporter(
    app: Any,
    runtime: HpaChangeRuntime,
    *,
    logger: logging.Logger | None = None,
) -> None:
    if not runtime.enabled:
        return
    actual_logger = logger or logging.getLogger(__name__)
    client = getattr(app, "client", None)
    if client is None:
        # 활성화 설정인데 client가 없으면 알림을 시작하지 않아
        # silently 잃어버리지 않는다.
        raise RuntimeError("HPA 변경 reporter를 시작할 Slack client가 없어")

    global _REPORTER_THREAD
    with _REPORTER_THREAD_LOCK:
        if _REPORTER_THREAD is not None and _REPORTER_THREAD.is_alive():
            return
        _REPORTER_THREAD = threading.Thread(
            target=_hpa_change_reporter_loop,
            args=(runtime, client, actual_logger),
            name="hpa-change-reporter",
            daemon=True,
        )
        _REPORTER_THREAD.start()
    actual_logger.info("Started HPA change reporter")


__all__ = [
    "attach_hpa_change_reporter",
    "run_hpa_change_reporter_once",
]
