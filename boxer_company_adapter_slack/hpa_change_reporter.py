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


def _item_text(item: Any, preferred_keys: Sequence[str]) -> str:
    if isinstance(item, Mapping):
        for key in preferred_keys:
            value = item.get(key)
            if value is not None and str(value).strip():
                return _safe_review_line(value)
        return ""
    return _safe_review_line(item)


def _collect_review_items(
    result: Mapping[str, Any],
    *,
    keys: Sequence[str],
    preferred_keys: Sequence[str],
    limit: int = 3,
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
                text = _item_text(item, preferred_keys)
                if text and text not in rendered:
                    rendered.append(text)
                if len(rendered) >= limit:
                    return rendered
    return rendered


def _safe_pr_urls(values: Sequence[Any]) -> list[str]:
    urls: list[str] = []
    for value in values:
        url = str(value or "").strip()
        if _SAFE_PR_URL_RE.fullmatch(url) and url not in urls:
            urls.append(url)
        if len(urls) >= 4:
            break
    return urls


def _format_hpa_change_poll_message(poll: HpaChangePollResult) -> str:
    task_id = _safe_review_line(poll.task_id, max_chars=80)
    base_lines = [f"• 요청 ID: `{task_id}`"]

    # 요청 원문·첨부·오류 원문은 출력하지 않고
    # worker가 구조화한 review 항목만 짧게 보여준다.
    result = poll.result if isinstance(poll.result, Mapping) else {}
    corrections = _collect_review_items(
        result,
        keys=("corrections",),
        preferred_keys=("correction", "summary", "message", "title"),
    )
    questions = _collect_review_items(
        result,
        keys=("blocking_questions", "blockingQuestions", "questions"),
        preferred_keys=("question", "summary", "message", "title"),
    )

    if poll.state is HpaChangePollState.RUNNING:
        return "\n".join(["*HPA 코드 변경 작업 진행 중*", *base_lines])

    if poll.state is HpaChangePollState.NEEDS_CLARIFICATION:
        lines = ["*HPA 코드 변경 검토 중 추가 확인이 필요해*", *base_lines]
        lines.extend(f"• 정정: {item}" for item in corrections)
        lines.extend(f"• 확인 질문: {item}" for item in questions)
        if not corrections and not questions:
            lines.append(
                "• 확인 질문: 구현 전에 결정할 내용이 있어. "
                "요청 담당자가 결과를 확인해줘"
            )
        return "\n".join(lines)

    if poll.state is HpaChangePollState.PR_OPENED:
        lines = ["*HPA 코드 변경 PR 준비 완료*", *base_lines]
        lines.extend(f"• 정정: {item}" for item in corrections)
        pr_urls = _safe_pr_urls(poll.pr_urls)
        lines.extend(f"• PR: {url}" for url in pr_urls)
        if not pr_urls:
            lines.append("• PR: 유효한 PR 링크를 확인하지 못했어")
        lines.append("• 다음 단계: 현 승인 후 머지·배포")
        return "\n".join(lines)

    if poll.state is HpaChangePollState.FAILED:
        return "\n".join(
            [
                "*HPA 코드 변경 자동화가 완료되지 못했어*",
                *base_lines,
                (
                    "• 안내: 내부 오류 원문은 노출하지 않았어. "
                    "운영 로그와 worker 상태를 확인해줘"
                ),
            ]
        )

    return ""


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
    client.chat_postMessage(
        channel=job.channel_id,
        thread_ts=job.thread_ts,
        text=text,
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

            message = _format_hpa_change_poll_message(poll)
            if not message:
                continue
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
