from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Callable, Sequence
from typing import Any

from boxer_company_adapter_slack.hpa_change_api_client import (
    HpaChangeApiClient,
    HpaChangeRemoteDelivery,
)
from boxer_company_adapter_slack.hpa_change_reporter import (
    _format_hpa_change_poll_messages,
)


_REPORTER_THREAD: threading.Thread | None = None
_REPORTER_THREAD_LOCK = threading.Lock()
HpaChangeDeliveryRenderer = Callable[[HpaChangeRemoteDelivery], Sequence[str]]


def _default_renderer(delivery: HpaChangeRemoteDelivery) -> Sequence[str]:
    # 기존 공개 결과 필터를 그대로 재사용하되 job/GitHub state는 API가 소유한다.
    return _format_hpa_change_poll_messages(delivery.to_poll_result())


def run_hpa_change_remote_reporter_once(
    api_client: HpaChangeApiClient,
    slack_client: Any,
    *,
    renderer: HpaChangeDeliveryRenderer = _default_renderer,
    logger: logging.Logger | None = None,
) -> int:
    """API pending을 Slack에 보내고 모든 댓글 성공 뒤 exact delivery를 ACK한다."""

    actual_logger = logger or logging.getLogger(__name__)
    delivered_count = 0
    deliveries = api_client.pull_pending()
    for delivery in deliveries:
        try:
            messages = tuple(str(item or "").strip() for item in renderer(delivery))
            if not messages or any(not item for item in messages):
                raise ValueError("HPA delivery renderer가 빈 메시지를 반환했어")
            for index, message in enumerate(messages, 1):
                # API ACK 응답이 유실돼 같은 pending을 다시 받아도 Slack dedupe key는 같다.
                client_msg_id = str(
                    uuid.uuid5(
                        uuid.NAMESPACE_URL,
                        f"boxer:hpa-delivery:{delivery.delivery_id}:{index}",
                    )
                )
                slack_client.chat_postMessage(
                    channel=delivery.channel_id,
                    thread_ts=delivery.thread_ts,
                    text=message,
                    client_msg_id=client_msg_id,
                    unfurl_links=False,
                    unfurl_media=False,
                )
            # Slack 전송 중 하나라도 실패하면 ACK하지 않아 API pending을 보존한다.
            api_client.acknowledge_delivery(delivery)
            delivered_count += 1
        except Exception as exc:
            actual_logger.warning(
                "Failed to deliver HPA API message task_id=%s error_type=%s",
                delivery.task_id,
                type(exc).__name__,
            )
    return delivered_count


def _reporter_loop(
    api_client: HpaChangeApiClient,
    slack_client: Any,
    poll_interval_sec: int,
    logger: logging.Logger,
) -> None:
    interval = max(1, int(poll_interval_sec))
    while True:
        try:
            run_hpa_change_remote_reporter_once(
                api_client,
                slack_client,
                logger=logger,
            )
        except Exception as exc:
            # API 장애 때 Slack-local workflow로 우회하지 않고 다음 transport poll만 기다린다.
            logger.warning(
                "Failed to pull HPA API deliveries error_type=%s",
                type(exc).__name__,
            )
        threading.Event().wait(interval)


def attach_hpa_change_remote_reporter(
    app: Any,
    api_client: HpaChangeApiClient,
    *,
    poll_interval_sec: int,
    logger: logging.Logger | None = None,
) -> None:
    actual_logger = logger or logging.getLogger(__name__)
    slack_client = getattr(app, "client", None)
    if slack_client is None:
        raise RuntimeError("HPA remote reporter를 시작할 Slack client가 없어")
    global _REPORTER_THREAD
    with _REPORTER_THREAD_LOCK:
        if _REPORTER_THREAD is not None and _REPORTER_THREAD.is_alive():
            return
        _REPORTER_THREAD = threading.Thread(
            target=_reporter_loop,
            args=(api_client, slack_client, poll_interval_sec, actual_logger),
            name="hpa-change-remote-reporter",
            daemon=True,
        )
        _REPORTER_THREAD.start()
    actual_logger.info("Started HPA change remote transport reporter")


__all__ = [
    "attach_hpa_change_remote_reporter",
    "run_hpa_change_remote_reporter_once",
]
