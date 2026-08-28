from __future__ import annotations

import logging
from unittest.mock import patch

from boxer_company_adapter_slack.thread_learning_routes import (
    _load_thread_context_entries_for_learning,
)


class _SlackClient:
    def conversations_replies(self, **_kwargs: object) -> dict[str, object]:
        return {
            "messages": [
                {"ts": "1.0", "user": "U1", "text": "원인 확인 요청"},
                {"ts": "1.1", "user": "U2", "text": "처리 방법 안내"},
                {"ts": "1.2", "user": "U3", "text": "이 스레드 학습"},
            ],
            "has_more": False,
        }


def test_loader_only_collects_bounded_slack_context_for_remote_operation() -> None:
    with patch(
        "boxer_company_adapter_slack.thread_learning_routes.cs.THREAD_PLAYBOOK_LEARNING_FETCH_LIMIT",
        10,
    ):
        entries = _load_thread_context_entries_for_learning(
            _SlackClient(),
            logging.getLogger("test.thread.context"),
            channel_id="C123456",
            thread_ts="1.0",
            current_ts="1.2",
        )

    assert [entry["text"] for entry in entries] == ["원인 확인 요청", "처리 방법 안내"]


def test_loader_does_not_call_slack_without_thread_identity() -> None:
    entries = _load_thread_context_entries_for_learning(
        object(),
        logging.getLogger("test.thread.empty"),
        channel_id="",
        thread_ts="",
        current_ts="",
    )

    assert entries == ()
