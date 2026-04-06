"""Slack adapter package for Boxer.

`boxer` 패키지는 채널 중립 코어를 유지하고,
Slack 런타임과 Slack 전용 어댑터는 이 패키지로 분리한다.
"""

from boxer_adapter_slack.common import (
    MentionPayload,
    SlackReplyFn,
    create_slack_app,
    merge_request_log_metadata,
    set_request_log_route,
    set_request_log_skip_persist,
    set_request_log_status,
)

__all__ = [
    "MentionPayload",
    "SlackReplyFn",
    "create_slack_app",
    "merge_request_log_metadata",
    "set_request_log_route",
    "set_request_log_skip_persist",
    "set_request_log_status",
]
