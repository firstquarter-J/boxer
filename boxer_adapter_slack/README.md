# Boxer Slack Adapter

Public Slack adapter package for Boxer.

이 패키지는 `boxer` 위에 붙는 공개 Slack runtime / adapter authoring helper를 제공한다.

This package contains:

- Slack runtime
- common event/reply wrapper
- sample/reference adapter

## Install

```bash
pip install -e .
pip install -e ./boxer_adapter_slack
```

## Public API

- `from boxer_adapter_slack import create_slack_app`
- `from boxer_adapter_slack import set_request_log_route`
- `from boxer_adapter_slack import MentionPayload, SlackReplyFn`

## Minimal Example

```python
from slack_bolt import App

from boxer_adapter_slack import (
    MentionPayload,
    SlackReplyFn,
    create_slack_app,
    set_request_log_route,
)


def create_app() -> App:
    def _handle_mention(payload: MentionPayload, reply: SlackReplyFn, _client, _logger) -> None:
        if payload["question"].strip() == "ping":
            set_request_log_route(payload, "sample_ping")
            reply("pong")
            return
        set_request_log_route(payload, "sample_default")
        reply("slack adapter is running")

    return create_slack_app(_handle_mention)
```

CLI entrypoint는 `boxer-slack`이다.
