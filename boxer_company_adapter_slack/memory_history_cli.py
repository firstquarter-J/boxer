"""Slack 과거 대화의 읽기 전용 metadata 조사·수집·보호 파일 출력 CLI."""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
from zoneinfo import ZoneInfo

from boxer_company.base_access import BaseAccessSettings, build_base_access_store
from boxer_company.team_chat_context import TEAM_MEMBER_PROFILES
from boxer_company_adapter_slack.memory_history import IMPORT_CHANNEL, SlackHistoryCollector, period


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Slack 10월 대화를 발송 없이 조사·수집해")
    parser.add_argument("command", choices=("scan", "collect", "export"))
    parser.add_argument("--state", required=True)
    parser.add_argument("--output")
    parser.add_argument("--membership-state", required=True)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--token-env", default="SLACK_BOT_TOKEN")
    parser.add_argument("--tenant-id", required=True)
    parser.add_argument("--channel-id", default=IMPORT_CHANNEL, choices=(IMPORT_CHANNEL,))
    parser.add_argument("--from-kst", default="2026-10-01T00:00:00+09:00")
    parser.add_argument("--through-kst")
    parser.add_argument("--all-history-roots", action="store_true")
    parser.add_argument("--max-pages", type=int, default=5)
    args = parser.parse_args(argv)
    if args.command == "export" and not args.output:
        parser.error("export에는 --output이 필요해")
    try:
        # 비밀값은 지정된 로컬 credential source에서만 읽고 stdout에는 절대 넣지 않는다.
        from dotenv import load_dotenv
        from slack_sdk import WebClient

        load_dotenv(args.env_file, override=False)
        token = os.environ.get(args.token_env, "")
        if not token:
            raise ValueError("slack_credential_missing")
        client = WebClient(token=token, timeout=10, retry_handlers=[])
        auth = client.auth_test()
        if auth.get("team_id") != args.tenant_id:
            raise ValueError("workspace_mismatch")
        info = client.conversations_info(channel=args.channel_id)
        channel_kind = "private_channel" if info["channel"].get("is_private") else "channel"
        access = build_base_access_store(BaseAccessSettings(state_path=args.membership_state))
        authors = {str(profile["user_id"]) for profile in TEAM_MEMBER_PROFILES
                   if profile.get("user_id") and access.is_allowed(args.tenant_id, str(profile["user_id"]))}
        state = Path(args.state).expanduser()
        # 재시작 때는 저장된 cutoff를 쓰고 현재 시각으로 수집 창을 넓히지 않는다.
        window = None if state.exists() and args.through_kst is None else period(
            args.from_kst, args.through_kst or datetime.now(ZoneInfo("Asia/Seoul")).isoformat())
        collector = SlackHistoryCollector(client, state_path=state, tenant_id=args.tenant_id,
            author_ids=authors, is_allowed=access.is_allowed, window=window,
            all_history_roots=args.all_history_roots, channel_id=args.channel_id, channel_kind=channel_kind)
        if args.command == "scan":
            result = collector.scan(max_pages=args.max_pages)
        elif args.command == "collect":
            result = collector.collect(max_pages=args.max_pages)
        else:
            result = collector.export(args.output)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    except Exception as exc:
        # Slack SDK 오류 원문은 요청·credential을 포함할 수 있으므로 유형만 출력한다.
        if hasattr(exc, "retry_after"):
            print(json.dumps({"errorCode": "slack_rate_limited", "retryAfterSeconds": exc.retry_after}))
        raise SystemExit("기억 수집 실패: " + type(exc).__name__) from None


if __name__ == "__main__":
    main()
