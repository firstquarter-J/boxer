"""과거 대화 수집의 기간·작성자·재시작·보호 파일 계약을 검증한다."""

from decimal import Decimal
import json
import os

import pytest

from boxer_company.memory.contracts import canonical_digest
from boxer_company_adapter_slack.memory_history import (
    HistoryImportError, HistoryRateLimited, IMPORT_CHANNEL, SlackHistoryCollector, period,
)


WINDOW = period("2026-10-01T00:00:00+09:00", "2026-10-17T12:00:00+09:00")


def stamp(offset):
    return f"{int(Decimal(WINDOW['oldestTs'])) + offset}.000001"


def human(offset, user="U1", text="앞으로 결론부터 짧게 답해줘", **extra):
    return {"ts": stamp(offset), "user": user, "text": text, **extra}


class Slack:
    def __init__(self, history=None, replies=None):
        self.history = history or {"": {"ok": True, "messages": []}}
        self.replies = replies or {}
        self.calls = []
        self.links = []

    def conversations_history(self, **kwargs):
        self.calls.append(("history", kwargs))
        return self.history[kwargs.get("cursor") or ""]

    def conversations_replies(self, **kwargs):
        self.calls.append(("replies", kwargs))
        result = self.replies[(kwargs["ts"], kwargs.get("cursor") or "")]
        if isinstance(result, Exception):
            raise result
        return result

    def chat_getPermalink(self, *, channel, message_ts):
        self.links.append(message_ts)
        return {"ok": True, "permalink": f"https://unit.slack.com/archives/{channel}/p{message_ts.replace('.', '')}"}


def collector(tmp_path, slack, *, window=WINDOW, all_history=True, allowed=None):
    return SlackHistoryCollector(slack, state_path=tmp_path.resolve() / "state.json",
        tenant_id="T1", author_ids={"U1", "U2"}, is_allowed=allowed or (lambda tenant, actor: tenant == "T1" and actor in {"U1", "U2"}),
        window=window, all_history_roots=all_history)


def test_metadata_scan_has_no_body_and_resumes_fixed_period_for_old_roots(tmp_path):
    # 기간 이전 root는 timestamp만 보관하고 10월 댓글을 수집 대상으로 남긴다.
    slack = Slack(history={
        "": {"ok": True, "messages": [human(1, text="본문은 메타에 없어야 해")],
             "response_metadata": {"next_cursor": "second"}},
        "second": {"ok": True, "messages": [human(-100, text="오래된 비공개 본문", reply_count=3, latest_reply=stamp(2))]},
    })
    first = collector(tmp_path, slack)
    assert not first.scan(max_pages=1)["historyDone"]
    resumed = collector(tmp_path, slack, window=None)
    result = resumed.scan(max_pages=1)
    assert result["historyDone"] and result["candidateThreads"] == 2
    persisted = first.path.read_text()
    assert "본문" not in persisted and '"text"' not in persisted
    assert slack.calls[1][1]["cursor"] == "second"
    assert all(call[1]["limit"] == 999 for call in slack.calls)
    assert all(call[1]["oldest"] == "0" for call in slack.calls)
    assert result["period"] == WINDOW
    with pytest.raises(HistoryImportError, match="resume_scope_changed"):
        collector(tmp_path, slack, window=period("2026-10-01", "2026-10-18"))


def test_collection_keeps_human_authorship_period_and_deduplicates_pages(tmp_path):
    old = human(-100, text="9월 글", reply_count=4, latest_reply=stamp(3))
    slack = Slack(history={"": {"ok": True, "messages": [old]}}, replies={
        (stamp(-100), ""): {"ok": True, "messages": [old, human(1), human(2, "U2", "나는 검토를 맡을게")],
                              "response_metadata": {"next_cursor": "next"}},
        (stamp(-100), "next"): {"ok": True, "messages": [human(2, "U2", "나는 검토를 맡을게"),
            human(3, text="확인 시간은 아직 미정이야"), {**human(4), "ts": "1999999999.000001"}]},
    })
    first = collector(tmp_path, slack)
    first.scan()
    first.collect(max_pages=1)
    resumed = collector(tmp_path, slack, window=None)
    result = resumed.collect(max_pages=1)
    output = tmp_path.resolve() / "input.json"
    resumed.export(output)
    document = json.loads(output.read_text())
    sources = document["threads"][0]["chunks"][0]["sources"]
    assert result["sourceCount"] == 3
    assert [(source["messageTs"], source["authorId"]) for source in sources] == [(stamp(1), "U1"), (stamp(2), "U2"), (stamp(3), "U1")]
    assert document["threads"][0]["threadTs"] == stamp(-100)
    assert document["coverage"] == "complete"
    assert document["inputDigest"] == canonical_digest(document["threads"])
    assert "9월 글" not in output.read_text()
    assert os.stat(output).st_mode & 0o777 == 0o600
    assert os.stat(first.path).st_mode & 0o777 == 0o600
    resumed.export(output)


def test_read_failure_restarts_same_page_without_duplicate_sources(tmp_path):
    slack = Slack(history={"": {"ok": True, "messages": [human(1)]}}, replies={(stamp(1), ""): TimeoutError("not logged")})
    first = collector(tmp_path, slack)
    first.scan()
    with pytest.raises(TimeoutError):
        first.collect()
    slack.replies[(stamp(1), "")] = {"ok": True, "messages": [human(1), human(1)]}
    result = collector(tmp_path, slack, window=None).collect()
    assert result["sourceCount"] == 1 and result["replyPages"] == 1


def test_sensitive_message_and_uncertain_followups_are_not_exported(tmp_path):
    # 제외 구간 이후 간접 발언을 새로운 선호로 오인하지 않도록 보수적으로 끊는다.
    slack = Slack(history={"": {"ok": True, "messages": [human(1)]}}, replies={
        (stamp(1), ""): {"ok": True, "messages": [human(1), human(2, text="password: secret"), human(3, text="응 계속 그렇게 해줘")]},
    })
    result = collector(tmp_path, slack)
    result.scan()
    summary = result.collect()
    result.export(tmp_path.resolve() / "input.json")
    raw = (tmp_path / "input.json").read_text()
    assert "secret" not in raw and "그렇게" not in raw
    assert summary["excludedMessages"] == 2 and summary["sourceCount"] == 1
    assert json.loads(raw)["coverage"] == "partial"


def test_long_thread_chunks_and_membership_recheck(tmp_path):
    messages = [human(index + 1, text=f"항목 {index}: " + "설명 " * 100) for index in range(120)]
    slack = Slack(history={"": {"ok": True, "messages": [messages[0]]}}, replies={(stamp(1), ""): {"ok": True, "messages": messages}})
    allowed = [True]
    result = collector(tmp_path, slack, allowed=lambda *_: allowed[0])
    result.scan()
    result.collect()
    allowed[0] = False
    with pytest.raises(HistoryImportError, match="membership_changed"):
        result.export(tmp_path.resolve() / "input.json")
    allowed[0] = True
    result.export(tmp_path.resolve() / "input.json")
    chunks = json.loads((tmp_path / "input.json").read_text())["threads"][0]["chunks"]
    assert len(chunks) >= 3
    assert sum(len(chunk["sources"]) for chunk in chunks) == 120
    assert all(len(chunk["sources"]) <= 100 and sum(len(source["text"]) for source in chunk["sources"]) <= 16000 for chunk in chunks)
    assert all(chunk["chunkDigest"] == canonical_digest(chunk["sources"]) for chunk in chunks)


def test_changed_duplicate_excludes_both_revisions(tmp_path):
    slack = Slack(history={"": {"ok": True, "messages": [human(1)]}}, replies={
        (stamp(1), ""): {"ok": True, "messages": [human(1)], "response_metadata": {"next_cursor": "next"}},
        (stamp(1), "next"): {"ok": True, "messages": [human(1, text="수정된 다른 내용", edited={"ts": stamp(3)})]},
    })
    result = collector(tmp_path, slack)
    result.scan()
    assert result.collect()["sourceCount"] == 0
    result.export(tmp_path.resolve() / "input.json")
    document = json.loads((tmp_path / "input.json").read_text())
    assert document["coverage"] == "partial" and document["threads"] == []


def test_incomplete_scan_and_unsafe_paths_fail_closed(tmp_path):
    slack = Slack(history={"": {"ok": True, "messages": [human(1)], "has_more": True}})
    result = collector(tmp_path, slack)
    with pytest.raises(HistoryImportError, match="pagination_incomplete"):
        result.scan()
    with pytest.raises(HistoryImportError, match="metadata_scan_incomplete"):
        result.collect()
    os.chmod(result.path, 0o644)
    with pytest.raises(HistoryImportError, match="unsafe_file"):
        result.scan()


def test_month_root_scan_explicitly_marks_missing_older_threads(tmp_path):
    result = collector(tmp_path, Slack(), all_history=False)
    result.scan()
    result.collect()
    result.export(tmp_path.resolve() / "input.json")
    document = json.loads((tmp_path / "input.json").read_text())
    assert document["coverageNotes"] == ["older_thread_roots_not_scanned"]
    assert document["channelId"] == IMPORT_CHANNEL


def test_rate_limit_is_checkpointed_and_does_not_retry_before_server_delay(tmp_path):
    class Response(dict):
        status_code = 429
        headers = {"Retry-After": "120"}
    class Limited(Exception):
        response = Response(error="ratelimited")
    slack = Slack()
    calls = []
    def limited(**kwargs):
        calls.append(kwargs)
        raise Limited()
    slack.conversations_history = limited
    result = collector(tmp_path, slack)
    with pytest.raises(HistoryRateLimited) as caught:
        result.scan()
    assert caught.value.retry_after == 120
    assert json.loads(result.path.read_text())["retryNotBefore"] > 0
    with pytest.raises(HistoryRateLimited):
        collector(tmp_path, slack, window=None).scan()
    assert len(calls) == 1


def test_private_channel_kind_and_symlink_protection(tmp_path):
    result = SlackHistoryCollector(Slack(), state_path=tmp_path.resolve() / "state.json",
        tenant_id="T1", author_ids={"U1"}, is_allowed=lambda *_: True,
        window=WINDOW, all_history_roots=True, channel_kind="private_channel")
    result.scan()
    result.collect()
    target = tmp_path.resolve() / "input.json"
    result.export(target)
    assert json.loads(target.read_text())["channelKind"] == "private_channel"
    link = tmp_path.resolve() / "link.json"
    link.symlink_to(target)
    with pytest.raises(HistoryImportError, match="unsafe_path"):
        result.export(link)


def test_deleted_thread_and_post_cutoff_edit_are_explicitly_partial(tmp_path):
    slack = Slack(history={"": {"ok": True, "messages": [human(1), human(2)]}}, replies={
        (stamp(1), ""): {"ok": False, "error": "thread_not_found"},
        (stamp(2), ""): {"ok": True, "messages": [human(2, edited={"ts": "1999999999.000001"})]},
    })
    result = collector(tmp_path, slack)
    result.scan()
    assert result.collect()["completedThreads"] == 2
    result.export(tmp_path.resolve() / "input.json")
    document = json.loads((tmp_path / "input.json").read_text())
    assert document["threads"] == []
    assert document["coverage"] == "partial"
    assert "unavailable_threads_excluded" in document["coverageNotes"]
