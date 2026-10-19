"""실시간 원문 수집의 private 경계·작성자·시간·부분 조회 계약을 SDK mock으로 검증한다."""

from decimal import Decimal

import pytest

from boxer_company.memory.contracts import source_revision
from boxer_company_adapter_slack import private_live_retrieval
from boxer_company_adapter_slack.private_live_retrieval import collect_private_live_sources


TENANT = "T19PA3EPL"
HUB = "C068FVD5V7Y"


def stamp(offset):
    return f"{1792100000 + offset}.000001"


def private(channel="CONE", **extra):
    return {"id": channel, "is_private": True, "is_member": True, "context_team_id": TENANT, **extra}


def human(offset, user="UDD", text="화요일에 회의하자", **extra):
    return {"ts": stamp(offset), "user": user, "text": text, **extra}


def page(field, values, cursor="", **extra):
    return {"ok": True, field: values, "response_metadata": {"next_cursor": cursor}, **extra}


class Slack:
    def __init__(self, *, channels=None, history=None, replies=None, latest_replies=None, links=None, elapsed=None, delay=0, infos=None):
        self.channels = channels if channels is not None else {"": page("channels", [private()])}
        self.history = history or {}
        self.replies = replies or {}
        self.latest_replies = latest_replies or {}
        self.links = links or {}
        self.calls = []
        self.timeouts = []
        self.elapsed, self.delay = elapsed, delay
        self.infos = infos or {}

    def factory(self, timeout):
        self.timeouts.append(timeout)
        return self

    def take(self, method, kwargs, value):
        self.calls.append((method, kwargs))
        if self.elapsed is not None:
            self.elapsed[0] += self.delay
        if isinstance(value, Exception):
            raise value
        return value

    def conversations_list(self, **kwargs):
        return self.take("list", kwargs, self.channels[kwargs.get("cursor") or ""])

    def conversations_info(self, **kwargs):
        channel = kwargs["channel"]
        return self.take("info", kwargs, self.infos.get(channel, {"ok": True, "channel": private(channel)}))

    def conversations_history(self, **kwargs):
        key = kwargs["channel"], kwargs.get("cursor") or ""
        return self.take("history", kwargs, self.history.get(key, page("messages", [])))

    def conversations_replies(self, **kwargs):
        key = kwargs["channel"], kwargs["ts"], kwargs.get("cursor") or ""
        value = self.replies.get(key, page("messages", []))
        # 날짜 범위 oldest와 최신 댓글 한 건을 읽는 exact probe를 구분한다.
        if kwargs.get("oldest") == kwargs.get("latest") and kwargs.get("oldest") and not isinstance(value, Exception):
            messages = [message for (channel, thread, _), response in self.replies.items()
                        if channel == key[0] and thread == key[1] and not isinstance(response, Exception)
                        for message in response.get("messages", []) if message.get("ts") == kwargs["latest"]]
            value = self.latest_replies.get(key[:2], page("messages", messages))
        return self.take("replies", kwargs, value)

    def chat_getPermalink(self, **kwargs):
        key = kwargs["channel"], kwargs["message_ts"]
        link = f"https://unit.slack.com/archives/{key[0]}/p{key[1].replace('.', '')}"
        return self.take("link", kwargs, self.links.get(key, {"ok": True, "permalink": link}))


def collect(slack, **overrides):
    values = dict(client_factory=slack.factory, tenant_id=TENANT, destination_channel_id=HUB,
                  actor_id="UACTOR", question="디디가 최근에 한 말은?", author_ids=["UDD"],
                  before_ts=stamp(1000), is_allowed=lambda tenant, actor: tenant == TENANT and actor in {"UACTOR", "UDD", "UOTHER"})
    values.update(overrides)
    return collect_private_live_sources(**values)


def test_ordinary_private_channel_reads_only_itself_even_when_other_channels_are_available():
    # 기존 대화 채널 질문은 목록을 탐색하지 않고 해당 채널의 마루 발언만 읽는다.
    slack = Slack(channels={"": page("channels", [private("COTHER"), private("CONE")])}, history={
        ("CONE", ""): page("messages", [human(10, user="UMARU", text="안내문 문구 검토를 마쳤어")]),
        ("COTHER", ""): page("messages", [human(11, user="UMARU", text="다른 채널의 이야기")]),
    })
    result = collect(slack, destination_channel_id="CONE", destination_channel_kind="private_channel",
        question="오늘 마루가 한 말 요약해줘", author_ids=["UMARU"],
        is_allowed=lambda tenant, actor: tenant == TENANT and actor in {"UACTOR", "UMARU"})
    assert result["coverage"] == "complete" and result["channelCount"] == 1
    assert len(result["sources"]) == 1 and result["sources"][0]["authorId"] == "UMARU"
    assert result["sources"][0]["channelId"] == "CONE"
    assert [method for method, _ in slack.calls] == ["info", "history", "link"]
    assert all(kwargs.get("channel") == "CONE" for _, kwargs in slack.calls)


@pytest.mark.parametrize("kind", [None, "channel", "dm", "group_dm", "private_channel ", True])
def test_ordinary_live_query_requires_explicit_private_channel_kind(kind):
    slack = Slack()
    result = collect(slack, destination_channel_id="CONE", destination_channel_kind=kind)
    assert result == {"sources": [], "coverage": "unavailable", "channelCount": 0}
    assert slack.calls == []


@pytest.mark.parametrize("info", [
    {"ok": True, "channel": private("COTHER")},
    {"ok": True, "channel": private(is_private=False)},
    {"ok": True, "channel": private(is_member=False)},
    {"ok": True, "channel": private(is_im=True)},
    {"ok": True, "channel": private(is_mpim=True)},
    {"ok": True, "channel": private(is_ext_shared=True)},
    {"ok": True, "channel": private(context_team_id="TOTHER")},
    {"ok": True, "channel": private(shared_team_ids=["TOTHER"])},
    {"ok": True, "channel": private(shared_team_ids="T19PA3EPL")},
    {"ok": True, "channel": {"id": "CONE", "is_private": True}},
    {"ok": False, "error": "channel_not_found"},
    {"ok": True},
])
def test_ordinary_channel_info_must_confirm_private_membership_and_workspace(info):
    # 요청 metadata를 private으로 주장해도 SDK가 원채널을 확인하지 못하면 history를 읽지 않는다.
    slack = Slack(infos={"CONE": info})
    result = collect(slack, destination_channel_id="CONE", destination_channel_kind="private_channel")
    assert result == {"sources": [], "coverage": "unavailable", "channelCount": 0}
    assert [method for method, _ in slack.calls] == ["info"]


def test_ordinary_channel_does_not_fall_back_to_list_after_info_failure():
    slack = Slack(infos={"CONE": RuntimeError("do not print SDK payload")})
    result = collect(slack, destination_channel_id="CONE", destination_channel_kind="private_channel")
    assert result == {"sources": [], "coverage": "unavailable", "channelCount": 0}
    assert [method for method, _ in slack.calls] == ["info"]


def dated_message(ts, **extra):
    return {"ts": ts, "user": "UDD", "text": "안내문 검토를 마쳤어", **extra}


@pytest.mark.parametrize("question,expected,reply_start,reply_end", [
    ("디디가 오늘 한 말 요약", ["1792350000.000001", "1792335600.000000"],
     "1792335600.000000", "1792350000.000001"),
    ("디디가 어제 한 말 요약", ["1792335599.999999", "1792249200.000000"],
     "1792249200.000000", "1792335600.000000"),
])
def test_relative_day_filters_roots_and_replies_without_hiding_old_parent(question, expected, reply_start, reply_end):
    # 오래된 부모와 다음날 최신 댓글이 있어도 요청한 하루의 댓글을 별도로 읽는다.
    parent_ts, latest_ts = "1792200000.000001", "1792350000.000001"
    root = dated_message(parent_ts, reply_count=99, latest_reply=latest_ts)
    reply_stamps = ["1792249199.999999", "1792249200.000000", "1792335599.999999",
                    "1792335600.000000", latest_ts, "1792350000.000002"]
    slack = Slack(history={("CONE", ""): page("messages", [root])}, replies={
        ("CONE", parent_ts, ""): page("messages", [root, *[
            dated_message(ts, thread_ts=parent_ts) for ts in reply_stamps]]),
    })
    result = collect(slack, question=question, before_ts=latest_ts)
    assert [source["messageTs"] for source in result["sources"]] == expected
    assert all(source["threadTs"] == parent_ts for source in result["sources"])
    assert result["coverage"] == "complete"
    histories = [kwargs for method, kwargs in slack.calls if method == "history"]
    assert len(histories) == 1 and "oldest" not in histories[0]
    assert histories[0]["latest"] == reply_end
    full_replies = [kwargs for method, kwargs in slack.calls
                    if method == "replies" and kwargs["oldest"] != kwargs["latest"]]
    assert len(full_replies) == 1
    assert full_replies[0]["oldest"] == reply_start and full_replies[0]["latest"] == reply_end
    assert all(kwargs["message_ts"] in expected for method, kwargs in slack.calls if method == "link")


def test_today_skips_thread_whose_verified_latest_hint_is_before_midnight():
    root = dated_message("1792200000.000001", reply_count=99, latest_reply="1792335599.999999")
    slack = Slack(history={("CONE", ""): page("messages", [root])})
    result = collect(slack, question="디디가 오늘 한 말 요약", before_ts="1792350000.000001")
    assert result == {"sources": [], "coverage": "complete", "channelCount": 1}
    assert not any(method in {"replies", "link"} for method, _ in slack.calls)


def test_date_window_reply_pagination_remains_partial_when_budget_does_not_finish():
    root_ts, first_ts, latest_ts = "1792200000.000001", "1792249200.000001", "1792350000.000001"
    root = dated_message(root_ts, reply_count=99, latest_reply=latest_ts)
    slack = Slack(history={("CONE", ""): page("messages", [root])}, replies={
        ("CONE", root_ts, ""): page("messages", [dated_message(first_ts, thread_ts=root_ts)], "one"),
        ("CONE", root_ts, "one"): page("messages", [], "two"),
    })
    result = collect(slack, question="디디가 어제 한 말 요약", before_ts=latest_ts)
    assert result["coverage"] == "partial" and result["sources"][0]["messageTs"] == first_ts
    assert len([method for method, _ in slack.calls if method == "replies"]) == 2


@pytest.mark.parametrize("overrides", [
    {"tenant_id": "TOTHER"}, {"destination_channel_id": "COTHER"}, {"actor_id": ""},
    {"is_allowed": lambda *_: False}, {"before_ts": "not-a-timestamp"},
    {"budget_seconds": 0}, {"budget_seconds": float("inf")},
])
def test_destination_tenant_actor_and_cutoff_fail_closed_before_sdk(overrides):
    slack = Slack()
    assert collect(slack, **overrides) == {"sources": [], "coverage": "unavailable", "channelCount": 0}
    assert slack.calls == slack.timeouts == []


def test_private_membership_and_workspace_are_required_for_every_channel():
    channels = [private(), private("CPUBLIC", is_private=False), private("CNOACCESS", is_member=False),
                private("D123", is_im=True), private("GMPIM", is_mpim=True),
                private("COTHER", context_team_id="TOTHER"),
                private("CSHARED", is_ext_shared=True), private("CFOREIGN", shared_team_ids=["TOTHER"])]
    slack = Slack(channels={"": page("channels", channels)}, history={("CONE", ""): page("messages", [human(1)])})
    result = collect(slack)
    assert [source["channelId"] for source in result["sources"]] == ["CONE"]
    assert [kwargs["channel"] for method, kwargs in slack.calls if method == "history"] == ["CONE"]
    listed = slack.calls[0][1]
    assert listed["types"] == "private_channel" and listed["exclude_archived"] is False
    assert listed["limit"] == 200 and listed["team_id"] == TENANT


def test_archived_private_channel_remains_in_explicit_all_private_scope():
    slack = Slack(channels={"": page("channels", [private(is_archived=True)])},
                  history={("CONE", ""): page("messages", [human(1)])})
    result = collect(slack)
    assert result["coverage"] == "complete" and result["channelCount"] == 1
    assert [row["messageTs"] for row in result["sources"]] == [stamp(1)]


def test_author_filter_uses_actual_writer_without_requiring_name_in_body():
    messages = [human(2, text="화요일로 옮길까?"), human(1, "UOTHER", "디디 이야기를 했어")]
    slack = Slack(history={("CONE", ""): page("messages", messages)})
    result = collect(slack)
    assert result["coverage"] == "complete"
    assert result["channelCount"] == 1
    assert [(row["authorId"], row["text"]) for row in result["sources"]] == [("UDD", "화요일로 옮길까?")]
    source = result["sources"][0]
    assert source["channelKind"] == "private_channel" and source["threadTs"] == source["messageTs"]
    assert source["revisionHash"] == source_revision(source["text"], "")


def test_unnamed_query_uses_explicit_topic_after_question_words_and_particles():
    slack = Slack(history={("CONE", ""): page("messages", [
        human(2, text="회의 장소는 아직 미정이야"), human(1, text="새로 나온 도구는 좋아")])})
    result = collect(slack, author_ids=[], question="최근에 회의에서 나온 대화 알려줘")
    assert [row["text"] for row in result["sources"]] == ["회의 장소는 아직 미정이야"]


@pytest.mark.parametrize("question", [
    "최근 대화는?", "비공개 채널에서 사람들 최근 소식 뭐있어?", "모든 비공개 채널 대화 전부 보여줘",
])
def test_recent_conversation_without_topic_or_author_reads_all_allowed_humans(question):
    slack = Slack(history={("CONE", ""): page("messages", [human(2), human(1, "UOTHER", "수요일에 보자")])})
    result = collect(slack, author_ids=[], question=question)
    assert [row["authorId"] for row in result["sources"]] == ["UDD", "UOTHER"]
    assert result["coverage"] == "complete"


@pytest.mark.parametrize("question", ["안녕", "대화하자", "보안에 대해 알려줘", "오늘 날씨 어때?"])
def test_ordinary_freeform_does_not_start_private_history_calls(question):
    slack = Slack()
    assert collect(slack, author_ids=[], question=question)["coverage"] == "unavailable"
    assert slack.calls == slack.timeouts == []


def test_sensitive_attached_bot_and_denied_author_sources_are_never_returned():
    messages = [human(1), human(2, text="password: never-return-this"), human(3, bot_id="BBOT"),
                human(4, files=[{"id": "F1"}]), human(5, attachments=[{"text": "hidden"}]),
                human(6, "UDENIED"), human(7, team="TOTHER"), human(8, is_truncated=True),
                human(9, text="장비 로그 조회해줘")]
    slack = Slack(history={("CONE", ""): page("messages", messages)})
    result = collect(slack, author_ids=["UDD", "UDENIED"])
    assert [row["messageTs"] for row in result["sources"]] == [stamp(1)]
    assert result["coverage"] == "partial"
    assert "never-return-this" not in str(result) and "hidden" not in str(result)
    assert [kwargs["message_ts"] for method, kwargs in slack.calls if method == "link"] == [stamp(1)]


def test_thread_replies_are_read_and_sorted_with_root_deduplication():
    root = human(1, reply_count=2, latest_reply=stamp(8))
    slack = Slack(history={("CONE", ""): page("messages", [root, human(5)])}, replies={
        ("CONE", stamp(1), ""): page("messages", [root, human(3, thread_ts=stamp(1))], "next"),
        ("CONE", stamp(1), "next"): page("messages", [human(3, thread_ts=stamp(1)), human(8, thread_ts=stamp(1))]),
    })
    result = collect(slack)
    assert result["coverage"] == "complete"
    assert [row["messageTs"] for row in result["sources"]] == [stamp(8), stamp(5), stamp(3), stamp(1)]
    assert result["sources"][0]["threadTs"] == stamp(1)
    assert len([call for call in slack.calls if call[0] == "link"]) == 4
    assert all(kwargs["latest"] == kwargs.get("oldest", stamp(1000)) and kwargs["inclusive"] is True
               for method, kwargs in slack.calls if method in {"history", "replies"})


def test_unconfirmed_latest_reply_is_partial_even_without_cursor():
    root = human(1, reply_count=1, latest_reply=stamp(8))
    slack = Slack(history={("CONE", ""): page("messages", [root])},
                  replies={("CONE", stamp(1), ""): page("messages", [root])})
    assert collect(slack)["coverage"] == "partial"


def test_missing_latest_hint_does_not_hide_unread_reply_count():
    root = human(1, reply_count=2)
    slack = Slack(history={("CONE", ""): page("messages", [root])},
                  replies={("CONE", stamp(1), ""): page("messages", [root, human(2, thread_ts=stamp(1))])})
    assert collect(slack)["coverage"] == "partial"


def test_thread_pages_are_fairly_distributed_after_each_channels_latest_history():
    root_one = human(1, reply_count=2, latest_reply=stamp(7))
    root_two = human(2, reply_count=1, latest_reply=stamp(8))
    slack = Slack(channels={"": page("channels", [private(), private("CTWO")])}, history={
        ("CONE", ""): page("messages", [root_one]), ("CTWO", ""): page("messages", [root_two]),
    }, replies={
        ("CONE", stamp(1), ""): page("messages", [root_one, human(3, thread_ts=stamp(1))], "next"),
        ("CONE", stamp(1), "next"): page("messages", [human(7, thread_ts=stamp(1))]),
        ("CTWO", stamp(2), ""): page("messages", [root_two, human(8, thread_ts=stamp(2))]),
    })
    result = collect(slack)
    calls = [(method, kwargs.get("channel"), kwargs.get("cursor")) for method, kwargs in slack.calls if method in {"history", "replies"}]
    assert calls == [("history", "CONE", None), ("history", "CTWO", None),
                     ("replies", "CONE", None), ("replies", "CTWO", None),
                     ("replies", "CONE", None), ("replies", "CTWO", None), ("replies", "CONE", "next")]
    assert result["coverage"] == "complete"


def test_latest_reply_on_old_parent_is_prioritized_under_small_read_budget(monkeypatch):
    # 부모 글이 오래됐어도 방금 달린 대상 작성자 댓글을 새 부모의 오래된 댓글보다 먼저 읽는다.
    monkeypatch.setattr(private_live_retrieval, "_MAX_READ_CALLS", 3)
    old_parent = human(1, "UOTHER", reply_count=1, latest_reply=stamp(999))
    new_parent = human(100, "UOTHER", reply_count=1, latest_reply=stamp(101))
    elapsed = [0.0]
    slack = Slack(history={("CONE", ""): page("messages", [new_parent, old_parent], "older")}, replies={
        ("CONE", stamp(1), ""): page("messages", [old_parent, human(999, thread_ts=stamp(1))]),
        ("CONE", stamp(100), ""): page("messages", [new_parent, human(101, thread_ts=stamp(100))]),
    }, elapsed=elapsed, delay=.15)
    result = collect(slack, clock=lambda: elapsed[0], budget_seconds=1)
    assert [kwargs["ts"] for method, kwargs in slack.calls if method == "replies"] == [stamp(1)]
    assert [row["messageTs"] for row in result["sources"]] == [stamp(999)]
    assert result["coverage"] == "partial" and elapsed[0] < 1


def test_latest_reply_beyond_first_two_thread_pages_is_read_by_exact_probe(monkeypatch):
    monkeypatch.setattr(private_live_retrieval, "_MAX_READ_CALLS", 3)
    root = human(1, "UOTHER", reply_count=300, latest_reply=stamp(999))
    slack = Slack(history={("CONE", ""): page("messages", [root])}, replies={
        ("CONE", stamp(1), ""): page("messages", [root, *[human(n, "UOTHER", thread_ts=stamp(1)) for n in range(2, 100)]], "second"),
    }, latest_replies={("CONE", stamp(1)): page("messages", [human(999, thread_ts=stamp(1))])})
    result = collect(slack)
    reply_call = next(kwargs for method, kwargs in slack.calls if method == "replies")
    assert reply_call["oldest"] == reply_call["latest"] == stamp(999)
    assert [row["messageTs"] for row in result["sources"]] == [stamp(999)]
    assert result["coverage"] == "partial"


def test_future_messages_and_post_cutoff_edits_cannot_be_presented_as_past():
    messages = [human(1001), human(4, edited={"ts": stamp(1001)}), human(3, edited={"ts": stamp(5)}), human(1000)]
    slack = Slack(history={("CONE", ""): page("messages", messages)})
    result = collect(slack)
    assert [row["messageTs"] for row in result["sources"]] == [stamp(1000), stamp(3)]
    assert result["sources"][1]["editedAt"] == stamp(5)
    assert result["sources"][1]["revisionHash"] == source_revision(messages[2]["text"], stamp(5))


def test_conflicting_duplicate_revisions_exclude_both_instead_of_returning_stale_body():
    slack = Slack(history={
        ("CONE", ""): page("messages", [human(1)], "next"),
        ("CONE", "next"): page("messages", [human(1, text="password: changed-secret")]),
    })
    result = collect(slack)
    assert result["sources"] == [] and result["coverage"] == "partial"
    assert not any(method == "link" for method, _ in slack.calls)


def test_channel_pagination_and_history_fairness_keep_later_channels_visible():
    slack = Slack(channels={
        "": page("channels", [private()], "channels-two"),
        "channels-two": page("channels", [private("CTWO")]),
    }, history={
        ("CONE", ""): page("messages", [human(2)], "older"),
        ("CONE", "older"): page("messages", [human(1)]),
        ("CTWO", ""): page("messages", [human(3)]),
    })
    result = collect(slack)
    histories = [(kwargs["channel"], kwargs["cursor"]) for method, kwargs in slack.calls if method == "history"]
    assert histories == [("CONE", None), ("CTWO", None), ("CONE", "older")]
    assert result["coverage"] == "complete" and result["channelCount"] == 2
    assert [row["messageTs"] for row in result["sources"]] == [stamp(3), stamp(2), stamp(1)]


@pytest.mark.parametrize("method", ["list", "history", "replies"])
def test_api_errors_and_rate_limits_are_partial_without_retry_or_exception_text(method):
    failure = RuntimeError("ratelimited secret must not be returned")
    root = human(1, reply_count=1, latest_reply=stamp(2))
    slack = Slack(channels={"": failure if method == "list" else page("channels", [private()])},
                  history={("CONE", ""): failure if method == "history" else page("messages", [root])},
                  replies={("CONE", stamp(1), ""): failure})
    result = collect(slack)
    assert result["coverage"] == "partial" and "secret" not in str(result)
    assert len([call for call in slack.calls if call[0] == method]) == 1


def test_returned_rate_limit_also_stops_reading_without_retries():
    slack = Slack(channels={"": {"ok": False, "error": "ratelimited"}})
    result = collect(slack)
    assert result == {"sources": [], "coverage": "partial", "channelCount": 0}
    assert len(slack.calls) == 1


def test_deadline_prevents_additional_sdk_calls_and_passes_bounded_timeouts():
    elapsed = [0.0]
    slack = Slack(elapsed=elapsed, delay=.2)
    result = collect(slack, clock=lambda: elapsed[0], budget_seconds=.2)
    assert result["coverage"] == "partial" and result["sources"] == []
    assert [method for method, _ in slack.calls] == ["list"]
    assert all(0 < value <= .2 for value in slack.timeouts)


def test_total_call_channel_and_source_limits_are_explicitly_partial():
    channels = [private(f"C{index:03}") for index in range(200)]
    slack = Slack(channels={"": page("channels", channels)}, history={
        (channel["id"], ""): page("messages", [human(index + 1)]) for index, channel in enumerate(channels)
    })
    result = collect(slack)
    assert len(slack.calls) <= 32 and len(result["sources"]) == 8
    assert result["channelCount"] > 8 and result["coverage"] == "partial"
    assert len([call for call in slack.calls if call[0] == "link"]) == 8
    assert [Decimal(row["messageTs"]) for row in result["sources"]] == sorted(
        [Decimal(row["messageTs"]) for row in result["sources"]], reverse=True)


@pytest.mark.parametrize("stream", ["channels", "history", "replies"])
def test_page_limits_and_repeated_cursors_cannot_claim_complete(stream):
    root = human(1, reply_count=1, latest_reply=stamp(2))
    channels = {"": page("channels", [private()], "same"), "same": page("channels", [], "same")}
    history = {("CONE", ""): page("messages", [root], "next"),
               ("CONE", "next"): page("messages", [], "more")}
    replies = {("CONE", stamp(1), ""): page("messages", [root], "same"),
               ("CONE", stamp(1), "same"): page("messages", [human(2, thread_ts=stamp(1))], "same")}
    slack = Slack(channels=channels if stream == "channels" else None,
                  history=history if stream == "history" else {("CONE", ""): page("messages", [root])},
                  replies=replies if stream == "replies" else {("CONE", stamp(1), ""): page("messages", [root, human(2, thread_ts=stamp(1))])})
    assert collect(slack)["coverage"] == "partial"
    assert len(slack.calls) < 32


def test_body_budget_never_truncates_original_text():
    slack = Slack(history={("CONE", ""): page("messages", [
        human(3, text="가" * 7000), human(2, text="나" * 2000), human(1, text="다" * 500)])})
    result = collect(slack)
    assert [len(row["text"]) for row in result["sources"]] == [7000, 500]
    assert sum(len(row["text"]) for row in result["sources"]) <= 8000
    assert result["coverage"] == "partial"


@pytest.mark.parametrize("link", [
    "https://evil.example/archives/CONE/p1792100001000001",
    "https://unit.slack.com/archives/COTHER/p1792100001000001",
    "https://unit.slack.com/archives/CONE/p1792100001000001?token=must-not-return",
    "https://unit.slack.com/archives/CONE/p1792100001000001?thread_ts=other",
])
def test_only_selected_sources_with_matching_safe_permalink_are_returned(link):
    slack = Slack(history={("CONE", ""): page("messages", [human(1)])},
                  links={("CONE", stamp(1)): {"ok": True, "permalink": link}})
    result = collect(slack)
    assert result["sources"] == [] and result["coverage"] == "partial"


def test_empty_fully_enumerated_private_history_is_complete_without_fake_sources():
    result = collect(Slack())
    assert result == {"sources": [], "coverage": "complete", "channelCount": 1}
