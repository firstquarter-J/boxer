"""지정된 비공개 허브에서만 최근 Slack 원문을 제한적으로 읽는다."""

from __future__ import annotations

from collections import deque
from decimal import Decimal
import re
import time
import unicodedata
from typing import Any, Callable, Mapping, Sequence
from urllib.parse import parse_qsl, urlsplit

from boxer_company._operation_routing_knowledge import TEAM_MEMBER_ALIASES_BY_NAME
from boxer_company.memory.access import is_private_memory_hub
from boxer_company.memory.contracts import source_revision
from boxer_company.memory.live_query import wants_private_live_sources
from boxer_company.memory.policy import is_allowed_conversation_source, is_safe_memory_text
from boxer_company.team_member_aliases import matches_team_member_alias
from boxer_company_adapter_slack.conversation_memory import _data, _safe_permalink


_TS = re.compile(r"^[0-9]{1,20}\.[0-9]{1,9}$")
_CHANNEL = re.compile(r"^[CG][A-Z0-9]+$")
_MAX_CALLS = 32
_MAX_READ_CALLS = 24
_MAX_CHANNELS = 200
_MAX_LIST_PAGES = 3
_MAX_HISTORY_PAGES = 2
_MAX_REPLY_PAGES = 2
_PAGE_SIZE = 100
_MAX_SOURCES = 8
_MAX_TEXT = 8_000
_MAX_CANDIDATES = 64
_ALIASES = tuple(alias for aliases in TEAM_MEMBER_ALIASES_BY_NAME.values() for alias in aliases)
_QUESTION_WORDS = frozenset({
    "최근", "최근에", "최근의", "요즘", "요새", "지난", "오늘", "어제", "이번", "대화", "기록",
    "내용", "말", "말은", "한말", "한말은", "발언", "이야기", "얘기", "어떤", "무엇", "뭐라고",
    "알려줘", "알려줄래", "보여줘", "찾아줘", "찾아봐", "검색", "검색해줘", "했어", "했던",
    "대한", "대해", "대해서", "관한", "관련", "관련한", "있는", "있어", "있나", "있니", "있는지",
    "나온", "했는지", "했나요", "해줘", "내역", "무슨", "어디", "알려", "찾아",
    "비공개", "채널", "비공개채널", "사람들", "전부", "모두", "모든", "소식", "뭐있어",
    "최근대화", "최근발언", "최근소식", "최근말", "어떤말", "무슨말",
})
_WORD_ENDINGS = ("에서는", "에게는", "한테는", "에서", "에는", "에도", "에게", "한테", "으로", "은", "는", "이", "가", "을", "를", "의", "에", "도", "만")


def _keywords(question: str) -> tuple[str, ...]:
    # 작성자 없는 질문은 상투어·사람 이름을 빼고 남은 명시 주제만 검색한다.
    text = re.sub(r"<@[^>]+>", " ", unicodedata.normalize("NFKC", question).casefold())
    result: list[str] = []
    for word in re.findall(r"[가-힣a-z0-9_]{2,}", text):
        if word in _QUESTION_WORDS or matches_team_member_alias(word, _ALIASES):
            continue
        for ending in _WORD_ENDINGS:
            if word.endswith(ending) and len(word) - len(ending) >= 2:
                word = word[:-len(ending)]
                break
        if word not in _QUESTION_WORDS and word not in result:
            result.append(word)
    return tuple(result[:6])


def _timestamp(value: Any) -> Decimal | None:
    return Decimal(value) if isinstance(value, str) and _TS.fullmatch(value) else None


class _Reader:
    def __init__(self, client_factory: Callable[[float], Any], *, tenant: str, authors: frozenset[str],
                 keywords: tuple[str, ...], cutoff: str, is_allowed: Callable[[str, str], bool],
                 clock: Callable[[], float], budget: float) -> None:
        self.factory, self.tenant, self.authors, self.keywords = client_factory, tenant, authors, keywords
        self.cutoff, self.cutoff_value, self.allowed, self.clock = cutoff, Decimal(cutoff), is_allowed, clock
        started = clock()
        self.deadline = started + budget
        # 링크 확인 시간과 호출 여유를 남겨 원문만 모으고 반환하지 못하는 경우를 줄인다.
        self.read_deadline = started + budget * .7
        self.calls = 0
        self.partial = False
        self.read_halted = False
        self.channels_read: set[str] = set()
        self.candidates: dict[tuple[str, str], dict[str, Any]] = {}
        self.seen: dict[tuple[str, str], tuple[str, str, str]] = {}
        self.conflicted: set[tuple[str, str]] = set()
        self.memberships: dict[str, bool] = {}

    def call(self, method: str, *, read: bool = True, **kwargs: Any) -> Mapping[str, Any] | None:
        deadline = self.read_deadline if read else self.deadline
        remaining = deadline - self.clock()
        if remaining <= 0 or self.calls >= (_MAX_READ_CALLS if read else _MAX_CALLS) or (read and self.read_halted):
            self.partial = True
            return None
        self.calls += 1
        try:
            client = self.factory(min(2.0, remaining))
            response = _data(getattr(client, method)(**kwargs))
            if response.get("ok") is not True:
                if response.get("error") in {"ratelimited", "rate_limited"}:
                    self.read_halted = True
                self.partial = True
                return None
            if response.get("warning") or _data(response.get("response_metadata")).get("warnings"):
                self.partial = True
            if self.clock() > deadline:
                self.partial = True
            return response
        except Exception:
            # 원문·자격증명이 포함될 수 있는 SDK 예외는 반환하거나 기록하지 않는다.
            self.partial = True
            self.read_halted = True
            return None

    def page(self, response: Mapping[str, Any], field: str, *, limit: int) -> tuple[list[Any], str]:
        values = response.get(field)
        if not isinstance(values, list):
            self.partial = True
            return [], ""
        cursor = _data(response.get("response_metadata")).get("next_cursor") or ""
        if not isinstance(cursor, str) or len(cursor) > 512:
            self.partial = True
            cursor = ""
        if response.get("has_more") and not cursor:
            self.partial = True
        if len(values) > limit:
            self.partial = True
        return values[:limit], cursor

    def channels(self) -> list[str]:
        channels: list[str] = []
        cursor, visited = "", set()
        for _ in range(_MAX_LIST_PAGES):
            response = self.call("conversations_list", types="private_channel", exclude_archived=False,
                                 limit=200, cursor=cursor or None, team_id=self.tenant)
            if response is None:
                break
            values, following = self.page(response, "channels", limit=200)
            for channel in values:
                if not isinstance(channel, Mapping):
                    self.partial = True
                    continue
                # Slack Connect나 다른 workspace의 metadata를 같은 회사 private로 승격하지 않는다.
                if (any(channel.get(key) and channel[key] != self.tenant for key in ("team_id", "context_team_id"))
                        or channel.get("is_ext_shared")
                        or not isinstance(channel.get("shared_team_ids", []), list)
                        or any(team != self.tenant for team in channel.get("shared_team_ids", []))):
                    self.partial = True
                    continue
                channel_id = channel.get("id")
                if (channel.get("is_private") is not True or channel.get("is_member") is not True
                        or channel.get("is_im") or channel.get("is_mpim")
                        or not isinstance(channel_id, str) or not _CHANNEL.fullmatch(channel_id)):
                    continue
                if channel_id not in channels:
                    if len(channels) >= _MAX_CHANNELS:
                        self.partial = True
                    else:
                        channels.append(channel_id)
            if not following:
                return channels
            if following in visited:
                self.partial = True
                return channels
            visited.add(following)
            cursor = following
        else:
            self.partial = True
        return channels

    def message(self, channel: str, value: Any, *, thread: str | None = None) -> None:
        if not isinstance(value, Mapping):
            self.partial = True
            return
        stamp = _timestamp(value.get("ts"))
        if stamp is None:
            self.partial = True
            return
        if stamp > self.cutoff_value:
            return
        if self.clock() >= self.read_deadline:
            self.partial = True
            return
        message_ts = value["ts"]
        thread_ts = value.get("thread_ts") or thread or message_ts
        thread_value = _timestamp(thread_ts)
        if thread_value is None or thread_value > stamp or (thread and thread_ts != thread):
            self.partial = True
            return
        key = (channel, message_ts)
        text, author = value.get("text"), value.get("user")
        edited = _data(value.get("edited")).get("ts") or ""
        if not isinstance(text, str) or not isinstance(author, str) or not author:
            self.partial = True
            return
        revision = source_revision(text, str(edited))
        identity = (author, revision, thread_ts)
        if key in self.seen and self.seen[key] != identity:
            self.conflicted.add(key)
            self.candidates.pop(key, None)
            self.partial = True
        self.seen[key] = identity
        if key in self.conflicted:
            return
        # 편집 뒤 원문을 과거 시점의 발언으로 제시하지 않고, 첨부/봇/민감 본문도 제외한다.
        if ((value.get("edited") and not edited)
                or (edited and (_timestamp(edited) is None or Decimal(edited) > self.cutoff_value))):
            self.partial = True
            return
        if (value.get("bot_id") or value.get("bot_profile")
                or value.get("subtype") not in {None, "", "thread_broadcast"}
                or value.get("files") or value.get("attachments")
                or value.get("is_truncated") or value.get("truncated")
                or any(value.get(field) and value[field] != self.tenant for field in ("team", "team_id"))):
            self.partial = True
            self.candidates.pop(key, None)
            return
        if self.authors and author not in self.authors:
            return
        if not self.authors and self.keywords:
            normalized = unicodedata.normalize("NFKC", text).casefold()
            if not any(
                re.search(r"(?<![a-z0-9_])" + re.escape(word) + r"(?![a-z0-9_])", normalized)
                if re.fullmatch(r"[a-z0-9_]+", word) else word in normalized
                for word in self.keywords
            ):
                return
        if author not in self.memberships:
            try:
                self.memberships[author] = bool(self.allowed(self.tenant, author))
            except Exception:
                self.memberships[author] = False
        if (not self.memberships[author] or not 1 <= len(text) <= _MAX_TEXT
                or not is_safe_memory_text(text) or not is_allowed_conversation_source(text)):
            self.partial = True
            self.candidates.pop(key, None)
            return
        self.candidates[key] = {
            "channelId": channel, "channelKind": "private_channel", "threadTs": thread_ts,
            "messageTs": message_ts, "revisionHash": revision, "authorId": author,
            "text": text, "editedAt": edited,
        }
        if len(self.candidates) > _MAX_CANDIDATES:
            oldest = min(self.candidates, key=lambda item: Decimal(item[1]))
            self.candidates.pop(oldest)
            self.partial = True

    def scan(self, channels: list[str]) -> None:
        queues: dict[str, deque] = {channel: deque() for channel in channels}
        roots: dict[tuple[str, str], dict[str, Any]] = {}

        def read(channel: str, kind: str, thread: str, cursor: str, pages: int, visited: set[str]) -> None:
            kwargs = {"channel": channel, "limit": _PAGE_SIZE, "latest": self.cutoff,
                      "inclusive": True, "cursor": cursor or None}
            if kind in {"replies", "latest"}:
                kwargs["ts"] = thread
            if kind == "latest":
                # 긴 thread의 첫 페이지 밖에 있는 최신 댓글도 읽기 예산 안에서 먼저 확인한다.
                latest = roots[(channel, thread)]["latest"]
                kwargs.update(oldest=latest, latest=latest)
            response = self.call("conversations_" + ("replies" if kind == "latest" else kind), **kwargs)
            if response is None:
                return
            if kind == "history":
                self.channels_read.add(channel)
            messages, following = self.page(response, "messages", limit=_PAGE_SIZE)
            for message in messages:
                self.message(channel, message, thread=thread or None)
                if not isinstance(message, Mapping):
                    continue
                stamp = _timestamp(message.get("ts"))
                if stamp is None or stamp > self.cutoff_value:
                    continue
                if kind in {"replies", "latest"}:
                    if (message.get("thread_ts") or thread) == thread and stamp >= Decimal(thread):
                        roots[(channel, thread)]["seen"].add(message["ts"])
                    continue
                count = message.get("reply_count", 0)
                if type(count) is not int:
                    self.partial = True
                    continue
                key = channel, message["ts"]
                if count > 0 and (message.get("thread_ts") or message["ts"]) == message["ts"] and key not in roots:
                    latest = message.get("latest_reply")
                    latest_value = _timestamp(latest)
                    if latest_value is not None and latest_value <= stamp:
                        self.partial = True
                        latest, latest_value = None, None
                    roots[key] = {"latest": latest, "count": count, "seen": set(), "done": False}
                    kind_next = "latest" if latest_value is not None and latest_value <= self.cutoff_value else "replies"
                    queues[channel].append((kind_next, message["ts"], "", 1, set()))
            if kind == "latest":
                # 좁은 조회는 전체 thread 확인을 대체하지 않는다. 후속 full 조회가 끝나야 done이다.
                if following or roots[(channel, thread)]["latest"] not in roots[(channel, thread)]["seen"]:
                    self.partial = True
                queues[channel].append(("replies", thread, "", 1, set()))
                return
            limit = _MAX_HISTORY_PAGES if kind == "history" else _MAX_REPLY_PAGES
            if following:
                if following in visited or pages >= limit:
                    self.partial = True
                else:
                    queues[channel].append((kind, thread, following, pages + 1, visited | {following}))
            elif kind == "replies":
                root = roots[(channel, thread)]
                root["done"] = True
                latest = _timestamp(root["latest"])
                # root의 최신 댓글 또는 댓글 수를 확인하지 못하면 전체 원문이라고 말하지 않는다.
                if ((latest is not None and latest <= self.cutoff_value and root["latest"] not in root["seen"])
                        or ((latest is None or latest <= self.cutoff_value)
                            and len(root["seen"] - {thread}) < root["count"])):
                    self.partial = True

        # 채널별 최신 history를 모두 우선한 뒤, 추가 history·댓글 페이지를 채널마다 한 번씩 배분한다.
        for channel in channels:
            read(channel, "history", "", "", 1, set())
        while any(queues.values()):
            if self.read_halted or self.calls >= _MAX_READ_CALLS or self.clock() >= self.read_deadline:
                self.partial = True
                break
            for channel in channels:
                if queues[channel]:
                    # 부모 작성일보다 최신 댓글 시각으로 댓글끼리 우선순위를 정한다.
                    # history 작업의 기존 위치는 유지해 긴 thread가 다음 history를 선점하지 않는다.
                    tasks = list(queues[channel])
                    replies = iter(sorted(
                        (task for task in tasks if task[0] in {"replies", "latest"}),
                        key=lambda task: (task[0] == "latest",
                                          _timestamp(roots[(channel, task[1])]["latest"]) or Decimal(task[1])),
                        reverse=True,
                    ))
                    queues[channel] = deque(next(replies) if task[0] in {"replies", "latest"} else task for task in tasks)
                    read(channel, *queues[channel].popleft())
        if any(not root["done"] for root in roots.values()):
            self.partial = True

    def result(self) -> dict[str, Any]:
        sources: list[dict[str, Any]] = []
        selected: list[dict[str, Any]] = []
        chars = 0
        for item in sorted(self.candidates.values(), key=lambda value: Decimal(value["messageTs"]), reverse=True):
            if len(selected) >= _MAX_SOURCES or chars + len(item["text"]) > _MAX_TEXT:
                self.partial = True
                continue
            selected.append(item)
            chars += len(item["text"])
        for item in selected:
            response = self.call("chat_getPermalink", read=False, channel=item["channelId"], message_ts=item["messageTs"])
            link = _safe_permalink(response.get("permalink"), item["channelId"], item["messageTs"]) if response else None
            try:
                parsed = urlsplit(link) if link else None
                pairs = parse_qsl(parsed.query, keep_blank_values=True) if parsed else []
                params = dict(pairs)
                valid = (parsed is not None and parsed.port in {None, 443} and len(params) == len(pairs)
                         and set(params) <= {"thread_ts", "cid"}
                         and params.get("thread_ts", item["threadTs"]) == item["threadTs"]
                         and params.get("cid", item["channelId"]) == item["channelId"])
            except ValueError:
                valid = False
            if not valid:
                self.partial = True
                continue
            sources.append({**item, "permalink": link})
        return {"sources": sources, "coverage": "partial" if self.partial else "complete",
                "channelCount": len(self.channels_read)}


def collect_private_live_sources(
    *, client_factory: Callable[[float], Any], tenant_id: str, destination_channel_id: str,
    actor_id: str, question: str, author_ids: Sequence[str], before_ts: str,
    is_allowed: Callable[[str, str], bool], clock: Callable[[], float] = time.monotonic,
    budget_seconds: float = 8.0,
) -> dict[str, Any]:
    """원문을 저장하지 않고 현재 읽을 수 있는 비공개 채널에서 bounded 근거만 반환한다."""
    unavailable = {"sources": [], "coverage": "unavailable", "channelCount": 0}
    if (not is_private_memory_hub(tenant_id, destination_channel_id) or not actor_id
            or _timestamp(before_ts) is None or not isinstance(question, str)
            or not is_safe_memory_text(question) or not 0 < budget_seconds <= 30):
        return unavailable
    try:
        if not is_allowed(tenant_id, actor_id):
            return unavailable
    except Exception:
        return unavailable
    authors = frozenset(value for value in author_ids if isinstance(value, str) and value)
    if not wants_private_live_sources(question, tuple(authors)):
        return unavailable
    keywords = () if authors else _keywords(question)
    reader = _Reader(client_factory, tenant=tenant_id, authors=authors, keywords=keywords, cutoff=before_ts,
                     is_allowed=is_allowed, clock=clock, budget=budget_seconds)
    channels = reader.channels()
    reader.scan(channels)
    return reader.result()
