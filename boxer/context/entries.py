from typing import Literal, NotRequired, Required, TypeAlias, TypedDict


class ContextEntry(TypedDict):
    kind: NotRequired["ContextEntryKind"]
    source: NotRequired["ContextEntrySource"]
    author_id: NotRequired[str | None]
    text: Required[str]
    created_at: NotRequired[str | None]


ContextEntryKind: TypeAlias = Literal[
    "message",
    "profile",
    "system",
    "tool",
    "evidence",
]

ContextEntrySource: TypeAlias = Literal[
    "slack",
    "widget",
    "team_profile",
    "telegram",
]
