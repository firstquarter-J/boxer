from boxer.context.entries import ContextEntry


def _trim_context_lines(lines: list[str], max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    kept: list[str] = []
    total_chars = 0
    for line in reversed(lines):
        next_len = len(line) + (1 if kept else 0)
        if total_chars + next_len > max_chars:
            break
        kept.append(line)
        total_chars += next_len
    kept.reverse()
    return "\n".join(kept)


def _limit_context_entries(
    entries: list[ContextEntry],
    max_entries: int,
) -> list[ContextEntry]:
    if max_entries <= 0 or not entries:
        return []
    return entries[-max_entries:]


def window_context_entries(
    entries: list[ContextEntry],
    *,
    max_chars: int,
) -> list[ContextEntry]:
    """렌더링 길이 기준으로 최신 entry만 보존해 semantic consumer도 같은 창을 쓴다."""
    if max_chars <= 0:
        return []

    candidates = [
        (entry, _render_context_entry_line(entry))
        for entry in entries
        if entry.get("text")
    ]
    kept: list[ContextEntry] = []
    total_chars = 0
    for entry, line in reversed(candidates):
        next_len = len(line) + (1 if kept else 0)
        if total_chars + next_len > max_chars:
            break
        kept.append(entry)
        total_chars += next_len
    kept.reverse()
    return kept


def _render_context_text(
    entries: list[ContextEntry],
    *,
    max_chars: int,
) -> str:
    lines = [
        _render_context_entry_line(entry)
        for entry in window_context_entries(
            entries,
            max_chars=max_chars,
        )
    ]
    return "\n".join(lines)


def _render_context_entry_line(entry: ContextEntry) -> str:
    author_id = str(entry.get("author_id") or "").strip()
    if author_id:
        label = author_id
    else:
        source = str(entry.get("source") or "").strip()
        kind = str(entry.get("kind") or "").strip()
        if source and kind:
            label = f"{source}/{kind}"
        else:
            label = source or kind or "context"
    return f"{label}: {entry['text']}"
