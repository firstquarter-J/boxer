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


def _render_context_text(
    entries: list[ContextEntry],
    *,
    max_chars: int,
) -> str:
    def _render_entry_label(entry: ContextEntry) -> str:
        author_id = str(entry.get("author_id") or "").strip()
        if author_id:
            return author_id
        source = str(entry.get("source") or "").strip()
        kind = str(entry.get("kind") or "").strip()
        if source and kind:
            return f"{source}/{kind}"
        if source:
            return source
        if kind:
            return kind
        return "context"

    lines = [
        f"{_render_entry_label(entry)}: {entry['text']}"
        for entry in entries
        if entry.get("text")
    ]
    if not lines:
        return ""
    return _trim_context_lines(lines, max_chars)
