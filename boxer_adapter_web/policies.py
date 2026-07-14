from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class HandoffPolicy:
    on_missing_evidence: bool = True
    prompt_before_queue: bool = False
