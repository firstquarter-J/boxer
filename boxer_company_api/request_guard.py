from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import threading
import time
from typing import Any, Literal, Mapping


@dataclass(frozen=True, slots=True)
class MutationRequestReservation:
    key: tuple[str, str, str]
    fingerprint: str


@dataclass(frozen=True, slots=True)
class MutationRequestDecision:
    status: Literal["bypass", "reserved", "replay", "busy", "conflict"]
    reservation: MutationRequestReservation | None = None
    payload: Mapping[str, Any] | None = None


@dataclass(slots=True)
class _Entry:
    fingerprint: str
    state: Literal["in_flight", "completed", "uncertain"]
    expires_at: float
    payload: Mapping[str, Any] | None = None


class MutationRequestGuard:
    """단일 API 프로세스 안에서 같은 mutation turn의 재실행만 억제한다.

    Slack redelivery처럼 request ID가 같은 전송은 재실행하지 않는다.
    서로 다른 정상 요청까지 직렬화하는 것은 기존 Slack 동작이 아니므로
    target 단위 lease를 두지 않는다.
    """

    def __init__(
        self,
        *,
        clock: Any = time.monotonic,
        completed_ttl_sec: float = 86_400.0,
    ) -> None:
        self._clock = clock
        self._completed_ttl_sec = max(60.0, float(completed_ttl_sec))
        self._lock = threading.Lock()
        self._entries: dict[tuple[str, str, str], _Entry] = {}

    def reserve(
        self,
        *,
        caller_id: str,
        request_id: str,
        turn: Any,
        mutation_capable: bool,
    ) -> MutationRequestDecision:
        # route group만으로는 operations의 조회와 mutation을 구분할 수 없다.
        # API admission이 공통 domain matcher로 확정한 요청만 registry에 넣는다.
        if not mutation_capable:
            return MutationRequestDecision(status="bypass")

        tenant_id = str(getattr(turn, "tenantId", "") or "").strip()
        key = (str(caller_id or "").strip(), tenant_id, request_id)
        fingerprint = _turn_fingerprint(turn)
        now = float(self._clock())

        with self._lock:
            self._remove_expired(now)
            existing = self._entries.get(key)
            if existing is not None:
                if existing.fingerprint != fingerprint:
                    return MutationRequestDecision(status="conflict")
                if existing.state == "completed" and existing.payload is not None:
                    return MutationRequestDecision(
                        status="replay",
                        payload=dict(existing.payload),
                    )
                return MutationRequestDecision(status="busy")

            reservation = MutationRequestReservation(
                key=key,
                fingerprint=fingerprint,
            )
            self._entries[key] = _Entry(
                fingerprint=fingerprint,
                state="in_flight",
                # 실행 중 entry는 정상 완료/불확실 전이 전까지 지우지 않는다.
                expires_at=float("inf"),
            )
            return MutationRequestDecision(
                status="reserved",
                reservation=reservation,
            )

    def complete(
        self,
        reservation: MutationRequestReservation,
        payload: Mapping[str, Any],
    ) -> None:
        now = float(self._clock())
        with self._lock:
            entry = self._matching_entry(reservation)
            if entry is None:
                return
            entry.state = "completed"
            entry.payload = dict(payload)
            entry.expires_at = now + self._completed_ttl_sec

    def mark_uncertain(
        self,
        reservation: MutationRequestReservation,
    ) -> None:
        with self._lock:
            entry = self._matching_entry(reservation)
            if entry is None:
                return
            entry.state = "uncertain"
            entry.payload = None
            entry.expires_at = float("inf")
            # 처리 여부를 모르는 동일 request ID만 재실행하지 않는다.

    def release(
        self,
        reservation: MutationRequestReservation,
    ) -> None:
        """외부 side effect 전 실패한 reservation만 완전히 해제한다."""

        with self._lock:
            entry = self._matching_entry(reservation)
            if entry is None:
                return
            self._entries.pop(reservation.key, None)

    def _matching_entry(
        self,
        reservation: MutationRequestReservation,
    ) -> _Entry | None:
        entry = self._entries.get(reservation.key)
        if entry is None or entry.fingerprint != reservation.fingerprint:
            return None
        return entry

    def _remove_expired(self, now: float) -> None:
        expired = [
            key
            for key, entry in self._entries.items()
            if entry.expires_at <= now
        ]
        for key in expired:
            self._entries.pop(key)


def _turn_fingerprint(turn: Any) -> str:
    if hasattr(turn, "model_dump"):
        value = turn.model_dump(mode="json")
    else:
        value = dict(turn)
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "MutationRequestDecision",
    "MutationRequestGuard",
    "MutationRequestReservation",
]
