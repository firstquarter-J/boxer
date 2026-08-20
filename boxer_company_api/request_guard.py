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
    target_key: str


@dataclass(frozen=True, slots=True)
class MutationRequestDecision:
    status: Literal["bypass", "reserved", "replay", "busy", "conflict"]
    reservation: MutationRequestReservation | None = None
    payload: Mapping[str, Any] | None = None


@dataclass(slots=True)
class _Entry:
    fingerprint: str
    state: Literal["in_flight", "completed", "uncertain"]
    target_key: str
    expires_at: float
    payload: Mapping[str, Any] | None = None


class MutationRequestGuard:
    """단일 API 프로세스 안에서 mutation-capable turn 중복만 억제한다.

    이 guard는 기존 단일 worker 운영의 Slack redelivery와 동시 요청을
    막는 최소 경계다. 프로세스 재시작이나 다중 인스턴스 exactly-once를
    약속하지 않으며, 그런 확장이 생기면 공유 저장소로 교체해야 한다.
    """

    def __init__(
        self,
        *,
        clock: Any = time.monotonic,
        completed_ttl_sec: float = 86_400.0,
        uncertain_ttl_sec: float | None = None,
    ) -> None:
        self._clock = clock
        self._completed_ttl_sec = max(60.0, float(completed_ttl_sec))
        # 호환 인자는 받되 결과 불명 mutation은 프로세스 생존 동안 자동
        # 해제하지 않는다. 재시작 전 운영 확인 없이 TTL로 재실행하면 안 된다.
        del uncertain_ttl_sec
        self._lock = threading.Lock()
        self._entries: dict[tuple[str, str, str], _Entry] = {}
        self._targets: dict[str, tuple[str, str, str]] = {}

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

        route_group = str(getattr(turn, "routeGroup", None) or "").strip()
        tenant_id = str(getattr(turn, "tenantId", "") or "").strip()
        key = (str(caller_id or "").strip(), tenant_id, request_id)
        fingerprint = _turn_fingerprint(turn)
        target_key = _turn_target_key(turn, route_group=route_group)
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

            target_owner = self._targets.get(target_key)
            if target_owner is not None and target_owner != key:
                # 같은 장비/바코드 mutation이 겹치면 두 번째 요청은 실행하지 않는다.
                return MutationRequestDecision(status="busy")

            reservation = MutationRequestReservation(
                key=key,
                fingerprint=fingerprint,
                target_key=target_key,
            )
            self._entries[key] = _Entry(
                fingerprint=fingerprint,
                state="in_flight",
                target_key=target_key,
                # 실행 중 entry는 정상 완료/불확실 전이 전까지 지우지 않는다.
                expires_at=float("inf"),
            )
            self._targets[target_key] = key
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
            self._release_target(reservation)

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
            # 처리 여부를 모르는 동안 같은 target을 다른 request ID가 재실행하지 않는다.

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
            self._release_target(reservation)

    def _matching_entry(
        self,
        reservation: MutationRequestReservation,
    ) -> _Entry | None:
        entry = self._entries.get(reservation.key)
        if entry is None or entry.fingerprint != reservation.fingerprint:
            return None
        return entry

    def _release_target(
        self,
        reservation: MutationRequestReservation,
    ) -> None:
        if self._targets.get(reservation.target_key) == reservation.key:
            self._targets.pop(reservation.target_key, None)

    def _remove_expired(self, now: float) -> None:
        expired = [
            key
            for key, entry in self._entries.items()
            if entry.expires_at <= now
        ]
        for key in expired:
            entry = self._entries.pop(key)
            if self._targets.get(entry.target_key) == key:
                self._targets.pop(entry.target_key, None)


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


def _turn_target_key(turn: Any, *, route_group: str) -> str:
    tenant_id = str(getattr(turn, "tenantId", "") or "").strip()
    del route_group
    # 바코드는 DB에서 장비로 해석되므로 HTTP 입력만으로 canonical target을
    # 안전하게 맞출 수 없다. 단일 worker의 mutation-capable turn 전체를
    # 직렬화해 서로 다른 route가 같은 장비 tunnel을 동시에 여는 일을 막는다.
    return f"{tenant_id}:mutation"


__all__ = [
    "MutationRequestDecision",
    "MutationRequestGuard",
    "MutationRequestReservation",
]
