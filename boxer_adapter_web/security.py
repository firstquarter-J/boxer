from __future__ import annotations

from collections import defaultdict, deque
from time import monotonic
from urllib.parse import urlparse


class SlidingWindowRateLimiter:
    def __init__(self, *, limit: int, window_seconds: float = 60.0) -> None:
        self._limit = max(0, int(limit))
        self._window_seconds = max(1.0, float(window_seconds))
        self._buckets: dict[str, deque[float]] = defaultdict(deque)

    def allow(self, key: str) -> bool:
        if self._limit <= 0:
            return True

        now = monotonic()
        bucket = self._buckets[key]
        # 위젯 WebSocket은 연결이 오래 유지되므로 key별 최근 이벤트만 남겨 메모리 증가를 제한한다.
        while bucket and now - bucket[0] > self._window_seconds:
            bucket.popleft()

        if len(bucket) >= self._limit:
            return False

        bucket.append(now)
        return True


def is_origin_allowed(origin: str | None, allowed_origins: list[str]) -> bool:
    normalized_allowed = [item.strip().rstrip("/") for item in allowed_origins if item.strip()]
    if not normalized_allowed or "*" in normalized_allowed:
        return True
    if not origin:
        return False

    normalized_origin = origin.strip().rstrip("/")
    if normalized_origin in normalized_allowed:
        return True

    return any(_matches_wildcard_origin(normalized_origin, allowed) for allowed in normalized_allowed)


def is_same_host_origin(origin: str | None, host: str | None) -> bool:
    if not origin:
        return True
    if not host:
        return False

    # admin websocket은 별도 허용 목록이 없으면 브라우저가 접속한 Host와만 통신한다.
    parsed_origin = urlparse(origin.strip())
    return bool(parsed_origin.netloc) and parsed_origin.netloc.lower() == host.strip().lower()


def parse_bool(raw_value: str | None, *, default: bool = False) -> bool:
    if raw_value is None:
        return default
    normalized = raw_value.strip().lower()
    if not normalized:
        return default
    return normalized in {"1", "true", "yes", "y", "on"}


def _matches_wildcard_origin(origin: str, allowed: str) -> bool:
    if "*" not in allowed:
        return False

    parsed_origin = urlparse(origin)
    parsed_allowed = urlparse(allowed)
    if parsed_origin.scheme != parsed_allowed.scheme:
        return False

    allowed_host = parsed_allowed.netloc.lower()
    origin_host = parsed_origin.netloc.lower()
    if not allowed_host.startswith("*."):
        return False

    suffix = allowed_host[1:]
    return origin_host.endswith(suffix) and origin_host != suffix.lstrip(".")
