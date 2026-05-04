import json
from typing import Any

from boxer_company import settings as cs


class DeviceStateRedisUnavailable(RuntimeError):
    pass


class DeviceStateRedisClient:
    def __init__(self, client: Any) -> None:
        self._client = client

    @classmethod
    def from_settings(cls) -> "DeviceStateRedisClient":
        if not cs.DEVICE_STATE_REDIS_HOST:
            raise DeviceStateRedisUnavailable("DEVICE_STATE_REDIS_HOST가 비어 있어")

        try:
            import redis
        except Exception as exc:  # pragma: no cover - dependency absence is environment-specific
            raise DeviceStateRedisUnavailable(f"redis 패키지를 불러오지 못했어: {exc}") from exc

        # MDA/socket Redis는 device:* / agent:* JSON 캐시를 보관하므로 문자열 응답으로 읽는다.
        client = redis.Redis(
            host=cs.DEVICE_STATE_REDIS_HOST,
            port=int(cs.DEVICE_STATE_REDIS_PORT or 6379),
            password=cs.DEVICE_STATE_REDIS_PASSWORD or None,
            ssl=bool(cs.DEVICE_STATE_REDIS_TLS),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=10,
        )
        return cls(client)

    def ping(self) -> None:
        try:
            self._client.ping()
        except Exception as exc:
            raise DeviceStateRedisUnavailable(f"Redis ping 실패: {exc}") from exc

    def load_device_and_agent_states(
        self,
        device_names: list[str],
    ) -> dict[str, dict[str, Any]]:
        names = [str(name or "").strip() for name in device_names if str(name or "").strip()]
        if not names:
            return {}

        snapshot: dict[str, dict[str, Any]] = {}
        batch_size = 200
        for start in range(0, len(names), batch_size):
            # 장비 수가 늘어나도 단일 MGET 요청이 너무 커지지 않게 장비/agent 키를 묶어서 읽는다.
            batch_names = names[start : start + batch_size]
            device_keys = [f"device:{name}" for name in batch_names]
            agent_keys = [f"agent:{name}" for name in batch_names]
            try:
                device_values = self._client.mget(device_keys)
                agent_values = self._client.mget(agent_keys)
            except Exception as exc:
                raise DeviceStateRedisUnavailable(f"Redis MGET 실패: {exc}") from exc

            for index, device_name in enumerate(batch_names):
                snapshot[device_name] = {
                    "deviceState": _parse_redis_json_value(
                        device_values[index] if index < len(device_values) else None
                    ),
                    "agentState": _parse_redis_json_value(
                        agent_values[index] if index < len(agent_values) else None
                    ),
                }
        return snapshot


def _parse_redis_json_value(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return value
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None
