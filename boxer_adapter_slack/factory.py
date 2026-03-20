import importlib
from collections.abc import Callable
from typing import Any

from boxer_adapter_slack import settings as ss


LEGACY_ENTRYPOINT_ALIASES = {
    "boxer.adapters.common.slack": "boxer_adapter_slack.common",
    "boxer.adapters.factory": "boxer_adapter_slack.factory",
    "boxer.adapters.sample.slack": "boxer_adapter_slack.sample",
    # Old alias wrapper defaulted to the sample adapter when self-pointing.
    "boxer.adapters.slack": "boxer_adapter_slack.sample",
}


def _normalize_entrypoint(path: str) -> str:
    target = (path or "").strip()
    if ":" not in target:
        return target
    module_name, callable_name = target.split(":", 1)
    module_name = module_name.strip()
    callable_name = callable_name.strip()
    normalized_module = LEGACY_ENTRYPOINT_ALIASES.get(module_name, module_name)
    return f"{normalized_module}:{callable_name}"


def load_entrypoint(path: str) -> Callable[[], Any]:
    target = _normalize_entrypoint(path)
    if ":" not in target:
        raise RuntimeError(
            "ADAPTER_ENTRYPOINT 형식 오류: '<module>:<callable>' 형식이어야 해"
        )

    module_name, callable_name = target.split(":", 1)
    module_name = module_name.strip()
    callable_name = callable_name.strip()
    if not module_name or not callable_name:
        raise RuntimeError(
            "ADAPTER_ENTRYPOINT 형식 오류: '<module>:<callable>' 형식이어야 해"
        )

    try:
        module = importlib.import_module(module_name)
    except Exception as exc:
        raise RuntimeError(f"어댑터 모듈 로딩 실패: {module_name}") from exc

    factory = getattr(module, callable_name, None)
    if not callable(factory):
        raise RuntimeError(f"어댑터 팩토리 함수가 없어: {target}")

    return factory


def create_app() -> Any:
    factory = load_entrypoint(ss.ADAPTER_ENTRYPOINT)
    return factory()


# legacy alias
_load_entrypoint = load_entrypoint
