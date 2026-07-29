from importlib import import_module
from typing import Any

# 패키지 metadata 조회가 FastAPI와 회사 runtime 조립까지 즉시 당기지 않도록
# app factory는 실제 사용 시점에만 가져온다.
_LAZY_EXPORTS = {
    "create_company_api_app": (
        "boxer_company_api.app",
        "create_company_api_app",
    ),
}


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(
            f"module {__name__!r} has no attribute {name!r}"
        )
    module_name, attribute_name = target
    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value


__all__ = ["create_company_api_app"]
