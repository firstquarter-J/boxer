"""Company-specific Slack adapter assembly for Boxer."""

from importlib import import_module
from typing import Any


# Transport DTO/client submodule을 검사할 때 production entry 전체를 조립하지
# 않도록 package facade도 실제 create_app 접근 시점까지 지연한다.
def __getattr__(name: str) -> Any:
    if name != "create_app":
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(
        import_module("boxer_company_adapter_slack.company"),
        "create_app",
    )
    globals()[name] = value
    return value

__all__ = ["create_app"]
