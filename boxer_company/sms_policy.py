"""신규 문자 발송과 기존 발송 결과 수집의 활성 범위를 분리한다."""

from __future__ import annotations

import os
from collections.abc import Mapping


def sms_sending_enabled(env: Mapping[str, str] | None = None) -> bool:
    """한국 설치의 기본 동작은 유지하고 미국은 명시적으로 발송을 닫는다."""

    from boxer_company.deployment import load_deployment_profile

    source = env if env is not None else os.environ
    profile = load_deployment_profile(source)
    default = "true" if profile.country == "kr" else "false"
    raw = str(source.get("BOXER_COMPANY_SMS_ENABLED", default)).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise ValueError("company_deployment_configuration_invalid")
