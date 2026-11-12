from __future__ import annotations

import logging
from unittest.mock import Mock

import pytest

from boxer_company.deployment import sms_sending_enabled


@pytest.mark.parametrize(("env", "enabled"), [
    ({}, True),
    ({"BOXER_COMPANY_COUNTRY": "us"}, False),
    ({"BOXER_COMPANY_SMS_ENABLED": "false"}, False),
    ({"BOXER_COMPANY_SMS_ENABLED": "true"}, True),
])
def test_sms_policy_defaults_preserve_korea_and_disable_america(env, enabled) -> None:
    # 신규 발송 flag는 provider나 결과 수집 flag와 독립된 배포 정책이다.
    assert sms_sending_enabled(env) is enabled


def test_sms_policy_rejects_an_invalid_explicit_flag() -> None:
    with pytest.raises(ValueError, match="company_deployment_configuration_invalid"):
        sms_sending_enabled({"BOXER_COMPANY_SMS_ENABLED": "flase"})


@pytest.mark.parametrize("sender_name", (
    "_send_device_health_alert_sms", "_send_device_health_alert_solapi_sms",
))
def test_disabled_sms_never_enters_configured_external_provider(monkeypatch, sender_name) -> None:
    from boxer_company.assistant import device_health_alert_action_route as route

    # 과거 provider 설정이 남아 있어도 공통 발송 경계에서 먼저 차단한다.
    monkeypatch.setenv("BOXER_COMPANY_SMS_ENABLED", "false")
    monkeypatch.setattr(route.company_settings, "DEVICE_HEALTH_MONITOR_SMS_PROVIDER", "solapi")
    post = Mock(side_effect=AssertionError("provider must not be called"))
    monkeypatch.setattr(route.requests, "post", post)

    result = getattr(route, sender_name)({}, logger=logging.getLogger(__name__))

    assert result == {"status": "disabled", "ok": False}
    post.assert_not_called()
