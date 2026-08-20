from __future__ import annotations

from dataclasses import dataclass, fields, replace
import logging
import threading
from typing import Any
import unittest
from unittest.mock import patch

import requests

from boxer_company.assistant import CompanyAssistantRequest
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiAmbiguousTimeoutError,
    CompanyApiAvailabilityError,
    CompanyApiClientSettings,
    CompanyApiContractError,
    CompanyApiPolicyError,
    CompanyAssistantApiClient,
    load_company_api_client_settings,
)


_TOKEN = "service-token-" + ("x" * 40)
_TRACEPARENT = (
    "00-0123456789abcdef0123456789abcdef-"
    "0123456789abcdef-01"
)
_QUESTION = "회사 노션에서 커머스 운영 기준 찾아줘"
_SILENT_LOGGER = logging.getLogger(f"{__name__}.silent")
_SILENT_LOGGER.disabled = True


@dataclass
class _FakeResponse:
    status_code: int
    payload: Any
    content_type: str
    raw_content: bytes = b"{}"

    @property
    def headers(self) -> dict[str, str]:
        return {"content-type": self.content_type}

    @property
    def content(self) -> bytes:
        return self.raw_content

    def json(self) -> Any:
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class _FakeSession:
    def __init__(self, *results: Any) -> None:
        self.results = list(results)
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        if not self.results:
            raise AssertionError("unexpected HTTP request")
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class _CollectingHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def _settings(
    *,
    max_retries: int = 0,
) -> CompanyApiClientSettings:
    return CompanyApiClientSettings(
        base_url="http://127.0.0.1:8010",
        token=_TOKEN,
        connect_timeout_sec=2.0,
        read_timeout_sec=90.0,
        max_retries=max_retries,
        notion_mode="remote",
        notion_fallback_enabled=True,
    )


def _request(
    *,
    request_id: str = "slack:T1:C1:1785312000.000001",
    context_entries: tuple[dict[str, Any], ...] = (),
    metadata: dict[str, Any] | None = None,
) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id=request_id,
        tenant_id="T1",
        actor_id="U1",
        channel="slack",
        conversation_id="1785312000.000001",
        question=_QUESTION,
        locale="ko",
        context_entries=context_entries,
        metadata=metadata or {"channel_id": "C1"},
    )


def _success_payload(
    request_id: str,
    *,
    outcome: str = "answered",
) -> dict[str, Any]:
    return {
        "requestId": request_id,
        "route": "company_notion_qa",
        "outcome": outcome,
        "messages": [
            {
                "body": "**회사 Notion 문서 답변**",
                "deliveryScope": "conversation",
                "mentionActor": True,
                "format": "commonmark",
            }
        ],
        "sources": [
            {
                "sourceId": "notion-source",
                "title": "운영 기준",
                "uri": "https://app.notion.com/p/operations",
                "score": 0.9,
            }
        ],
        "usedLlm": True,
        "fallbackReason": None,
        "suggestedAction": None,
        "asyncJob": None,
    }


def _success_response(
    request_id: str,
    *,
    outcome: str = "answered",
) -> _FakeResponse:
    return _FakeResponse(
        status_code=200,
        payload=_success_payload(request_id, outcome=outcome),
        content_type="application/json",
    )


def _problem_response(
    request_id: str,
    *,
    status: int,
    code: str,
    retryable: bool,
) -> _FakeResponse:
    return _FakeResponse(
        status_code=status,
        payload={
            "type": f"urn:boxer-company-api:problem:{code}",
            "title": "Safe problem",
            "status": status,
            "code": code,
            "requestId": request_id,
            "retryable": retryable,
        },
        content_type="application/problem+json",
    )


def _client(
    session: _FakeSession,
    *,
    settings: CompanyApiClientSettings | None = None,
    sleep: Any = lambda _seconds: None,
    logger: logging.Logger | None = None,
    traceparent: str = _TRACEPARENT,
) -> CompanyAssistantApiClient:
    return CompanyAssistantApiClient(
        settings or _settings(),
        session=session,
        sleep=sleep,
        traceparent_factory=lambda: traceparent,
        logger=logger or _SILENT_LOGGER,
    )


class CompanyApiClientSettingsTests(unittest.TestCase):
    def test_transport_only_remote_requires_every_route_and_fallback(
        self,
    ) -> None:
        # 새 rollout field가 추가돼도 완전 remote 판정에서 빠지지 않도록
        # dataclass field 전체를 기준으로 경계를 고정한다.
        base = CompanyApiClientSettings(
            base_url="http://127.0.0.1:8010",
            token=_TOKEN,
        )
        mode_fields = tuple(
            item.name
            for item in fields(base)
            if item.name.endswith("_mode")
        )
        fallback_fields = tuple(
            item.name
            for item in fields(base)
            if item.name.endswith("_fallback_enabled")
        )
        remote = replace(
            base,
            **{name: "remote" for name in mode_fields},
            **{name: False for name in fallback_fields},
            automation_remote_cycles=(
                "weekly_recordings",
                "daily_device_round",
                "device_health_monitor",
                "device_notification_alert",
                "sms_delivery",
            ),
        )

        self.assertTrue(remote.transport_only_remote)
        for field_name in mode_fields:
            for mode in ("local", "shadow"):
                with self.subTest(field_name=field_name, mode=mode):
                    changes: dict[str, Any] = {field_name: mode}
                    if field_name == "operations_mode":
                        # remote action cycle은 operations와 분리할 수
                        # 없으므로 이 property 테스트에서만 함께 놓는다.
                        changes["automation_remote_cycles"] = tuple(
                            cycle
                            for cycle in remote.automation_remote_cycles
                            if cycle
                            not in {
                                "device_health_monitor",
                                "device_notification_alert",
                            }
                        )
                    self.assertFalse(
                        replace(
                            remote,
                            **changes,
                        ).transport_only_remote
                    )
        for field_name in fallback_fields:
            with self.subTest(field_name=field_name):
                self.assertFalse(
                    replace(
                        remote,
                        **{field_name: True},
                    ).transport_only_remote
                )

    def test_local_is_the_credential_free_rollback_default(self) -> None:
        settings = load_company_api_client_settings({})

        self.assertEqual(settings.notion_mode, "local")
        self.assertEqual(settings.structured_mode, "local")
        self.assertEqual(settings.device_mode, "local")
        self.assertEqual(settings.device_detail_mode, "local")
        self.assertEqual(settings.recording_failure_mode, "local")
        self.assertEqual(settings.barcode_log_mode, "local")
        self.assertEqual(settings.barcode_mode, "local")
        self.assertEqual(settings.barcode_residual_mode, "local")
        self.assertEqual(settings.barcode_timeline_mode, "local")
        self.assertEqual(settings.barcode_freeform_mode, "local")
        self.assertEqual(settings.freeform_mode, "local")
        self.assertEqual(settings.playbook_mode, "local")
        self.assertEqual(settings.weekly_summary_mode, "local")
        self.assertEqual(settings.operations_mode, "local")
        self.assertEqual(settings.operations_read_timeout_sec, 700.0)
        self.assertFalse(settings.enabled)
        self.assertEqual(settings.base_url, "")
        self.assertEqual(settings.token, "")
        self.assertFalse(settings.notion_fallback_enabled)
        self.assertFalse(settings.structured_fallback_enabled)
        self.assertFalse(settings.device_fallback_enabled)
        self.assertFalse(settings.device_detail_fallback_enabled)
        self.assertFalse(settings.recording_failure_fallback_enabled)
        self.assertFalse(settings.barcode_log_fallback_enabled)
        self.assertFalse(settings.barcode_fallback_enabled)
        self.assertFalse(settings.barcode_residual_fallback_enabled)
        self.assertFalse(settings.barcode_timeline_fallback_enabled)
        self.assertFalse(settings.barcode_freeform_fallback_enabled)
        self.assertFalse(settings.freeform_fallback_enabled)
        self.assertFalse(settings.playbook_fallback_enabled)
        self.assertFalse(settings.weekly_summary_fallback_enabled)
        self.assertFalse(settings.operations_fallback_enabled)

    def test_local_rollback_ignores_stale_remote_credentials(self) -> None:
        settings = load_company_api_client_settings(
            {
                "BOXER_COMPANY_API_NOTION_MODE": "local",
                "BOXER_COMPANY_API_STRUCTURED_MODE": "local",
                "BOXER_COMPANY_API_DEVICE_MODE": "local",
                "BOXER_COMPANY_API_DEVICE_DETAIL_MODE": "local",
                "BOXER_COMPANY_API_RECORDING_FAILURE_MODE": "local",
                "BOXER_COMPANY_API_BARCODE_LOG_MODE": "local",
                "BOXER_COMPANY_API_BARCODE_MODE": "local",
                "BOXER_COMPANY_API_BARCODE_RESIDUAL_MODE": "local",
                "BOXER_COMPANY_API_BARCODE_TIMELINE_MODE": "local",
                "BOXER_COMPANY_API_BARCODE_FREEFORM_MODE": "local",
                "BOXER_COMPANY_API_FREEFORM_MODE": "local",
                "BOXER_COMPANY_API_PLAYBOOK_MODE": "local",
                "BOXER_COMPANY_API_WEEKLY_SUMMARY_MODE": "local",
                "BOXER_COMPANY_API_OPERATIONS_MODE": "local",
                "BOXER_COMPANY_API_BASE_URL": "http://public.example.com",
                "BOXER_COMPANY_API_SERVICE_TOKEN": "stale-invalid-token",
                "BOXER_COMPANY_API_CONNECT_TIMEOUT_SEC": "invalid",
                "BOXER_COMPANY_API_READ_TIMEOUT_SEC": "-1",
                "BOXER_COMPANY_API_OPERATIONS_READ_TIMEOUT_SEC": "invalid",
                "BOXER_COMPANY_API_MAX_RETRIES": "999",
                "BOXER_COMPANY_API_NOTION_FALLBACK_ENABLED": "invalid",
                "BOXER_COMPANY_API_STRUCTURED_FALLBACK_ENABLED": "invalid",
                "BOXER_COMPANY_API_DEVICE_FALLBACK_ENABLED": "invalid",
                "BOXER_COMPANY_API_DEVICE_DETAIL_FALLBACK_ENABLED": "invalid",
                "BOXER_COMPANY_API_RECORDING_FAILURE_FALLBACK_ENABLED": "invalid",
                "BOXER_COMPANY_API_BARCODE_LOG_FALLBACK_ENABLED": "invalid",
                "BOXER_COMPANY_API_BARCODE_FALLBACK_ENABLED": "invalid",
                "BOXER_COMPANY_API_BARCODE_RESIDUAL_FALLBACK_ENABLED": "invalid",
                "BOXER_COMPANY_API_BARCODE_TIMELINE_FALLBACK_ENABLED": "invalid",
                "BOXER_COMPANY_API_BARCODE_FREEFORM_FALLBACK_ENABLED": "invalid",
                "BOXER_COMPANY_API_FREEFORM_FALLBACK_ENABLED": "invalid",
                "BOXER_COMPANY_API_PLAYBOOK_FALLBACK_ENABLED": "invalid",
                "BOXER_COMPANY_API_WEEKLY_SUMMARY_FALLBACK_ENABLED": "invalid",
                "BOXER_COMPANY_API_OPERATIONS_FALLBACK_ENABLED": "invalid",
            }
        )

        self.assertEqual(settings.notion_mode, "local")
        self.assertEqual(settings.structured_mode, "local")
        self.assertEqual(settings.device_mode, "local")
        self.assertEqual(settings.device_detail_mode, "local")
        self.assertEqual(settings.recording_failure_mode, "local")
        self.assertEqual(settings.barcode_log_mode, "local")
        self.assertEqual(settings.barcode_mode, "local")
        self.assertEqual(settings.barcode_residual_mode, "local")
        self.assertEqual(settings.barcode_timeline_mode, "local")
        self.assertEqual(settings.barcode_freeform_mode, "local")
        self.assertEqual(settings.freeform_mode, "local")
        self.assertEqual(settings.playbook_mode, "local")
        self.assertEqual(settings.weekly_summary_mode, "local")
        self.assertEqual(settings.operations_mode, "local")
        self.assertEqual(settings.base_url, "")
        self.assertEqual(settings.token, "")

    def test_manual_remote_settings_cannot_bypass_transport_validation(
        self,
    ) -> None:
        invalid_settings = (
            replace(
                _settings(),
                base_url="http://public.example.com",
            ),
            replace(_settings(), token="short"),
            replace(_settings(), read_timeout_sec=float("nan")),
            replace(
                _settings(),
                operations_read_timeout_sec=float("nan"),
            ),
            replace(_settings(), max_retries=3),
            replace(_settings(), structured_mode="invalid"),
            replace(_settings(), structured_fallback_enabled="true"),
            replace(_settings(), device_detail_mode="invalid"),
            replace(
                _settings(),
                device_detail_fallback_enabled="true",
            ),
            replace(_settings(), weekly_summary_mode="invalid"),
            replace(
                _settings(),
                weekly_summary_fallback_enabled="true",
            ),
            replace(_settings(), barcode_freeform_mode="invalid"),
            replace(
                _settings(),
                barcode_freeform_fallback_enabled="true",
            ),
            replace(_settings(), freeform_mode="invalid"),
            replace(
                _settings(),
                freeform_fallback_enabled="true",
            ),
            replace(_settings(), operations_mode="shadow"),
            replace(
                _settings(),
                operations_mode="remote",
                operations_fallback_enabled=True,
            ),
        )

        for settings in invalid_settings:
            with self.subTest(base_url=settings.base_url):
                with self.assertRaises(CompanyApiContractError):
                    CompanyAssistantApiClient(
                        settings,
                        session=_FakeSession(),
                    )

    def test_remote_settings_validate_internal_transport_and_hide_token(self) -> None:
        settings = load_company_api_client_settings(
            {
                "BOXER_COMPANY_API_BASE_URL": (
                    "http://10.40.102.50:8010/"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                "BOXER_COMPANY_API_NOTION_MODE": "shadow",
                "BOXER_COMPANY_API_CONNECT_TIMEOUT_SEC": "1.5",
                "BOXER_COMPANY_API_READ_TIMEOUT_SEC": "75",
                "BOXER_COMPANY_API_OPERATIONS_READ_TIMEOUT_SEC": "720",
                "BOXER_COMPANY_API_MAX_RETRIES": "2",
                "BOXER_COMPANY_API_NOTION_FALLBACK_ENABLED": "false",
            }
        )

        self.assertEqual(
            settings.base_url,
            "http://10.40.102.50:8010",
        )
        self.assertEqual(settings.notion_mode, "shadow")
        self.assertEqual(settings.structured_mode, "local")
        self.assertEqual(settings.connect_timeout_sec, 1.5)
        self.assertEqual(settings.read_timeout_sec, 75)
        self.assertEqual(settings.operations_read_timeout_sec, 720)
        self.assertEqual(settings.max_retries, 2)
        self.assertFalse(settings.notion_fallback_enabled)
        self.assertFalse(settings.structured_fallback_enabled)
        self.assertNotIn(_TOKEN, repr(settings))

    def test_structured_mode_independently_enables_shared_transport(
        self,
    ) -> None:
        settings = load_company_api_client_settings(
            {
                "BOXER_COMPANY_API_BASE_URL": (
                    "http://10.40.102.50:8010"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                "BOXER_COMPANY_API_NOTION_MODE": "local",
                "BOXER_COMPANY_API_STRUCTURED_MODE": "remote",
                "BOXER_COMPANY_API_NOTION_FALLBACK_ENABLED": "false",
                "BOXER_COMPANY_API_STRUCTURED_FALLBACK_ENABLED": "true",
            }
        )

        self.assertTrue(settings.enabled)
        self.assertEqual(settings.notion_mode, "local")
        self.assertEqual(settings.structured_mode, "remote")
        self.assertFalse(settings.notion_fallback_enabled)
        self.assertTrue(settings.structured_fallback_enabled)
        self.assertEqual(
            settings.base_url,
            "http://10.40.102.50:8010",
        )

    def test_remaining_route_mode_independently_enables_shared_transport(
        self,
    ) -> None:
        mode_keys = (
            (
                "BOXER_COMPANY_API_DEVICE_MODE",
                "device_mode",
            ),
            (
                "BOXER_COMPANY_API_DEVICE_DETAIL_MODE",
                "device_detail_mode",
            ),
            (
                "BOXER_COMPANY_API_RECORDING_FAILURE_MODE",
                "recording_failure_mode",
            ),
            (
                "BOXER_COMPANY_API_BARCODE_LOG_MODE",
                "barcode_log_mode",
            ),
            (
                "BOXER_COMPANY_API_BARCODE_MODE",
                "barcode_mode",
            ),
            (
                "BOXER_COMPANY_API_PLAYBOOK_MODE",
                "playbook_mode",
            ),
            (
                "BOXER_COMPANY_API_BARCODE_RESIDUAL_MODE",
                "barcode_residual_mode",
            ),
            (
                "BOXER_COMPANY_API_BARCODE_TIMELINE_MODE",
                "barcode_timeline_mode",
            ),
            (
                "BOXER_COMPANY_API_BARCODE_FREEFORM_MODE",
                "barcode_freeform_mode",
            ),
            (
                "BOXER_COMPANY_API_FREEFORM_MODE",
                "freeform_mode",
            ),
            (
                "BOXER_COMPANY_API_WEEKLY_SUMMARY_MODE",
                "weekly_summary_mode",
            ),
        )
        for env_key, field_name in mode_keys:
            with self.subTest(field_name=field_name):
                settings = load_company_api_client_settings(
                    {
                        "BOXER_COMPANY_API_BASE_URL": (
                            "http://10.40.102.50:8010"
                        ),
                        "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                        env_key: "shadow",
                    }
                )

                self.assertTrue(settings.enabled)
                self.assertTrue(settings.shadow_enabled)
                self.assertEqual(
                    getattr(settings, field_name),
                    "shadow",
                )
        self.assertNotIn(_TOKEN, repr(settings))

    def test_new_route_fallbacks_load_only_for_non_local_modes(self) -> None:
        # 신규 route는 같은 transport를 써도 fallback 정책은 각자 가진다.
        cases = (
            (
                "BOXER_COMPANY_API_WEEKLY_SUMMARY_MODE",
                "BOXER_COMPANY_API_WEEKLY_SUMMARY_FALLBACK_ENABLED",
                "weekly_summary_fallback_enabled",
            ),
            (
                "BOXER_COMPANY_API_BARCODE_FREEFORM_MODE",
                "BOXER_COMPANY_API_BARCODE_FREEFORM_FALLBACK_ENABLED",
                "barcode_freeform_fallback_enabled",
            ),
            (
                "BOXER_COMPANY_API_FREEFORM_MODE",
                "BOXER_COMPANY_API_FREEFORM_FALLBACK_ENABLED",
                "freeform_fallback_enabled",
            ),
        )
        for mode_key, fallback_key, field_name in cases:
            with self.subTest(field_name=field_name):
                settings = load_company_api_client_settings(
                    {
                        "BOXER_COMPANY_API_BASE_URL": (
                            "http://10.40.102.50:8010"
                        ),
                        "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                        mode_key: "remote",
                        fallback_key: "true",
                    }
                )

                self.assertTrue(settings.enabled)
                self.assertFalse(settings.shadow_enabled)
                self.assertTrue(getattr(settings, field_name))

    def test_device_detail_rejects_local_fallback_when_non_local(
        self,
    ) -> None:
        # remote 전환 뒤 tunnel lifecycle을 가진 legacy local 경로로
        # 조용히 되돌아가지 않도록 env와 수동 settings를 fail-closed한다.
        with self.assertRaisesRegex(
            CompanyApiContractError,
            "company_api_device_detail_fallback_unsafe",
        ):
            load_company_api_client_settings(
                {
                    "BOXER_COMPANY_API_BASE_URL": (
                        "http://10.40.102.50:8010"
                    ),
                    "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                    "BOXER_COMPANY_API_DEVICE_DETAIL_MODE": "remote",
                    "BOXER_COMPANY_API_DEVICE_DETAIL_FALLBACK_ENABLED": (
                        "true"
                    ),
                }
            )

        with self.assertRaisesRegex(
            CompanyApiContractError,
            "company_api_device_detail_fallback_unsafe",
        ):
            CompanyAssistantApiClient(
                replace(
                    _settings(),
                    device_detail_mode="remote",
                    device_detail_fallback_enabled=True,
                )
            )

    def test_operations_allows_only_remote_without_fallback(
        self,
    ) -> None:
        settings = load_company_api_client_settings(
            {
                "BOXER_COMPANY_API_BASE_URL": (
                    "http://10.40.102.50:8010"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                "BOXER_COMPANY_API_OPERATIONS_MODE": "remote",
            }
        )

        self.assertTrue(settings.enabled)
        self.assertEqual(settings.operations_mode, "remote")
        self.assertFalse(settings.operations_fallback_enabled)
        self.assertFalse(settings.shadow_enabled)

        for env in (
            {"BOXER_COMPANY_API_OPERATIONS_MODE": "shadow"},
            {
                "BOXER_COMPANY_API_BASE_URL": (
                    "http://10.40.102.50:8010"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                "BOXER_COMPANY_API_OPERATIONS_MODE": "remote",
                "BOXER_COMPANY_API_OPERATIONS_FALLBACK_ENABLED": "true",
            },
        ):
            with self.subTest(env_keys=sorted(env)):
                with self.assertRaises(CompanyApiContractError):
                    load_company_api_client_settings(env)

    def test_notion_and_structured_modes_enable_transport_independently(
        self,
    ) -> None:
        base = _settings()
        cases = (
            ("local", "local", False, False),
            ("shadow", "local", True, True),
            ("local", "shadow", True, True),
            ("remote", "local", True, False),
            ("local", "remote", True, False),
        )
        for (
            notion_mode,
            structured_mode,
            expected_enabled,
            expected_shadow_enabled,
        ) in cases:
            with self.subTest(
                notion_mode=notion_mode,
                structured_mode=structured_mode,
            ):
                settings = replace(
                    base,
                    notion_mode=notion_mode,
                    structured_mode=structured_mode,
                )
                self.assertEqual(settings.enabled, expected_enabled)
                self.assertEqual(
                    settings.shadow_enabled,
                    expected_shadow_enabled,
                )

    def test_remote_configuration_rejects_unsafe_or_missing_values(self) -> None:
        cases = (
            {
                "BOXER_COMPANY_API_NOTION_MODE": "remote",
            },
            {
                "BOXER_COMPANY_API_STRUCTURED_MODE": "remote",
            },
            {
                "BOXER_COMPANY_API_STRUCTURED_MODE": "invalid",
            },
            {
                "BOXER_COMPANY_API_PLAYBOOK_MODE": "remote",
            },
            {
                "BOXER_COMPANY_API_BARCODE_RESIDUAL_MODE": "remote",
            },
            {
                "BOXER_COMPANY_API_BARCODE_TIMELINE_MODE": "remote",
            },
            {
                "BOXER_COMPANY_API_BARCODE_FREEFORM_MODE": "remote",
            },
            {
                "BOXER_COMPANY_API_FREEFORM_MODE": "remote",
            },
            {
                "BOXER_COMPANY_API_DEVICE_DETAIL_MODE": "remote",
            },
            {
                "BOXER_COMPANY_API_WEEKLY_SUMMARY_MODE": "remote",
            },
            {
                "BOXER_COMPANY_API_OPERATIONS_MODE": "remote",
            },
            {
                "BOXER_COMPANY_API_BASE_URL": (
                    "http://10.40.102.50:8010"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                "BOXER_COMPANY_API_STRUCTURED_MODE": "shadow",
                "BOXER_COMPANY_API_STRUCTURED_FALLBACK_ENABLED": "invalid",
            },
            {
                "BOXER_COMPANY_API_BASE_URL": (
                    "http://10.40.102.50:8010"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                "BOXER_COMPANY_API_PLAYBOOK_MODE": "shadow",
                "BOXER_COMPANY_API_PLAYBOOK_FALLBACK_ENABLED": "invalid",
            },
            {
                "BOXER_COMPANY_API_BASE_URL": (
                    "http://10.40.102.50:8010"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                "BOXER_COMPANY_API_BARCODE_RESIDUAL_MODE": "shadow",
                "BOXER_COMPANY_API_BARCODE_RESIDUAL_FALLBACK_ENABLED": "invalid",
            },
            {
                "BOXER_COMPANY_API_BASE_URL": (
                    "http://10.40.102.50:8010"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                "BOXER_COMPANY_API_BARCODE_TIMELINE_MODE": "shadow",
                "BOXER_COMPANY_API_BARCODE_TIMELINE_FALLBACK_ENABLED": "invalid",
            },
            {
                "BOXER_COMPANY_API_BASE_URL": (
                    "http://10.40.102.50:8010"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                "BOXER_COMPANY_API_BARCODE_FREEFORM_MODE": "shadow",
                "BOXER_COMPANY_API_BARCODE_FREEFORM_FALLBACK_ENABLED": "invalid",
            },
            {
                "BOXER_COMPANY_API_BASE_URL": (
                    "http://10.40.102.50:8010"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                "BOXER_COMPANY_API_FREEFORM_MODE": "shadow",
                "BOXER_COMPANY_API_FREEFORM_FALLBACK_ENABLED": "invalid",
            },
            {
                "BOXER_COMPANY_API_BASE_URL": (
                    "http://10.40.102.50:8010"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                "BOXER_COMPANY_API_DEVICE_DETAIL_MODE": "shadow",
                "BOXER_COMPANY_API_DEVICE_DETAIL_FALLBACK_ENABLED": "invalid",
            },
            {
                "BOXER_COMPANY_API_BASE_URL": (
                    "http://10.40.102.50:8010"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                "BOXER_COMPANY_API_WEEKLY_SUMMARY_MODE": "shadow",
                "BOXER_COMPANY_API_WEEKLY_SUMMARY_FALLBACK_ENABLED": "invalid",
            },
            {
                "BOXER_COMPANY_API_NOTION_MODE": "remote",
                "BOXER_COMPANY_API_BASE_URL": (
                    "http://public.example.com:8010"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
            },
            {
                "BOXER_COMPANY_API_NOTION_MODE": "remote",
                "BOXER_COMPANY_API_BASE_URL": (
                    "https://user:password@api.example.com"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
            },
            {
                "BOXER_COMPANY_API_NOTION_MODE": "remote",
                "BOXER_COMPANY_API_BASE_URL": (
                    "https://api.example.com/path"
                ),
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
            },
        )
        for env in cases:
            with self.subTest(env_keys=sorted(env)):
                with self.assertRaises(CompanyApiContractError):
                    load_company_api_client_settings(env)


class CompanyApiClientContractTests(unittest.TestCase):
    def test_serializes_valid_route_group_and_rejects_unknown_group(
        self,
    ) -> None:
        request = _request()
        session = _FakeSession(
            *(
                _success_response(request.request_id)
                for _ in range(6)
            )
        )

        for route_group in (
            "knowledge",
            "freeform",
            "health",
            "fun",
            "device_detail",
            "operations",
        ):
            _client(session).answer(
                request,
                route_group=route_group,
            )

        self.assertEqual(
            [call["json"]["routeGroup"] for call in session.calls],
            [
                "knowledge",
                "freeform",
                "health",
                "fun",
                "device_detail",
                "operations",
            ],
        )
        invalid_session = _FakeSession()
        with self.assertRaises(CompanyApiContractError):
            _client(invalid_session).answer(
                request,
                route_group="unsafe",  # type: ignore[arg-type]
            )
        self.assertEqual(invalid_session.calls, [])

    def test_operations_use_the_dedicated_long_read_timeout(self) -> None:
        request = _request()
        session = _FakeSession(_success_response(request.request_id))
        settings = replace(
            _settings(),
            operations_mode="remote",
            operations_read_timeout_sec=701,
        )

        _client(session, settings=settings).answer(
            request,
            route_group="operations",
        )

        self.assertEqual(session.calls[0]["timeout"], (2.0, 701))

    def test_serializes_typed_device_health_alert_operation(self) -> None:
        request = _request(
            metadata={
                "channel_id": "C1",
                "device_name": "MB2-C00419",
                "operation_action": {
                    "name": "device_health_alert_contact_hospital",
                    "phase": "execute",
                    "target": {
                        "hospital_seq": 7,
                        "hospital_name": "테스트병원",
                        "room_name": "2진료실",
                        "device_name": "MB2-C00419",
                        "issue": "캡처보드 연결 확인 필요",
                        "alert_category": "video_signal",
                        "problem_components": ["캡처보드"],
                    },
                    "sms": {
                        "phone_number": "010-1234-5678",
                        "message": "연결 상태를 확인해 주세요.",
                    },
                },
            }
        )
        session = _FakeSession(_success_response(request.request_id))

        _client(session).answer(request, route_group="operations")

        self.assertEqual(
            session.calls[0]["json"]["operationAction"],
            {
                "name": "device_health_alert_contact_hospital",
                "phase": "execute",
                "target": {
                    "hospitalSeq": 7,
                    "hospitalName": "테스트병원",
                    "roomName": "2진료실",
                    "deviceName": "MB2-C00419",
                    "issue": "캡처보드 연결 확인 필요",
                    "alertCategory": "video_signal",
                    "problemComponents": ["캡처보드"],
                },
                "sms": {
                    "phoneNumber": "010-1234-5678",
                    "message": "연결 상태를 확인해 주세요.",
                },
            },
        )

    def test_operation_action_requires_operations_group(self) -> None:
        request = _request(
            metadata={"operation_action": {"name": "invalid"}}
        )
        session = _FakeSession()

        with self.assertRaises(CompanyApiContractError):
            _client(session).answer(request, route_group="structured")

        self.assertEqual(session.calls, [])

    def test_deserializes_allowlisted_operation_receipts(self) -> None:
        request = _request()
        preparation = _success_payload(request.request_id)
        preparation["operationResult"] = {
            "kind": "sms_contact_preparation",
            "deliveryScope": "requester",
            "phoneNumber": "01012345678",
            "message": "케이블을 확인해 주세요.",
            "templateId": "captureboard_disconnected",
            "target": {
                "hospital": "테스트병원",
                "room": "2진료실",
                "device": "MB2-C00419",
                "components": ["캡처보드"],
                "issue": "캡처보드 연결 확인 필요",
            },
        }
        delivery = _success_payload(request.request_id)
        delivery["operationResult"] = {
            "kind": "sms_delivery",
            "provider": "solapi",
            "deliveryStatus": "accepted",
            "groupId": "GROUP-1",
            "messageId": "MESSAGE-1",
            "acceptedAt": "2026-08-14T12:34:56+09:00",
            "target": {
                "hospital": "테스트병원",
                "room": "2진료실",
                "device": "MB2-C00419",
                "components": ["캡처보드"],
                "issue": "캡처보드 연결 확인 필요",
            },
        }
        session = _FakeSession(
            _FakeResponse(200, preparation, "application/json"),
            _FakeResponse(200, delivery, "application/json"),
        )

        prepared = _client(session).answer(request, route_group="operations")
        sent = _client(session).answer(request, route_group="operations")

        self.assertEqual(
            prepared.operation_result["phoneNumber"],
            "01012345678",
        )
        self.assertEqual(sent.operation_result["groupId"], "GROUP-1")

    def test_rejects_unknown_or_sensitive_operation_receipt_shape(self) -> None:
        request = _request()
        payload = _success_payload(request.request_id)
        payload["operationResult"] = {
            "kind": "sms_delivery",
            "provider": "solapi",
            "deliveryStatus": "accepted",
            "groupId": "GROUP-1",
            "messageId": "MESSAGE-1",
            "acceptedAt": "2026-08-14T12:34:56+09:00",
            "target": {
                "hospital": "테스트병원",
                "room": "2진료실",
                "device": "MB2-C00419",
                "components": [],
                "issue": "확인 필요",
            },
            "phoneNumber": "01012345678",
        }
        session = _FakeSession(
            _FakeResponse(200, payload, "application/json")
        )

        with self.assertRaises(CompanyApiContractError):
            _client(session).answer(request, route_group="operations")

    def test_serializes_headers_scope_and_success_result(self) -> None:
        request = _request(
            metadata={
                "barcode": "12345678901",
                "hospital_name": "테스트 병원",
                "room_name": "검사실",
                "device_name": "MB2-C00419",
                "channel_id": "C1",
                "followup_kind": "barcode_log",
                "role": "must-not-cross",
            }
        )
        session = _FakeSession(
            _success_response(request.request_id)
        )

        result = _client(session).answer(request)

        self.assertEqual(result.route, "company_notion_qa")
        self.assertEqual(result.outcome, "answered")
        self.assertEqual(len(result.messages), 1)
        self.assertEqual(len(result.sources), 1)
        self.assertEqual(len(session.calls), 1)
        call = session.calls[0]
        self.assertEqual(
            call["url"],
            (
                "http://127.0.0.1:8010"
                "/internal/v1/assistant/turns"
            ),
        )
        self.assertEqual(
            call["headers"]["Authorization"],
            f"Bearer {_TOKEN}",
        )
        self.assertEqual(
            call["headers"]["X-Request-ID"],
            request.request_id,
        )
        self.assertEqual(
            call["headers"]["traceparent"],
            _TRACEPARENT,
        )
        self.assertFalse(call["allow_redirects"])
        self.assertEqual(call["timeout"], (2.0, 90.0))
        self.assertEqual(
            call["json"]["scope"],
            {
                "barcode": "12345678901",
                "hospitalName": "테스트 병원",
                "roomName": "검사실",
                "deviceName": "MB2-C00419",
                "channelContextId": "C1",
                "followupKind": "barcode_log",
            },
        )
        self.assertNotIn("role", call["json"]["scope"])

    def test_context_uses_the_newest_entries_within_both_budgets(self) -> None:
        entries = tuple(
            {
                "kind": "message",
                "source": "slack",
                "author_id": "U1",
                "text": f"{index:02d}-" + ("x" * 597),
                "created_at": f"17853120{index:02d}.000001",
            }
            for index in range(14)
        )
        request = _request(context_entries=entries)
        session = _FakeSession(
            _success_response(request.request_id)
        )

        _client(session).answer(request)

        serialized = session.calls[0]["json"]["contextEntries"]
        self.assertEqual(len(serialized), 9)
        self.assertEqual(
            sum(len(entry["text"]) for entry in serialized),
            5_000,
        )
        self.assertTrue(serialized[0]["text"].startswith("05-"))
        self.assertEqual(len(serialized[0]["text"]), 200)
        self.assertTrue(serialized[-1]["text"].startswith("13-"))

    def test_invalid_request_or_trace_context_never_calls_http(self) -> None:
        invalid_requests = (
            replace(_request(), channel="web"),
            replace(_request(), actor_id=None),
        )
        for request in invalid_requests:
            with self.subTest(channel=request.channel):
                session = _FakeSession()
                with self.assertRaises(CompanyApiContractError):
                    _client(session).answer(request)
                self.assertEqual(session.calls, [])

        session = _FakeSession()
        with self.assertRaises(CompanyApiContractError):
            _client(
                session,
                traceparent=(
                    "00-00000000000000000000000000000000-"
                    "0000000000000000-01"
                ),
            ).answer(_request())
        self.assertEqual(session.calls, [])

    def test_connect_retry_preserves_request_and_trace_ids(self) -> None:
        request = _request()
        sleeps: list[float] = []
        session = _FakeSession(
            requests.exceptions.ConnectTimeout(),
            _success_response(request.request_id),
        )

        result = _client(
            session,
            settings=_settings(max_retries=1),
            sleep=sleeps.append,
        ).answer(request)

        self.assertEqual(result.outcome, "answered")
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(sleeps, [0.1])
        self.assertEqual(
            {
                call["headers"]["X-Request-ID"]
                for call in session.calls
            },
            {request.request_id},
        )
        self.assertEqual(
            {
                call["headers"]["traceparent"]
                for call in session.calls
            },
            {_TRACEPARENT},
        )

    def test_mutating_turn_never_retries_transport_or_503(
        self,
    ) -> None:
        # mutation 수행 여부가 불명확한 모든 실패와 명시적
        # 503도 두 번째 POST로 이어지지 않는다.
        request = replace(
            _request(),
            question="MB2-C00419 장비 정보",
        )
        cases = (
            (
                requests.exceptions.ConnectTimeout(),
                CompanyApiAvailabilityError,
            ),
            (
                requests.exceptions.ReadTimeout(),
                CompanyApiAmbiguousTimeoutError,
            ),
            (
                requests.exceptions.ConnectionError(
                    "reset-after-send"
                ),
                CompanyApiAvailabilityError,
            ),
            (
                _problem_response(
                    request.request_id,
                    status=503,
                    code="service_not_ready",
                    retryable=True,
                ),
                CompanyApiAvailabilityError,
            ),
        )
        for route_group in ("device_detail", "operations"):
            for first_result, error_type in cases:
                with self.subTest(
                    route_group=route_group,
                    result_type=type(first_result).__name__,
                ):
                    session = _FakeSession(
                        first_result,
                        _success_response(request.request_id),
                    )

                    with self.assertRaises(error_type):
                        _client(
                            session,
                            settings=_settings(max_retries=2),
                        ).answer(
                            request,
                            route_group=route_group,
                        )

                    self.assertEqual(len(session.calls), 1)

    def test_connection_reset_is_not_retried(self) -> None:
        session = _FakeSession(
            requests.exceptions.ConnectionError("reset-after-send"),
            _success_response(_request().request_id),
        )

        with self.assertRaises(CompanyApiAvailabilityError):
            _client(
                session,
                settings=_settings(max_retries=2),
            ).answer(_request())

        self.assertEqual(len(session.calls), 1)

    def test_default_sessions_are_isolated_per_caller_thread(self) -> None:
        sessions: list[_FakeSession] = []
        errors: list[Exception] = []

        def build_session() -> _FakeSession:
            session = _FakeSession(
                _success_response(_request().request_id)
            )
            sessions.append(session)
            return session

        with patch(
            "boxer_company_adapter_slack.company_api_client."
            "requests.Session",
            side_effect=build_session,
        ):
            client = CompanyAssistantApiClient(
                _settings(),
                traceparent_factory=lambda: _TRACEPARENT,
            )
            client.answer(_request())

            def call_from_worker() -> None:
                try:
                    client.answer(_request())
                except Exception as exc:
                    errors.append(exc)

            worker = threading.Thread(target=call_from_worker)
            worker.start()
            worker.join(timeout=1)

        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(sessions), 2)
        self.assertTrue(all(session.trust_env is False for session in sessions))

    def test_service_not_ready_is_the_only_http_retry(self) -> None:
        request = _request()
        session = _FakeSession(
            _problem_response(
                request.request_id,
                status=503,
                code="service_not_ready",
                retryable=True,
            ),
            _success_response(request.request_id),
        )

        result = _client(
            session,
            settings=_settings(max_retries=1),
        ).answer(request)

        self.assertEqual(result.outcome, "answered")
        self.assertEqual(len(session.calls), 2)

    def test_read_timeout_is_ambiguous_and_is_not_retried(self) -> None:
        session = _FakeSession(
            requests.exceptions.ReadTimeout()
        )

        with self.assertRaises(
            CompanyApiAmbiguousTimeoutError
        ):
            _client(
                session,
                settings=_settings(max_retries=2),
            ).answer(_request())

        self.assertEqual(len(session.calls), 1)

    def test_auth_and_validation_problems_do_not_retry_or_fallback(self) -> None:
        cases = (
            (401, "authentication_failed", CompanyApiPolicyError),
            (403, "caller_not_allowed", CompanyApiPolicyError),
            (400, "invalid_request_id", CompanyApiContractError),
            (422, "validation_failed", CompanyApiContractError),
        )
        for status, code, error_type in cases:
            with self.subTest(status=status):
                request = _request()
                session = _FakeSession(
                    _problem_response(
                        request.request_id,
                        status=status,
                        code=code,
                        retryable=False,
                    ),
                    _success_response(request.request_id),
                )
                with self.assertRaises(error_type):
                    _client(
                        session,
                        settings=_settings(max_retries=2),
                    ).answer(request)
                self.assertEqual(len(session.calls), 1)

    def test_domain_denial_is_a_successful_result(self) -> None:
        request = _request()
        response = _success_response(
            request.request_id,
            outcome="denied",
        )

        result = _client(_FakeSession(response)).answer(request)

        self.assertEqual(result.outcome, "denied")

    def test_gateway_error_without_problem_body_is_availability(self) -> None:
        response = _FakeResponse(
            status_code=502,
            payload=ValueError("gateway-secret-body"),
            content_type="text/html",
            raw_content=b"gateway-secret-body",
        )

        with self.assertRaises(CompanyApiAvailabilityError) as raised:
            _client(_FakeSession(response)).answer(_request())

        self.assertEqual(raised.exception.status, 502)
        self.assertEqual(
            raised.exception.code,
            "server_response_invalid",
        )
        self.assertNotIn(
            "gateway-secret-body",
            str(raised.exception),
        )

    def test_response_contract_rejects_mismatch_actions_and_unsafe_sources(
        self,
    ) -> None:
        request = _request()
        cases: list[dict[str, Any]] = []

        mismatched = _success_payload("OTHER-REQUEST")
        cases.append(mismatched)
        action = _success_payload(request.request_id)
        action["suggestedAction"] = {"action": "unsafe"}
        cases.append(action)
        extra = _success_payload(request.request_id)
        extra["unexpected"] = "field"
        cases.append(extra)
        bad_outcome = _success_payload(request.request_id)
        bad_outcome["outcome"] = "unknown"
        cases.append(bad_outcome)
        signed_source = _success_payload(request.request_id)
        signed_source["sources"][0]["uri"] = (
            "https://storage.example/file?sig=must-not-leak"
        )
        cases.append(signed_source)
        requester_format = _success_payload(request.request_id)
        requester_format["messages"][0]["format"] = "slack"
        cases.append(requester_format)
        too_many_messages = _success_payload(request.request_id)
        too_many_messages["messages"] *= 9
        cases.append(too_many_messages)
        too_many_sources = _success_payload(request.request_id)
        too_many_sources["sources"] *= 21
        cases.append(too_many_sources)
        oversized_message = _success_payload(request.request_id)
        oversized_message["messages"][0]["body"] = "x" * 30_001
        cases.append(oversized_message)

        for payload in cases:
            with self.subTest(keys=sorted(payload)):
                session = _FakeSession(
                    _FakeResponse(
                        status_code=200,
                        payload=payload,
                        content_type="application/json",
                    )
                )
                with self.assertRaises(CompanyApiContractError):
                    _client(session).answer(request)

    def test_redirect_and_invalid_media_type_are_never_followed(self) -> None:
        request = _request()
        for response in (
            _FakeResponse(
                status_code=307,
                payload={},
                content_type="text/html",
            ),
            _FakeResponse(
                status_code=200,
                payload=_success_payload(request.request_id),
                content_type="text/plain",
            ),
        ):
            with self.subTest(status=response.status_code):
                session = _FakeSession(response)
                with self.assertRaises(CompanyApiContractError):
                    _client(session).answer(request)
                self.assertFalse(
                    session.calls[0]["allow_redirects"]
                )

    def test_problem_code_cannot_inject_log_content(self) -> None:
        request = _request()
        forged_code = "validation_failed\nFORGED-LOG-LINE"
        response = _problem_response(
            request.request_id,
            status=400,
            code=forged_code,
            retryable=False,
        )

        with self.assertRaises(CompanyApiContractError) as raised:
            _client(_FakeSession(response)).answer(request)

        self.assertNotIn(
            "FORGED-LOG-LINE",
            str(raised.exception),
        )

    def test_error_and_logs_never_include_sensitive_transport_content(
        self,
    ) -> None:
        logger = logging.getLogger(
            f"{__name__}.safe-log"
        )
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        handler = _CollectingHandler()
        logger.addHandler(handler)
        request = _request()
        secret_answer = "secret-answer-body"
        response = _problem_response(
            request.request_id,
            status=500,
            code="internal_error",
            retryable=True,
        )
        response.raw_content = (
            f"{_TOKEN}|{_QUESTION}|{secret_answer}".encode()
        )

        try:
            with self.assertRaises(
                CompanyApiAvailabilityError
            ) as raised:
                _client(
                    _FakeSession(response),
                    logger=logger,
                ).answer(request)
        finally:
            logger.removeHandler(handler)

        diagnostics = "\n".join(
            [str(raised.exception), *handler.messages]
        )
        self.assertNotIn(_TOKEN, diagnostics)
        self.assertNotIn(_QUESTION, diagnostics)
        self.assertNotIn(secret_answer, diagnostics)


if __name__ == "__main__":
    unittest.main()
