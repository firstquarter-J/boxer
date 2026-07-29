from __future__ import annotations

import json
import unittest

from boxer_company_api.settings import load_company_api_settings


class CompanyApiSettingsTests(unittest.TestCase):
    def test_loads_server_side_caller_registry(self) -> None:
        token = "t" * 48
        settings = load_company_api_settings(
            {
                "BOXER_COMPANY_API_HOST": "127.0.0.1",
                "BOXER_COMPANY_API_PORT": "8010",
                "BOXER_COMPANY_API_CALLERS_JSON": json.dumps(
                    [
                        {
                            "callerId": "slack-prod",
                            "token": token,
                            "tenantIds": ["TENANT-1"],
                            "channels": ["slack"],
                            "actorIds": ["*"],
                            "allowAnonymousActor": False,
                            "capabilities": ["assistant.turn.read"],
                        }
                    ]
                ),
            }
        )

        self.assertIsNone(settings.configuration_error)
        self.assertEqual(settings.host, "127.0.0.1")
        self.assertEqual(settings.port, 8010)
        self.assertEqual(settings.callers[0].caller_id, "slack-prod")
        self.assertEqual(settings.callers[0].token, token)
        self.assertEqual(settings.callers[0].channels, {"slack"})
        self.assertNotIn(token, repr(settings))

    def test_missing_or_invalid_registry_fails_closed_without_secret_detail(self) -> None:
        cases = (
            ({}, "caller_registry_missing"),
            (
                {
                    "BOXER_COMPANY_API_CALLERS_JSON": (
                        '[{"callerId":"slack","token":"secret-value"}]'
                    )
                },
                "caller_registry_invalid",
            ),
        )
        for env, expected_code in cases:
            with self.subTest(expected_code=expected_code):
                settings = load_company_api_settings(env)

                self.assertEqual(settings.callers, ())
                self.assertEqual(settings.configuration_error, expected_code)
                self.assertNotIn("secret-value", str(settings))

    def test_duplicate_tokens_fail_closed(self) -> None:
        token = "d" * 48
        caller = {
            "token": token,
            "tenantIds": ["TENANT-1"],
            "channels": ["slack"],
            "actorIds": ["*"],
            "capabilities": ["assistant.turn.read"],
        }
        settings = load_company_api_settings(
            {
                "BOXER_COMPANY_API_CALLERS_JSON": json.dumps(
                    [
                        {"callerId": "one", **caller},
                        {"callerId": "two", **caller},
                    ]
                )
            }
        )

        self.assertEqual(settings.callers, ())
        self.assertEqual(
            settings.configuration_error,
            "caller_registry_invalid",
        )

    def test_wildcard_or_missing_turn_capability_fails_closed(self) -> None:
        base_caller = {
            "callerId": "slack-prod",
            "token": "c" * 48,
            "tenantIds": ["TENANT-1"],
            "channels": ["slack"],
            "actorIds": ["*"],
        }
        for capabilities in (["*"], ["unrelated.read"]):
            with self.subTest(capabilities=capabilities):
                settings = load_company_api_settings(
                    {
                        "BOXER_COMPANY_API_CALLERS_JSON": json.dumps(
                            [
                                {
                                    **base_caller,
                                    "capabilities": capabilities,
                                }
                            ]
                        )
                    }
                )

                self.assertEqual(settings.callers, ())
                self.assertEqual(
                    settings.configuration_error,
                    "caller_registry_invalid",
                )

    def test_anonymous_company_runtime_caller_is_not_configurable_yet(
        self,
    ) -> None:
        settings = load_company_api_settings(
            {
                "BOXER_COMPANY_API_CALLERS_JSON": json.dumps(
                    [
                        {
                            "callerId": "web-anonymous",
                            "token": "a" * 48,
                            "tenantIds": ["TENANT-1"],
                            "channels": ["web"],
                            "actorIds": ["*"],
                            "allowAnonymousActor": True,
                            "capabilities": [
                                "assistant.turn.read"
                            ],
                        }
                    ]
                )
            }
        )

        self.assertEqual(settings.callers, ())
        self.assertEqual(
            settings.configuration_error,
            "caller_registry_invalid",
        )


if __name__ == "__main__":
    unittest.main()
