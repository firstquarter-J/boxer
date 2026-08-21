from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from boxer_company.routers import device_ssh_security as security


class DeviceSshSecurityTests(unittest.TestCase):
    def test_slack_local_uses_mda_endpoint_and_auto_add_policy(self) -> None:
        client = Mock()
        fake_paramiko = SimpleNamespace(
            AutoAddPolicy=Mock(return_value="auto-policy"),
        )

        connect_host = security._prepare_device_ssh_client(
            client,
            reported_host="remotes.example",
            port=12345,
            paramiko_module=fake_paramiko,
        )

        self.assertEqual(connect_host, "remotes.example")
        client.set_missing_host_key_policy.assert_called_once_with(
            "auto-policy"
        )

    def test_api_uses_same_mda_endpoint_and_auto_add_policy(self) -> None:
        client = Mock()
        fake_paramiko = SimpleNamespace(
            AutoAddPolicy=Mock(return_value="auto-policy"),
        )

        with security.company_api_device_ssh_context():
            connect_host = security._prepare_device_ssh_client(
                client,
                reported_host="remotes.example",
                port=12345,
                paramiko_module=fake_paramiko,
            )

        self.assertEqual(connect_host, "remotes.example")
        client.set_missing_host_key_policy.assert_called_once_with(
            "auto-policy"
        )

    def test_rejects_invalid_mda_endpoint_before_setting_policy(self) -> None:
        client = Mock()
        fake_paramiko = SimpleNamespace(
            AutoAddPolicy=Mock(return_value="auto-policy"),
        )

        invalid_endpoints = (
            ("", 43123),
            ("remotes.example", 0),
            ("remotes.example", 65536),
        )
        for host, port in invalid_endpoints:
            with self.subTest(host=host, port=port):
                with self.assertRaisesRegex(
                    security.DeviceSshSecurityError,
                    "device_ssh_endpoint_invalid",
                ):
                    security._prepare_device_ssh_client(
                        client,
                        reported_host=host,
                        port=port,
                        paramiko_module=fake_paramiko,
                    )

        client.set_missing_host_key_policy.assert_not_called()

    def test_api_rejects_second_ssh_open_before_transport(self) -> None:
        with security.company_api_device_ssh_context() as state:
            security._mark_company_api_device_ssh_open_attempted()
            with self.assertRaisesRegex(
                security.DeviceSshSecurityError,
                "device_ssh_open_budget_exhausted",
            ):
                security._mark_company_api_device_ssh_open_attempted()

        self.assertTrue(state.mutation_attempted)
        self.assertTrue(state.open_attempted)

    def test_api_automation_allows_one_open_for_each_device(self) -> None:
        with security.company_api_device_ssh_context(
            per_device_open_budget=True,
        ) as state:
            security._mark_company_api_device_ssh_open_attempted(
                "MB2-C00419"
            )
            security._mark_company_api_device_ssh_open_attempted(
                "MB2-C00420"
            )
            with self.assertRaisesRegex(
                security.DeviceSshSecurityError,
                "device_ssh_open_budget_exhausted",
            ):
                security._mark_company_api_device_ssh_open_attempted(
                    "mb2-c00419"
                )

        self.assertEqual(
            state.opened_device_names,
            {"mb2-c00419", "mb2-c00420"},
        )
        self.assertTrue(state.mutation_attempted)

if __name__ == "__main__":
    unittest.main()
