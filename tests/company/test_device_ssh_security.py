from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from boxer_company import settings as company_settings
from boxer_company.routers import device_ssh_security as security


class DeviceSshSecurityTests(unittest.TestCase):
    def setUp(self) -> None:
        security._ENDPOINT_DEVICE_NAMES.clear()

    def test_slack_local_keeps_existing_endpoint_policy(self) -> None:
        client = Mock()
        fake_paramiko = SimpleNamespace(
            AutoAddPolicy=Mock(return_value="auto-policy"),
        )

        connect_host = security._prepare_device_ssh_client(
            client,
            device_name="MB2-C00419",
            reported_host="remotes.example",
            port=12345,
            paramiko_module=fake_paramiko,
        )

        self.assertEqual(connect_host, "remotes.example")
        client.set_missing_host_key_policy.assert_called_once_with(
            "auto-policy"
        )

    def test_api_requires_exact_policy_before_sending_credentials(self) -> None:
        client = Mock()
        fake_paramiko = SimpleNamespace()

        with security.company_api_device_ssh_context():
            with self.assertRaisesRegex(
                security.DeviceSshSecurityError,
                "device_ssh_policy_unavailable",
            ):
                security._prepare_device_ssh_client(
                    client,
                    device_name="MB2-C00419",
                    reported_host="remotes.example",
                    port=12345,
                    paramiko_module=fake_paramiko,
                )

        client.set_missing_host_key_policy.assert_not_called()

    def test_api_maps_pinned_device_key_to_private_dynamic_endpoint(self) -> None:
        pinned_key = object()

        class FakeHostKeys:
            def load(self, path: str) -> None:
                self.path = path

            def lookup(self, name: str) -> dict[str, object]:
                self.name = name
                return {"ssh-ed25519": pinned_key}

        fake_paramiko = SimpleNamespace(
            HostKeys=FakeHostKeys,
            RejectPolicy=Mock(return_value="reject-policy"),
        )
        endpoint_host_keys = Mock()
        client = Mock()
        client.get_host_keys.return_value = endpoint_host_keys

        with (
            patch.object(
                company_settings,
                "BOXER_COMPANY_API_DEVICE_SSH_ALLOWED_HOSTS",
                ("remotes.example",),
            ),
            patch.object(
                company_settings,
                "BOXER_COMPANY_API_DEVICE_SSH_CONNECT_HOST",
                "10.40.21.27",
            ),
            patch.object(
                company_settings,
                "BOXER_COMPANY_API_DEVICE_SSH_KNOWN_HOSTS_PATH",
                "/etc/boxer-company-api/device_known_hosts",
            ),
            patch.object(security, "_validate_known_hosts_path"),
            patch.object(
                security,
                "_load_known_hosts_entries",
                return_value=[
                    (
                        "MB2-C00419",
                        "ssh-ed25519",
                        "AAAAC3NzaC1lZDI1NTE5AAAAITest",
                    )
                ],
            ),
            security.company_api_device_ssh_context(),
        ):
            connect_host = security._prepare_device_ssh_client(
                client,
                device_name="MB2-C00419",
                reported_host="remotes.example",
                port=43123,
                paramiko_module=fake_paramiko,
            )

        self.assertEqual(connect_host, "10.40.21.27")
        endpoint_host_keys.add.assert_called_once_with(
            "[10.40.21.27]:43123",
            "ssh-ed25519",
            pinned_key,
        )
        client.set_missing_host_key_policy.assert_called_once_with(
            "reject-policy"
        )

    def test_mda_endpoint_registry_keeps_device_identity(self) -> None:
        security._register_mda_ssh_endpoint_device(
            "MB2-C00419",
            "remotes.example",
            43123,
        )

        self.assertEqual(
            security._resolve_mda_ssh_endpoint_device(
                "REMOTES.EXAMPLE",
                "43123",
            ),
            "MB2-C00419",
        )


if __name__ == "__main__":
    unittest.main()
