import unittest
from unittest.mock import patch

from boxer_company_adapter_slack import company
from boxer_company_adapter_slack.startup_guard import (
    _find_forbidden_ec2_aws_env_keys,
    _validate_ec2_runtime_aws_env,
)


class StartupGuardTests(unittest.TestCase):
    def test_finds_only_forbidden_aws_env_keys(self) -> None:
        env = {
            "AWS_REGION": "ap-northeast-2",
            "AWS_ACCESS_KEY_ID": "test-access-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret-key",
            "AWS_PROFILE": "",
        }

        self.assertEqual(
            _find_forbidden_ec2_aws_env_keys(env),
            ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"],
        )

    def test_allows_non_ec2_runtime_even_with_static_aws_env(self) -> None:
        env = {
            "AWS_ACCESS_KEY_ID": "test-access-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret-key",
        }

        _validate_ec2_runtime_aws_env(env=env, is_ec2=False)

    def test_allows_ec2_runtime_when_only_region_is_set(self) -> None:
        _validate_ec2_runtime_aws_env(
            env={"AWS_REGION": "ap-northeast-2"},
            is_ec2=True,
        )

    def test_blocks_ec2_runtime_when_static_aws_env_exists(self) -> None:
        env = {
            "AWS_ACCESS_KEY_ID": "test-access-key",
            "AWS_SECRET_ACCESS_KEY": "test-secret-key",
            "AWS_PROFILE": "prod",
        }

        with self.assertRaisesRegex(
            RuntimeError,
            "AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_PROFILE",
        ):
            _validate_ec2_runtime_aws_env(env=env, is_ec2=True)

    def test_company_create_app_runs_startup_guard_first(self) -> None:
        with patch.object(
            company,
            "_validate_ec2_runtime_aws_env",
            side_effect=RuntimeError("startup guard fired"),
        ) as guard:
            with self.assertRaisesRegex(RuntimeError, "startup guard fired"):
                company.create_app()

        guard.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
