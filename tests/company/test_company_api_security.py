from __future__ import annotations

import unittest

from boxer_company_api.security import (
    validate_company_api_runtime_security,
)


class CompanyApiRuntimeSecurityTests(unittest.TestCase):
    def test_local_runtime_can_use_normal_sdk_credential_chain(self) -> None:
        # 개발 머신에서는 profile/shared credential 등 SDK 기본 체인을
        # 그대로 쓸 수 있고, EC2 운영 런타임에서만 instance role을 강제한다.
        validate_company_api_runtime_security(
            env={"AWS_PROFILE": "local-development"},
            is_ec2=False,
        )

    def test_ec2_runtime_rejects_static_aws_environment_without_leaking_it(self) -> None:
        with self.assertRaises(RuntimeError) as raised:
            validate_company_api_runtime_security(
                env={
                    "AWS_ACCESS_KEY_ID": "must-not-leak",
                    "AWS_SECRET_ACCESS_KEY": "must-not-leak",
                },
                is_ec2=True,
            )

        self.assertNotIn("must-not-leak", str(raised.exception))


if __name__ == "__main__":
    unittest.main()
