from __future__ import annotations

import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

from boxer_company.routers.s3_domain import (
    _fetch_s3_device_log_lines,
)


class _ForbiddenHeadClient:
    def head_object(self, **kwargs: object) -> dict[str, object]:
        raise ClientError(
            {
                "Error": {"Code": "AccessDenied"},
                "ResponseMetadata": {"HTTPStatusCode": 403},
            },
            "HeadObject",
        )


class S3DeviceLogLookupTests(unittest.TestCase):
    def test_get_only_api_treats_ambiguous_head_403_as_missing(self) -> None:
        # ListBucket 없는 role은 없는 key도 403으로 받으므로 API에서만
        # 명시적으로 missing으로 정규화한다.
        with (
            patch(
                "boxer_company.routers.s3_domain.s.S3_LOG_BUCKET",
                "mmb-log-prod-kr",
            ),
            patch(
                "boxer_company.routers.s3_domain."
                "cs.BOXER_COMPANY_API_S3_HEAD_403_AS_MISSING",
                True,
            ),
        ):
            result = _fetch_s3_device_log_lines(
                _ForbiddenHeadClient(),
                "MB2-T00001",
                "2026-08-05",
            )

        self.assertFalse(result["found"])
        self.assertEqual(
            result["lookup_status"],
            "not_found_or_forbidden",
        )

    def test_slack_default_keeps_head_403_as_dependency_error(self) -> None:
        # Slack local은 실제 IAM 장애를 숨기지 않고 기존처럼 올려 보낸다.
        with (
            patch(
                "boxer_company.routers.s3_domain.s.S3_LOG_BUCKET",
                "mmb-log-prod-kr",
            ),
            patch(
                "boxer_company.routers.s3_domain."
                "cs.BOXER_COMPANY_API_S3_HEAD_403_AS_MISSING",
                False,
            ),
        ):
            with self.assertRaises(ClientError):
                _fetch_s3_device_log_lines(
                    _ForbiddenHeadClient(),
                    "MB2-T00001",
                    "2026-08-05",
                )


if __name__ == "__main__":
    unittest.main()
