from typing import Any

import boto3
from botocore.config import Config as BotoConfig

from boxer.core import settings as s


def _build_s3_client() -> Any:
    timeout_sec = max(1, s.S3_QUERY_TIMEOUT_SEC)
    config = BotoConfig(
        region_name=s.AWS_REGION,
        connect_timeout=timeout_sec,
        read_timeout=timeout_sec,
        retries={"max_attempts": 2, "mode": "standard"},
    )
    return boto3.client("s3", region_name=s.AWS_REGION, config=config)
