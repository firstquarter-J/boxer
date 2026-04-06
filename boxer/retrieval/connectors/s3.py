from typing import Any

from boxer.core import settings as s


def _load_boto3_components() -> tuple[Any, Any]:
    try:
        import boto3
        from botocore.config import Config as BotoConfig
    except ModuleNotFoundError as exc:
        raise RuntimeError('S3 connector를 쓰려면 `pip install "boxer[s3]"`가 필요해') from exc
    return boto3, BotoConfig


def _build_s3_client() -> Any:
    boto3, BotoConfig = _load_boto3_components()
    timeout_sec = max(1, s.S3_QUERY_TIMEOUT_SEC)
    config = BotoConfig(
        region_name=s.AWS_REGION,
        connect_timeout=timeout_sec,
        read_timeout=timeout_sec,
        retries={"max_attempts": 2, "mode": "standard"},
    )
    return boto3.client("s3", region_name=s.AWS_REGION, config=config)
