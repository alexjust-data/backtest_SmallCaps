from __future__ import annotations

import boto3
from botocore.config import Config

from src.core.settings import settings


def get_r2_client():
    """
    Cloudflare R2 is S3-compatible. This returns a boto3 S3 client configured
    for R2 endpoint.
    """
    # Keep retries sane & deterministic
    cfg = Config(
        retries={"max_attempts": 10, "mode": "standard"},
        # You can tune timeouts later if needed
    )

    return boto3.client(
        "s3",
        endpoint_url=settings.R2_ENDPOINT,
        aws_access_key_id=settings.R2_ACCESS_KEY_ID,
        aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
        region_name=settings.R2_REGION,
        config=cfg,
    )
