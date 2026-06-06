"""
upload_to_s3.py
---------------
Uploads transformed weather data (Parquet) to AWS S3 using Hive-style
partition paths so AWS Glue Crawler can auto-discover the schema.

Partition layout:
    s3://<bucket>/processed/weather/fetch_date=2024-11-01/fetch_hour=12/data.parquet

Responsibilities:
  - Create a Boto3 S3 client with credentials from config
  - Build the correct S3 key with date/hour partitions
  - Upload raw JSON (for data lake / audit trail)
  - Upload processed Parquet (for Athena queries)
  - Graceful error handling with retries

Usage:
    python src/upload_to_s3.py
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    S3_BUCKET_NAME,
    S3_RAW_PREFIX,
    S3_PROCESSED_PREFIX,
    validate_config,
    setup_logging,
)
from fetch_weather import fetch_all_cities
from transform_data import transform_records, dataframe_to_parquet_bytes

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# S3 client factory
# ---------------------------------------------------------------------------

def get_s3_client():
    """
    Return a Boto3 S3 client configured from environment variables.

    Uses explicit credentials if set; otherwise falls back to the
    default credential chain (~/.aws/credentials, EC2 role, etc.).
    """
    kwargs = {"region_name": AWS_REGION}
    if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY:
        kwargs["aws_access_key_id"]     = AWS_ACCESS_KEY_ID
        kwargs["aws_secret_access_key"] = AWS_SECRET_ACCESS_KEY

    client = boto3.client("s3", **kwargs)
    logger.debug("Boto3 S3 client created.")
    return client


# ---------------------------------------------------------------------------
# Bucket helpers
# ---------------------------------------------------------------------------

def ensure_bucket_exists(s3_client, bucket_name: str) -> bool:
    """
    Check the bucket exists and we have access.
    Creates it if not found (only in us-east-1 where LocationConstraint is omitted).
    Returns True on success, False on permission error.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"S3 bucket '{bucket_name}' confirmed accessible.")
        return True
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "404":
            logger.warning(f"Bucket '{bucket_name}' not found — attempting to create…")
            try:
                if AWS_REGION == "us-east-1":
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
                    )
                logger.info(f"Bucket '{bucket_name}' created in {AWS_REGION}.")
                return True
            except ClientError as create_err:
                logger.error(f"Failed to create bucket: {create_err}")
                return False
        elif code in ("403", "AccessDenied"):
            logger.error(f"Access denied to bucket '{bucket_name}'. Check IAM permissions.")
            return False
        else:
            logger.error(f"Unexpected S3 error: {e}")
            return False


# ---------------------------------------------------------------------------
# Upload helpers
# ---------------------------------------------------------------------------

def build_s3_key(prefix: str, fetch_date: str, fetch_hour: str, filename: str) -> str:
    """
    Build a Hive-partitioned S3 key.

    Example:
        processed/weather/fetch_date=2024-11-01/fetch_hour=12/weather_20241101_1200.parquet
    """
    return f"{prefix}/fetch_date={fetch_date}/fetch_hour={fetch_hour}/{filename}"


def upload_bytes_to_s3(
    s3_client,
    data: bytes,
    bucket: str,
    key: str,
    content_type: str = "application/octet-stream",
) -> bool:
    """
    Upload raw bytes to S3.

    Returns True on success, False on failure.
    """
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
        )
        logger.info(f"Uploaded {len(data):,} bytes → s3://{bucket}/{key}")
        return True
    except ClientError as e:
        logger.error(f"S3 upload failed for key '{key}': {e.response['Error']['Message']}")
        return False
    except (BotoCoreError, Exception) as e:
        logger.error(f"Unexpected upload error for key '{key}': {e}")
        return False


def upload_raw_json(
    s3_client,
    records: list[dict],
    bucket: str,
    fetch_date: str,
    fetch_hour: str,
) -> Optional[str]:
    """
    Upload raw JSON records to the raw S3 prefix (audit / data lake layer).

    Returns the S3 key on success, None on failure.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"weather_raw_{ts}.json"
    key = build_s3_key(S3_RAW_PREFIX, fetch_date, fetch_hour, filename)
    data = json.dumps(records, indent=2, ensure_ascii=False).encode("utf-8")

    success = upload_bytes_to_s3(s3_client, data, bucket, key, content_type="application/json")
    return key if success else None


def upload_parquet(
    s3_client,
    parquet_bytes: bytes,
    bucket: str,
    fetch_date: str,
    fetch_hour: str,
) -> Optional[str]:
    """
    Upload a Parquet file to the processed S3 prefix (Athena / Glue layer).

    Returns the S3 key on success, None on failure.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"weather_{ts}.parquet"
    key = build_s3_key(S3_PROCESSED_PREFIX, fetch_date, fetch_hour, filename)

    success = upload_bytes_to_s3(s3_client, parquet_bytes, bucket, key, content_type="application/octet-stream")
    return key if success else None


# ---------------------------------------------------------------------------
# Orchestration function
# ---------------------------------------------------------------------------

def run_upload_pipeline() -> dict:
    """
    Full pipeline:
      1. Fetch weather for all configured cities
      2. Transform into clean DataFrame
      3. Upload raw JSON to S3
      4. Upload Parquet to S3

    Returns:
        Summary dict with keys: success, raw_key, parquet_key, record_count
    """
    result = {"success": False, "raw_key": None, "parquet_key": None, "record_count": 0}

    # ── Step 1: Fetch ────────────────────────────────────────────────────────
    logger.info("=== Step 1: Fetching weather data ===")
    records = fetch_all_cities()
    if not records:
        logger.error("No records fetched. Aborting pipeline.")
        return result

    result["record_count"] = len(records)

    # ── Step 2: Transform ────────────────────────────────────────────────────
    logger.info("=== Step 2: Transforming records ===")
    df = transform_records(records)
    if df is None or df.empty:
        logger.error("Transformation produced no valid data. Aborting.")
        return result

    # Derive partition values from the first row
    fetch_date = str(df["fetch_date"].iloc[0])
    fetch_hour = str(df["fetch_hour"].iloc[0])

    # ── Step 3: S3 client ────────────────────────────────────────────────────
    logger.info("=== Step 3: Connecting to S3 ===")
    s3 = get_s3_client()
    if not ensure_bucket_exists(s3, S3_BUCKET_NAME):
        logger.error("Cannot access S3 bucket. Aborting.")
        return result

    # ── Step 4: Upload raw JSON ──────────────────────────────────────────────
    logger.info("=== Step 4: Uploading raw JSON ===")
    raw_key = upload_raw_json(s3, records, S3_BUCKET_NAME, fetch_date, fetch_hour)
    result["raw_key"] = raw_key

    # ── Step 5: Upload Parquet ───────────────────────────────────────────────
    logger.info("=== Step 5: Uploading Parquet ===")
    parquet_bytes = dataframe_to_parquet_bytes(df)
    parquet_key = upload_parquet(s3, parquet_bytes, S3_BUCKET_NAME, fetch_date, fetch_hour)
    result["parquet_key"] = parquet_key

    result["success"] = raw_key is not None and parquet_key is not None

    if result["success"]:
        logger.info(
            f"Pipeline complete ✓  |  {len(records)} records  "
            f"|  raw: s3://{S3_BUCKET_NAME}/{raw_key}  "
            f"|  parquet: s3://{S3_BUCKET_NAME}/{parquet_key}"
        )
    else:
        logger.error("Pipeline finished with upload errors. Check logs above.")

    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    setup_logging()
    validate_config()
    summary = run_upload_pipeline()
    print("\n=== Upload Summary ===")
    for k, v in summary.items():
        print(f"  {k:<15} : {v}")
