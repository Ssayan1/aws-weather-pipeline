"""
main.py
-------
Pipeline orchestrator — runs all stages in sequence.

Usage:
    python main.py            # Full pipeline
    python main.py --dry-run  # Fetch + transform only (no S3 upload)
"""

import argparse
import logging
import sys
import os
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from config import setup_logging, validate_config
from fetch_weather import fetch_all_cities
from transform_data import transform_records, save_locally
from upload_to_s3 import get_s3_client, ensure_bucket_exists, upload_raw_json, upload_parquet
from upload_to_s3 import dataframe_to_parquet_bytes
from config import S3_BUCKET_NAME, S3_RAW_PREFIX, S3_PROCESSED_PREFIX

logger = logging.getLogger(__name__)


def run_pipeline(dry_run: bool = False) -> int:
    """
    Execute the full data pipeline.

    Returns:
        0 on success, 1 on failure.
    """
    start = time.time()
    logger.info("=" * 60)
    logger.info(f"  AWS Weather Pipeline  |  {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)

    # ── Validate config ──────────────────────────────────────────────────────
    try:
        validate_config()
    except EnvironmentError as e:
        logger.error(f"Config validation failed: {e}")
        return 1

    # ── Step 1: Fetch ────────────────────────────────────────────────────────
    logger.info("[1/4] Fetching weather data…")
    records = fetch_all_cities()
    if not records:
        logger.error("No records fetched. Aborting.")
        return 1
    logger.info(f"      Fetched {len(records)} records.")

    # ── Step 2: Transform ────────────────────────────────────────────────────
    logger.info("[2/4] Transforming records…")
    df = transform_records(records)
    if df is None or df.empty:
        logger.error("Transformation produced no valid data. Aborting.")
        return 1
    logger.info(f"      DataFrame shape: {df.shape}")

    fetch_date = str(df["fetch_date"].iloc[0])
    fetch_hour = str(df["fetch_hour"].iloc[0])

    if dry_run:
        logger.info("[DRY RUN] Saving locally instead of uploading to S3.")
        path = save_locally(df, output_dir="data/local")
        logger.info(f"      Saved to: {path}")
        logger.info("[DRY RUN] Pipeline finished (no S3 upload).")
        return 0

    # ── Step 3: Upload ───────────────────────────────────────────────────────
    logger.info("[3/4] Uploading to S3…")
    s3 = get_s3_client()
    if not ensure_bucket_exists(s3, S3_BUCKET_NAME):
        return 1

    raw_key = upload_raw_json(s3, records, S3_BUCKET_NAME, fetch_date, fetch_hour)
    parquet_bytes = dataframe_to_parquet_bytes(df)
    parquet_key   = upload_parquet(s3, parquet_bytes, S3_BUCKET_NAME, fetch_date, fetch_hour)

    if not raw_key or not parquet_key:
        logger.error("Upload failed.")
        return 1

    # ── Step 4: Summary ──────────────────────────────────────────────────────
    elapsed = round(time.time() - start, 1)
    logger.info("[4/4] Pipeline complete ✓")
    logger.info(f"      Records   : {len(records)}")
    logger.info(f"      Raw key   : s3://{S3_BUCKET_NAME}/{raw_key}")
    logger.info(f"      Parquet   : s3://{S3_BUCKET_NAME}/{parquet_key}")
    logger.info(f"      Duration  : {elapsed}s")
    return 0


if __name__ == "__main__":
    setup_logging()
    parser = argparse.ArgumentParser(description="Run the weather data pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Skip S3 upload, save locally")
    args = parser.parse_args()
    sys.exit(run_pipeline(dry_run=args.dry_run))
