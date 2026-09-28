"""
Lambda: Category Reference JSON -> Silver Parquet
──────────────────────────────────────────────────
Reads the flattened category reference JSONL that the ingestion Lambda wrote
to Bronze (youtube/raw_reference_data/region=xx/...), dedupes it, and writes a
clean `clean_reference_data` Parquet table to Silver, registered in the Glue
Catalog. Gold uses this for the category_id -> category_name lookup.

Fix vs. the reference implementation:
  The original relied on `event["Records"]` (an S3-event trigger). But Step
  Functions invokes it with a plain payload that has NO Records key, so it
  silently processed nothing. This version LISTS the Bronze reference prefix
  and processes whatever is there — works whether triggered by SFN, manually,
  or on a schedule.

Environment Variables:
    S3_BUCKET_BRONZE       - source bucket
    S3_BUCKET_SILVER       - target bucket
    GLUE_DB_SILVER         - Glue catalog DB (default yt_pipeline_silver_dev)
    GLUE_TABLE_REFERENCE   - table name (default clean_reference_data)
    SNS_ALERT_TOPIC_ARN    - optional alert topic
"""

import os
import json
import logging
from datetime import datetime, timezone

import boto3
import awswrangler as wr
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
sns_client = boto3.client("sns")

BRONZE_BUCKET = os.environ["S3_BUCKET_BRONZE"]
SILVER_BUCKET = os.environ["S3_BUCKET_SILVER"]
GLUE_DB = os.environ.get("GLUE_DB_SILVER", "yt_pipeline_silver_dev")
GLUE_TABLE = os.environ.get("GLUE_TABLE_REFERENCE", "clean_reference_data")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")

BRONZE_PREFIX = "youtube/raw_reference_data/"
SILVER_PATH = f"s3://{SILVER_BUCKET}/youtube/reference_data/"


def list_reference_keys(bucket: str, prefix: str) -> list:
    """List every reference JSON object under the Bronze prefix."""
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return keys


def read_jsonl(bucket: str, key: str) -> list:
    """Read a JSONL (one JSON object per line) file into a list of dicts."""
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    rows = []
    for line in body.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def send_alert(subject: str, message: str):
    if SNS_TOPIC:
        sns_client.publish(TopicArn=SNS_TOPIC, Subject=subject[:100], Message=message)


def lambda_handler(event, context):
    keys = list_reference_keys(BRONZE_BUCKET, BRONZE_PREFIX)
    logger.info(f"Found {len(keys)} reference files under s3://{BRONZE_BUCKET}/{BRONZE_PREFIX}")

    if not keys:
        msg = "No reference files found in Bronze — nothing to transform."
        logger.warning(msg)
        return {"statusCode": 200, "rows_written": 0, "message": msg}

    all_rows = []
    for key in keys:
        try:
            all_rows.extend(read_jsonl(BRONZE_BUCKET, key))
        except Exception as e:
            logger.error(f"Failed reading {key}: {e}")

    if not all_rows:
        return {"statusCode": 200, "rows_written": 0, "message": "No rows parsed."}

    df = pd.DataFrame(all_rows)
    logger.info(f"Raw reference rows: {len(df)}")

    # Keep only the columns we care about; guard against missing ones.
    for col in ["category_id", "category_name", "region"]:
        if col not in df.columns:
            df[col] = None
    df = df[["category_id", "category_name", "region"]].copy()

    # Types + cleanup.
    df["category_id"] = pd.to_numeric(df["category_id"], errors="coerce").astype("Int64")
    df = df[df["category_id"].notna()]
    df["region"] = df["region"].astype(str).str.lower()

    # Dedupe: one row per (region, category_id).
    before = len(df)
    df = df.drop_duplicates(subset=["region", "category_id"], keep="last")
    logger.info(f"After dedup: {len(df)} (removed {before - len(df)})")

    df["_processed_at"] = datetime.now(timezone.utc).isoformat()

    try:
        wr.s3.to_parquet(
            df=df,
            path=SILVER_PATH,
            dataset=True,
            database=GLUE_DB,
            table=GLUE_TABLE,
            partition_cols=["region"],
            mode="overwrite_partitions",   # idempotent per region
            compression="snappy",
        )
    except Exception as e:
        logger.error(f"Parquet write failed: {e}", exc_info=True)
        send_alert("[YT Pipeline] Reference transform failed", str(e))
        raise

    logger.info(f"Wrote {len(df)} rows -> {SILVER_PATH} (table {GLUE_DB}.{GLUE_TABLE})")
    return {
        "statusCode": 200,
        "rows_written": int(len(df)),
        "table": f"{GLUE_DB}.{GLUE_TABLE}",
    }
