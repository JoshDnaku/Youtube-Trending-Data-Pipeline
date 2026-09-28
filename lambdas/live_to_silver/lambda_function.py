"""
Lambda: Live API JSON -> Silver Parquet  (statistics)
──────────────────────────────────────────────────────
The tiny, free counterpart to the Glue backfill job. Reads the flattened live
trending JSONL the ingestion Lambda wrote to Bronze (youtube/raw_statistics/),
cleanses it with the SAME logic as bronze_to_silver_statistics.py, and appends
to the SAME Silver table (clean_statistics).

Why Lambda (not Glue) for the live path?
  Live data is ~50 videos x 10 regions ≈ 2 MB. Spark startup alone would dwarf
  the work. Lambda+pandas does it in ~1s, free. (The 514 MB backfill uses Spark
  because THAT volume justifies it — hybrid by design.)

Both writers target clean_statistics with a compatible schema, so downstream
Gold doesn't care which produced a given row. Live rows have dislikes=0
(YouTube removed public dislikes in 2021); backfill rows have real dislikes.

Environment Variables:
    S3_BUCKET_BRONZE   - source bucket
    S3_BUCKET_SILVER   - target bucket
    GLUE_DB_SILVER     - Glue catalog DB (default yt_pipeline_silver_dev)
    GLUE_TABLE_STATS   - table name (default clean_statistics)
    SNS_ALERT_TOPIC_ARN- optional alert topic
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
GLUE_TABLE = os.environ.get("GLUE_TABLE_STATS", "clean_statistics")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")

BRONZE_PREFIX = "youtube/raw_statistics/"
SILVER_PATH = f"s3://{SILVER_BUCKET}/youtube/statistics/"

NUMERIC_COLS = ["views", "likes", "dislikes", "comment_count", "category_id"]


def list_stat_keys(bucket: str, prefix: str) -> list:
    keys = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return keys


def read_jsonl(bucket: str, key: str) -> list:
    body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    return [json.loads(l) for l in body.splitlines() if l.strip()]


def send_alert(subject: str, message: str):
    if SNS_TOPIC:
        sns_client.publish(TopicArn=SNS_TOPIC, Subject=subject[:100], Message=message)


def lambda_handler(event, context):
    keys = list_stat_keys(BRONZE_BUCKET, BRONZE_PREFIX)
    logger.info(f"Found {len(keys)} live statistics files")

    if not keys:
        return {"statusCode": 200, "rows_written": 0, "message": "No live data in Bronze."}

    rows = []
    for key in keys:
        try:
            rows.extend(read_jsonl(BRONZE_BUCKET, key))
        except Exception as e:
            logger.error(f"Failed reading {key}: {e}")

    if not rows:
        return {"statusCode": 200, "rows_written": 0, "message": "No rows parsed."}

    df = pd.DataFrame(rows)
    logger.info(f"Raw live rows: {len(df)}")

    # ── Schema align to clean_statistics ──
    for c in NUMERIC_COLS:
        if c not in df.columns:
            df[c] = 0
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")

    if "video_id" not in df.columns:
        return {"statusCode": 200, "rows_written": 0, "message": "No video_id column."}
    df = df[df["video_id"].notna() & (df["video_id"].astype(str).str.strip() != "")]

    # Live data has no Kaggle trending_date; use the ingestion date as the snapshot date.
    if "_ingested_at" in df.columns:
        df["trending_date_parsed"] = pd.to_datetime(df["_ingested_at"], errors="coerce", utc=True).dt.date
    else:
        df["trending_date_parsed"] = datetime.now(timezone.utc).date()
    df["trending_date"] = df["trending_date_parsed"].astype(str)

    df["region"] = df["region"].astype(str).str.lower()

    # Derived metrics (same formulas as the Glue job).
    df["like_ratio"] = (df["likes"] / df["views"].where(df["views"] > 0) * 100).round(4).fillna(0.0)
    df["engagement_rate"] = (
        (df["likes"] + df["dislikes"] + df["comment_count"]) / df["views"].where(df["views"] > 0) * 100
    ).round(4).fillna(0.0)

    for c in ["title", "channel_title", "tags", "publish_time"]:
        if c not in df.columns:
            df[c] = None

    df["comments_disabled"] = df.get("comments_disabled", False)
    df["ratings_disabled"] = df.get("ratings_disabled", False)
    df["_processed_at"] = datetime.now(timezone.utc).isoformat()
    df["_job_name"] = "live_to_silver_lambda"

    # Dedup: latest per video_id + region + date.
    before = len(df)
    df = df.drop_duplicates(subset=["video_id", "region", "trending_date_parsed"], keep="last")
    logger.info(f"After dedup: {len(df)} (removed {before - len(df)})")

    # Column order compatible with the Glue-written table.
    cols = [
        "video_id", "trending_date", "title", "channel_title", "category_id",
        "publish_time", "tags", "views", "likes", "dislikes", "comment_count",
        "comments_disabled", "ratings_disabled", "trending_date_parsed",
        "like_ratio", "engagement_rate", "_processed_at", "_job_name", "region",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    try:
        wr.s3.to_parquet(
            df=df,
            path=SILVER_PATH,
            dataset=True,
            database=GLUE_DB,
            table=GLUE_TABLE,
            partition_cols=["region"],
            mode="append",              # add live rows alongside the backfill
            compression="snappy",
            schema_evolution=True,
        )
    except Exception as e:
        logger.error(f"Parquet write failed: {e}", exc_info=True)
        send_alert("[YT Pipeline] Live silver transform failed", str(e))
        raise

    logger.info(f"Appended {len(df)} live rows -> {SILVER_PATH}")
    return {"statusCode": 200, "rows_written": int(len(df)), "table": f"{GLUE_DB}.{GLUE_TABLE}"}
