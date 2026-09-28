"""
Lambda: YouTube Data API Ingestion  (Bronze Layer)
──────────────────────────────────────────────────
Triggered by Step Functions (or EventBridge). For each configured region it:
  1. Pulls the top-50 trending videos from the YouTube Data API v3
  2. Pulls the video-category id -> name mapping
  3. FLATTENS both into a stable, tabular schema
  4. Writes newline-delimited JSON (JSONL) to the Bronze S3 bucket,
     Hive-partitioned by region / date / hour.

Why flatten here (vs. storing the raw nested API response)?
  - Downstream Spark/Pandas gets a stable, predictable schema.
  - No Glue crawler needed to infer deeply-nested columns (saves cost,
    removes a class of "column name changed" bugs).
  - We keep only the fields the pipeline actually uses.

Why JSONL (one JSON object per line) instead of one big JSON doc?
  - Both Spark and awswrangler/pandas read JSONL directly, row per line.
  - Matches how the Kaggle CSV backfill maps to rows.

Environment Variables:
    YOUTUBE_API_KEY       - Google API key with YouTube Data API v3 enabled
    S3_BUCKET_BRONZE      - Target S3 bucket for raw data
    YOUTUBE_REGIONS       - Comma-separated region codes (UPPERCASE, e.g. US,GB,IN)
    SNS_ALERT_TOPIC_ARN   - SNS topic ARN for failure alerts (optional)
"""

import json
import os
import logging
from datetime import datetime, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

import boto3

# ── Logging ──────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── AWS clients ──────────────────────────────────────────────────────────────
s3_client = boto3.client("s3")
sns_client = boto3.client("sns")

# ── Config ───────────────────────────────────────────────────────────────────
API_KEY = os.environ["YOUTUBE_API_KEY"]
BUCKET = os.environ["S3_BUCKET_BRONZE"]
REGIONS = os.environ.get("YOUTUBE_REGIONS", "US,GB,CA,DE,FR,IN,JP,KR,MX,RU").split(",")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
API_BASE = "https://www.googleapis.com/youtube/v3"
MAX_RESULTS = 50


# ── YouTube API calls ─────────────────────────────────────────────────────────
def fetch_trending_videos(region_code_upper: str) -> dict:
    """Top trending videos for a region. region_code_upper must be UPPERCASE."""
    params = urlencode({
        "part": "snippet,statistics,contentDetails",
        "chart": "mostPopular",
        "regionCode": region_code_upper,   # API requires ISO-3166 alpha-2 UPPERCASE
        "maxResults": MAX_RESULTS,
        "key": API_KEY,
    })
    req = Request(f"{API_BASE}/videos?{params}", headers={"Accept": "application/json"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_video_categories(region_code_upper: str) -> dict:
    """Category id -> name mapping for a region. UPPERCASE code."""
    params = urlencode({
        "part": "snippet",
        "regionCode": region_code_upper,
        "key": API_KEY,
    })
    req = Request(f"{API_BASE}/videoCategories?{params}", headers={"Accept": "application/json"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ── Flattening ────────────────────────────────────────────────────────────────
def flatten_video(item: dict, region_lower: str, ingested_at: str) -> dict:
    """Extract a flat row from one nested video item."""
    snippet = item.get("snippet", {}) or {}
    stats = item.get("statistics", {}) or {}
    tags = snippet.get("tags", []) or []

    def to_int(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return 0

    return {
        "video_id": item.get("id"),
        "title": snippet.get("title"),
        "channel_id": snippet.get("channelId"),
        "channel_title": snippet.get("channelTitle"),
        "category_id": to_int(snippet.get("categoryId")),
        "publish_time": snippet.get("publishedAt"),
        "tags": "|".join(tags) if tags else None,
        "views": to_int(stats.get("viewCount")),
        "likes": to_int(stats.get("likeCount")),
        # likeCount is public but dislikeCount was removed by YouTube in 2021;
        # keep the column for schema-compatibility with the Kaggle backfill.
        "dislikes": 0,
        "comment_count": to_int(stats.get("commentCount")),
        "comments_disabled": "commentCount" not in stats,
        "ratings_disabled": "likeCount" not in stats,
        "region": region_lower,
        "_ingested_at": ingested_at,
        "_source": "youtube_data_api_v3",
    }


def flatten_categories(payload: dict, region_lower: str, ingested_at: str) -> list:
    """Extract flat category rows from the videoCategories response."""
    rows = []
    for item in payload.get("items", []):
        snippet = item.get("snippet", {}) or {}
        rows.append({
            "category_id": int(item["id"]) if str(item.get("id", "")).isdigit() else None,
            "category_name": snippet.get("title"),
            "region": region_lower,
            "_ingested_at": ingested_at,
            "_source": "youtube_data_api_v3",
        })
    return rows


# ── S3 write (JSONL) ───────────────────────────────────────────────────────────
def write_jsonl_to_s3(rows: list, bucket: str, key: str):
    body = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )


def send_alert(subject: str, message: str):
    if SNS_TOPIC:
        sns_client.publish(TopicArn=SNS_TOPIC, Subject=subject[:100], Message=message)


# ── Handler ────────────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    date_partition = now.strftime("%Y-%m-%d")
    hour_partition = now.strftime("%H")
    ingested_at = now.isoformat()
    ingestion_id = now.strftime("%Y%m%d_%H%M%S")

    results = {"success": [], "failed": []}

    for raw_region in REGIONS:
        region_upper = raw_region.strip().upper()   # for the API
        region_lower = region_upper.lower()          # for the S3 partition key
        if not region_upper:
            continue
        logger.info(f"Processing region: {region_upper}")

        # ── Trending videos ──────────────────────────────────────────────
        try:
            payload = fetch_trending_videos(region_upper)
            rows = [flatten_video(it, region_lower, ingested_at) for it in payload.get("items", [])]

            stats_key = (
                f"youtube/raw_statistics/"
                f"region={region_lower}/date={date_partition}/hour={hour_partition}/"
                f"{ingestion_id}.json"
            )
            write_jsonl_to_s3(rows, BUCKET, stats_key)
            logger.info(f"  {len(rows)} videos -> s3://{BUCKET}/{stats_key}")

        except (HTTPError, URLError) as e:
            logger.error(f"  API error ({region_upper} trending): {e}")
            results["failed"].append({"region": region_upper, "type": "trending", "error": str(e)})
            continue
        except Exception as e:
            logger.error(f"  Unexpected error ({region_upper} trending): {e}")
            results["failed"].append({"region": region_upper, "type": "trending", "error": str(e)})
            continue

        # ── Category reference ───────────────────────────────────────────
        try:
            cat_payload = fetch_video_categories(region_upper)
            cat_rows = flatten_categories(cat_payload, region_lower, ingested_at)

            ref_key = (
                f"youtube/raw_reference_data/"
                f"region={region_lower}/date={date_partition}/"
                f"{region_lower}_category_id.json"
            )
            write_jsonl_to_s3(cat_rows, BUCKET, ref_key)
            logger.info(f"  {len(cat_rows)} categories -> s3://{BUCKET}/{ref_key}")

        except (HTTPError, URLError) as e:
            logger.error(f"  API error ({region_upper} categories): {e}")
            results["failed"].append({"region": region_upper, "type": "categories", "error": str(e)})
            continue

        results["success"].append(region_upper)

    summary = (
        f"Ingestion {ingestion_id} complete. "
        f"Success: {len(results['success'])}/{len(REGIONS)} regions. "
        f"Failed: {len(results['failed'])}."
    )
    logger.info(summary)

    if results["failed"]:
        send_alert(
            subject=f"[YT Pipeline] Ingestion partial failure - {ingestion_id}",
            message=json.dumps(results, indent=2),
        )

    return {
        "statusCode": 200,
        "ingestion_id": ingestion_id,
        "regions_succeeded": len(results["success"]),
        "regions_failed": len(results["failed"]),
        "results": results,
    }
