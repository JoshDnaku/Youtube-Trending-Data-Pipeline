"""
Lambda: Data Quality Gate  (Silver -> [gate] -> Gold)
──────────────────────────────────────────────────────
Runs AFTER the Silver layer is built and BEFORE Gold aggregation.
Reads Silver tables via Athena, runs quality checks, and returns a single
boolean verdict `quality_passed`. Step Functions branches on that verdict:
  pass -> run Gold ;  fail -> SNS alert + stop (Gold never runs).

Checks:
  1. Row count       — enough data present?
  2. Null percentage — critical columns populated (<= MAX_NULL_PCT)?
  3. Schema          — required columns exist?
  4. Value ranges    — no negative / absurd view counts?
  5. Freshness       — newest record recent enough?

Environment Variables:
    GLUE_DB_SILVER        — Silver Glue database (default yt_pipeline_silver_dev)
    ATHENA_OUTPUT         — s3://.../athena-results/  (Athena writes results here)
    SNS_ALERT_TOPIC_ARN   — alert topic
    DQ_MIN_ROW_COUNT      — min rows (default 10)
    DQ_MAX_NULL_PERCENT   — max null %% on critical cols (default 5.0)
"""

import os
import json
import logging
from datetime import datetime, timezone, timedelta

import boto3
import awswrangler as wr
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

sns_client = boto3.client("sns")

GLUE_DB = os.environ.get("GLUE_DB_SILVER", "yt_pipeline_silver_dev")
ATHENA_OUTPUT = os.environ.get("ATHENA_OUTPUT", "")
SNS_TOPIC = os.environ.get("SNS_ALERT_TOPIC_ARN", "")
MIN_ROW_COUNT = int(os.environ.get("DQ_MIN_ROW_COUNT", "10"))
MAX_NULL_PCT = float(os.environ.get("DQ_MAX_NULL_PERCENT", "5.0"))
MAX_VIEWS = 50_000_000_000
FRESHNESS_HOURS = 48

CRITICAL_COLUMNS = {
    "clean_statistics": ["video_id", "title", "channel_title", "views", "region"],
    "clean_reference_data": ["category_id", "region"],
}


def _read_sample(table: str) -> pd.DataFrame:
    sql = f'SELECT * FROM "{table}" LIMIT 10000'
    return wr.athena.read_sql_query(
        sql=sql, database=GLUE_DB, ctas_approach=False, s3_output=ATHENA_OUTPUT
    )


def check_row_count(df, table):
    count = len(df)
    return {"check": "row_count", "table": table, "value": count,
            "threshold": MIN_ROW_COUNT, "passed": count >= MIN_ROW_COUNT,
            "message": f"{count} rows (min {MIN_ROW_COUNT})"}


def check_nulls(df, table):
    out = []
    for col in CRITICAL_COLUMNS.get(table, []):
        if col not in df.columns:
            out.append({"check": "null_pct", "table": table, "column": col,
                        "passed": False, "message": f"column '{col}' missing"})
            continue
        pct = float(df[col].isna().sum() / len(df) * 100) if len(df) else 0.0
        out.append({"check": "null_pct", "table": table, "column": col,
                    "value": round(pct, 2), "threshold": MAX_NULL_PCT,
                    "passed": bool(pct <= MAX_NULL_PCT),
                    "message": f"{col} null {pct:.2f}% (max {MAX_NULL_PCT}%)"})
    return out


def check_schema(df, table):
    expected = set(CRITICAL_COLUMNS.get(table, []))
    missing = expected - set(df.columns)
    return {"check": "schema", "table": table, "missing": list(missing),
            "passed": not missing,
            "message": ("all required columns present" if not missing
                        else f"missing {missing}")}


def check_value_ranges(df, table):
    if table != "clean_statistics" or "views" not in df.columns:
        return []
    v = pd.to_numeric(df["views"], errors="coerce")
    neg = int((v < 0).sum())
    extreme = int((v > MAX_VIEWS).sum())
    return [{"check": "value_range", "table": table, "column": "views",
             "negative": neg, "extreme": extreme, "passed": neg == 0 and extreme == 0,
             "message": f"{neg} negative, {extreme} extreme views"}]


def check_freshness(df, table):
    col = next((c for c in ("_processed_at", "_ingested_at") if c in df.columns), None)
    if not col:
        return {"check": "freshness", "table": table, "passed": True,
                "message": "no timestamp column — skipped (backfill)"}
    try:
        latest = pd.to_datetime(df[col], errors="coerce", utc=True).max()
        cutoff = datetime.now(timezone.utc) - timedelta(hours=FRESHNESS_HOURS)
        return {"check": "freshness", "table": table,
                "latest": str(latest), "passed": bool(latest >= cutoff),
                "message": f"latest {latest} vs cutoff {cutoff}"}
    except Exception as e:
        return {"check": "freshness", "table": table, "passed": True,
                "message": f"unparseable timestamps — skipped ({e})"}


def lambda_handler(event, context):
    database = event.get("database", GLUE_DB)
    tables = event.get("tables", ["clean_statistics"])

    results = []
    overall = True

    for table in tables:
        logger.info(f"DQ checks on {database}.{table}")
        try:
            df = _read_sample(table)
        except Exception as e:
            logger.error(f"read failed for {table}: {e}")
            results.append({"check": "read_table", "table": table,
                            "passed": False, "message": str(e)})
            overall = False
            continue

        checks = [check_row_count(df, table)]
        checks += check_nulls(df, table)
        checks.append(check_schema(df, table))
        checks += check_value_ranges(df, table)
        checks.append(check_freshness(df, table))

        for c in checks:
            logger.info(f"  {c['check']}: {'PASS' if c['passed'] else 'FAIL'} — {c['message']}")
            if not c["passed"]:
                overall = False
        results.extend(checks)

    passed = sum(1 for r in results if r["passed"])
    logger.info(f"DQ: {passed}/{len(results)} passed. Overall: {'PASS' if overall else 'FAIL'}")

    if not overall and SNS_TOPIC:
        failed = [r for r in results if not r["passed"]]
        sns_client.publish(
            TopicArn=SNS_TOPIC,
            Subject="[YT Pipeline] Data quality checks FAILED",
            Message=json.dumps(failed, indent=2, default=str),
        )

    return {
        "quality_passed": bool(overall),
        "checks_passed": int(passed),
        "checks_total": int(len(results)),
        "details": json.loads(json.dumps(results, default=str)),
    }
