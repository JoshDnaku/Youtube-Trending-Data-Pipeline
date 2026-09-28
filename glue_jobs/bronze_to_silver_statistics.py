"""
Glue Job: Bronze -> Silver  (Statistics)
─────────────────────────────────────────
Reads the historical Kaggle CSV backfill from the Bronze S3 prefix,
cleanses + standardises it, and writes clean Parquet (Snappy) to Silver,
registering the table `clean_statistics` in the Glue Catalog.

Why Spark here (and not Lambda+pandas)?
  The backfill is ~514 MB / ~2M rows across 10 region CSVs, with messy
  multiline quoted description fields. That volume + messiness is a genuine
  fit for Spark. (The tiny live API path uses Lambda instead — see
  lambdas/, Stage 3c.)

Reads directly from S3 CSV (not via a crawler-built catalog table) so the
schema is explicit and stable — no crawler cost, no column-name surprises.

Job Parameters (passed via --arguments or Step Functions):
    --JOB_NAME          (auto)
    --bronze_bucket     Bronze S3 bucket
    --silver_bucket     Silver S3 bucket
    --silver_database   Glue catalog DB for Silver
    --silver_table      Silver table name (e.g. clean_statistics)
    --regions           Comma-separated region codes, or 'all' (default: all)
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, BooleanType, StringType

# ── Job setup ────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_bucket",
    "silver_bucket",
    "silver_database",
    "silver_table",
    "regions",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

BRONZE_BUCKET = args["bronze_bucket"]
SILVER_BUCKET = args["silver_bucket"]
SILVER_DB = args["silver_database"]
SILVER_TABLE = args["silver_table"]
REGIONS = args.get("regions", "all")

CSV_PREFIX = f"s3://{BRONZE_BUCKET}/youtube/raw_statistics_csv/"
SILVER_PATH = f"s3://{SILVER_BUCKET}/youtube/statistics/"

logger.info(f"Reading CSV backfill from {CSV_PREFIX}")
logger.info(f"Writing Silver to {SILVER_PATH} (table {SILVER_DB}.{SILVER_TABLE})")

# ── Step 1: Read CSV (multiline-safe) ─────────────────────────────────────────
# The partition column `region` is encoded in the path (region=xx/), so Spark
# picks it up automatically via basePath partition discovery.
df = (
    spark.read
    .option("header", "true")
    .option("multiLine", "true")       # description fields contain newlines
    .option("quote", '"')
    .option("escape", '"')
    .option("mode", "PERMISSIVE")      # keep going on malformed rows
    .csv(CSV_PREFIX)
)

initial_count = df.count()
logger.info(f"Rows read from CSV: {initial_count}")

# Region comes from the path; if partition discovery didn't add it, derive later.
if "region" not in df.columns:
    df = df.withColumn(
        "region",
        F.regexp_extract(F.input_file_name(), r"region=([^/]+)/", 1)
    )

# Optional region filter (default 'all' keeps every region)
if REGIONS.strip().lower() != "all":
    wanted = [r.strip().lower() for r in REGIONS.split(",") if r.strip()]
    df = df.filter(F.lower(F.col("region")).isin(wanted))
    logger.info(f"Filtered to regions: {wanted}")

# ── Step 2: Schema enforcement / type casting ─────────────────────────────────
df = df.select(
    F.col("video_id").cast(StringType()),
    F.col("trending_date").cast(StringType()),
    F.col("title").cast(StringType()),
    F.col("channel_title").cast(StringType()),
    F.col("category_id").cast(LongType()),
    F.col("publish_time").cast(StringType()),
    F.col("tags").cast(StringType()),
    F.col("views").cast(LongType()),
    F.col("likes").cast(LongType()),
    F.col("dislikes").cast(LongType()),
    F.col("comment_count").cast(LongType()),
    F.col("comments_disabled").cast(BooleanType()),
    F.col("ratings_disabled").cast(BooleanType()),
    F.lower(F.trim(F.col("region"))).alias("region"),
)

# ── Step 3: Cleansing ─────────────────────────────────────────────────────────
# Drop rows with no video_id (corrupt / malformed).
df = df.filter(F.col("video_id").isNotNull() & (F.trim(F.col("video_id")) != ""))

# Parse Kaggle trending_date format 'YY.DD.MM' -> proper date.
df = df.withColumn(
    "trending_date_parsed",
    F.when(
        F.col("trending_date").rlike(r"^\d{2}\.\d{2}\.\d{2}$"),
        F.to_date(F.col("trending_date"), "yy.dd.MM"),
    ).otherwise(F.to_date(F.col("trending_date"))),
)

# Fill numeric nulls with 0.
for c in ["views", "likes", "dislikes", "comment_count"]:
    df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0)))

# Derived metrics.
df = df.withColumn(
    "like_ratio",
    F.when(F.col("views") > 0, F.round(F.col("likes") / F.col("views") * 100, 4)).otherwise(0.0),
)
df = df.withColumn(
    "engagement_rate",
    F.when(
        F.col("views") > 0,
        F.round((F.col("likes") + F.col("dislikes") + F.col("comment_count")) / F.col("views") * 100, 4),
    ).otherwise(0.0),
)

df = df.withColumn("_processed_at", F.current_timestamp())
df = df.withColumn("_job_name", F.lit(args["JOB_NAME"]))

# ── Step 4: Deduplicate (latest row per video/region/date) ────────────────────
w = Window.partitionBy("video_id", "region", "trending_date_parsed").orderBy(F.col("_processed_at").desc())
df = df.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

clean_count = df.count()
logger.info(f"Rows after cleanse + dedup: {clean_count} (removed {initial_count - clean_count})")

# ── Step 5: Write to Silver (Parquet/Snappy, partitioned by region) ───────────
dyf = DynamicFrame.fromDF(df, glueContext, "silver_statistics")
sink = glueContext.getSink(
    connection_type="s3",
    path=SILVER_PATH,
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["region"],
)
sink.setCatalogInfo(catalogDatabase=SILVER_DB, catalogTableName=SILVER_TABLE)
sink.setFormat("glueparquet", compression="snappy")
sink.writeFrame(dyf)

logger.info(f"Silver write complete: {clean_count} rows -> {SILVER_PATH}")
job.commit()
