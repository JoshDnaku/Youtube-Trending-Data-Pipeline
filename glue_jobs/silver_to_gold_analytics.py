"""
Glue Job: Silver -> Gold  (Analytics Aggregations)
───────────────────────────────────────────────────
Reads cleansed Silver (clean_statistics + clean_reference_data), joins them
for category names, and builds three analytics tables in Gold:

  1. trending_analytics  — daily trending metrics per region
  2. channel_analytics   — channel performance + rank within region
  3. category_analytics  — category breakdowns with view-share %

Spark features exercised here (vs. the bronze->silver job):
  - broadcast join (tiny reference table joined to stats)
  - window functions (row_number ranking, share-of-total)
  - collect_set (distinct categories per channel as an array)

All Gold tables: Parquet/Snappy, partitioned by region, registered in Catalog.

Job Parameters:
    --JOB_NAME
    --silver_database   Silver Glue DB
    --gold_bucket       Gold S3 bucket
    --gold_database     Gold Glue DB
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "silver_database", "gold_bucket", "gold_database",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
logger = glueContext.get_logger()

SILVER_DB = args["silver_database"]
GOLD_BUCKET = args["gold_bucket"]
GOLD_DB = args["gold_database"]


def write_gold(df, table, path):
    dyf = DynamicFrame.fromDF(df, glueContext, table)
    sink = glueContext.getSink(
        connection_type="s3",
        path=path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["region"],
    )
    sink.setCatalogInfo(catalogDatabase=GOLD_DB, catalogTableName=table)
    sink.setFormat("glueparquet", compression="snappy")
    sink.writeFrame(dyf)


# ── Read Silver ───────────────────────────────────────────────────────────────
logger.info("Reading Silver clean_statistics...")
stats = glueContext.create_dynamic_frame.from_catalog(
    database=SILVER_DB, table_name="clean_statistics", transformation_ctx="stats"
).toDF()
logger.info(f"Statistics rows: {stats.count()}")

# ── Join category names from reference (best-effort) ──────────────────────────
logger.info("Reading Silver clean_reference_data for category names...")
try:
    ref = glueContext.create_dynamic_frame.from_catalog(
        database=SILVER_DB, table_name="clean_reference_data", transformation_ctx="ref"
    ).toDF()

    if "category_id" in ref.columns and "category_name" in ref.columns:
        lookup = (
            ref.select(
                F.col("category_id").cast("long").alias("category_id"),
                F.col("category_name"),
            )
            .dropDuplicates(["category_id"])
        )
        stats = stats.withColumn("category_id", F.col("category_id").cast("long"))
        stats = stats.join(F.broadcast(lookup), on="category_id", how="left")
    else:
        logger.warn(f"reference missing expected cols; found {ref.columns}")
except Exception as e:
    logger.warn(f"Could not load reference data: {e}. Proceeding without names.")

if "category_name" not in stats.columns:
    stats = stats.withColumn("category_name", F.lit("Unknown"))
else:
    stats = stats.fillna("Unknown", subset=["category_name"])

# ══ GOLD 1: trending_analytics (region x date) ════════════════════════════════
logger.info("Building trending_analytics...")
trending = (
    stats.groupBy("region", "trending_date_parsed").agg(
        F.count("video_id").alias("total_videos"),
        F.sum("views").alias("total_views"),
        F.sum("likes").alias("total_likes"),
        F.sum("comment_count").alias("total_comments"),
        F.avg("views").alias("avg_views_per_video"),
        F.avg("like_ratio").alias("avg_like_ratio"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.max("views").alias("max_views"),
        F.countDistinct("channel_title").alias("unique_channels"),
        F.countDistinct("category_id").alias("unique_categories"),
    )
    .withColumn("_aggregated_at", F.current_timestamp())
)
write_gold(trending, "trending_analytics", f"s3://{GOLD_BUCKET}/youtube/trending_analytics/")
logger.info(f"  trending_analytics rows: {trending.count()}")

# ══ GOLD 2: channel_analytics (channel x region, ranked) ══════════════════════
logger.info("Building channel_analytics...")
channel = (
    stats.groupBy("channel_title", "region").agg(
        F.countDistinct("video_id").alias("total_videos"),
        F.sum("views").alias("total_views"),
        F.sum("likes").alias("total_likes"),
        F.sum("comment_count").alias("total_comments"),
        F.avg("views").alias("avg_views_per_video"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.max("views").alias("peak_views"),
        F.count("trending_date_parsed").alias("times_trending"),
        F.collect_set("category_name").alias("categories"),
    )
)
rank_w = Window.partitionBy("region").orderBy(F.col("total_views").desc())
channel = channel.withColumn("rank_in_region", F.row_number().over(rank_w))
channel = channel.withColumn("_aggregated_at", F.current_timestamp())
write_gold(channel, "channel_analytics", f"s3://{GOLD_BUCKET}/youtube/channel_analytics/")
logger.info(f"  channel_analytics rows: {channel.count()}")

# ══ GOLD 3: category_analytics (category x region x date, view share) ═════════
logger.info("Building category_analytics...")
category = (
    stats.groupBy("category_name", "category_id", "region", "trending_date_parsed").agg(
        F.count("video_id").alias("video_count"),
        F.sum("views").alias("total_views"),
        F.sum("likes").alias("total_likes"),
        F.sum("comment_count").alias("total_comments"),
        F.avg("engagement_rate").alias("avg_engagement_rate"),
        F.countDistinct("channel_title").alias("unique_channels"),
    )
)
share_w = Window.partitionBy("region", "trending_date_parsed")
category = category.withColumn(
    "view_share_pct",
    F.round(F.col("total_views") / F.sum("total_views").over(share_w) * 100, 2),
)
category = category.withColumn("_aggregated_at", F.current_timestamp())
write_gold(category, "category_analytics", f"s3://{GOLD_BUCKET}/youtube/category_analytics/")
logger.info(f"  category_analytics rows: {category.count()}")

logger.info("Gold build complete.")
job.commit()
