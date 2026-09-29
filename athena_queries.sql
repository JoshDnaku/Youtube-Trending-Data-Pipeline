-- ─────────────────────────────────────────────────────────────────────
-- Example Athena queries against the Gold layer (database: yt_pipeline_gold_dev)
-- Set query result location to your Athena output S3 path before running.
-- ─────────────────────────────────────────────────────────────────────

-- 1. Top 10 trending channels in the US by total views
SELECT channel_title, total_views, times_trending, rank_in_region
FROM channel_analytics
WHERE region = 'us'
ORDER BY total_views DESC
LIMIT 10;

-- 2. Category view-share in the US (which content categories dominate)
SELECT category_name,
       SUM(total_views)            AS views,
       ROUND(AVG(view_share_pct),2) AS avg_share_pct
FROM category_analytics
WHERE region = 'us'
GROUP BY category_name
ORDER BY views DESC
LIMIT 10;

-- 3. Daily trending volume and engagement by region
SELECT region,
       COUNT(*)                        AS days_tracked,
       SUM(total_videos)               AS total_trending_videos,
       ROUND(AVG(avg_engagement_rate),4) AS avg_engagement
FROM trending_analytics
GROUP BY region
ORDER BY total_trending_videos DESC;

-- 4. Peak trending day per region (highest total views in a single day)
SELECT region, trending_date_parsed, total_views, total_videos
FROM trending_analytics t
WHERE total_views = (
    SELECT MAX(total_views)
    FROM trending_analytics t2
    WHERE t2.region = t.region
)
ORDER BY total_views DESC;

-- 5. Channels that trend across the most categories (versatility)
SELECT channel_title, region, cardinality(categories) AS category_count, total_views
FROM channel_analytics
WHERE region = 'us'
ORDER BY category_count DESC, total_views DESC
LIMIT 10;
