# YouTube Trending Data Pipeline

A cloud-native ETL pipeline that ingests YouTube trending video data across 10
regions via the **YouTube Data API v3**, transforms it through a **medallion
architecture** (Bronze → Silver → Gold) with a **data-quality gate**, and
produces analytics-ready aggregation tables queried through Athena.

Fully AWS-native: **Lambda + Glue (PySpark)** for compute, **S3** for storage
(Parquet/Snappy), **Step Functions** for orchestration, **EventBridge** for
scheduling, **Glue Data Catalog + Athena** for querying, **SNS** for alerts,
**CloudWatch** for monitoring, **IAM** for security.

> Built as a hands-on learning project. Region `ap-south-1`, environment `dev`.

---

## Architecture

```
YouTube API v3 ──> Bronze (raw JSON, S3) ──> Silver (clean Parquet, S3)
                                                     │
                                              Data Quality Gate ──(fail)──> SNS alert
                                                     │ (pass)
                                                     ▼
                                              Gold (analytics tables) ──> Athena
```

Orchestrated by **AWS Step Functions**; scheduled by **EventBridge**.

---

## Design decisions (and why)

- **Hybrid compute.** Live data (~2 MB/run) is transformed by **Lambda + AWS SDK
  for pandas** — free, sub-second iteration. The one-time **514 MB historical
  backfill** is processed by **Glue PySpark**, which is the right tool for that
  volume. Spark on 500 live rows would be overkill.
- **No Glue crawlers.** Tables are registered directly via the Glue API /
  awswrangler. Crawlers carry a 10-minute minimum billing per run; skipping them
  is both cheaper and more deterministic (stable, self-defined schema).
- **Flatten JSON at ingestion.** The ingestion Lambda writes a flat, stable
  schema instead of deeply nested API JSON, avoiding fragile column-name
  inference downstream.

---

## Cost

Target: **~$0**. Everything sits in free tiers except **AWS Glue** (no free
tier, ~$0.44/DPU-hour, 1-min minimum). The full-backfill Glue runs are expected
to cost **under $0.15 total**, flagged before running. EventBridge schedule is
created **disabled** to prevent recurring Glue charges.

---

## Project structure

```
.
├── lambdas/
│   ├── youtube_api_ingestion/   # Bronze: YouTube API → raw JSON in S3
│   └── json_to_parquet/         # Silver: category reference JSON → Parquet
├── glue_jobs/
│   ├── bronze_to_silver_statistics.py   # PySpark: raw → cleansed
│   └── silver_to_gold_analytics.py      # PySpark: cleansed → aggregations
├── data_quality/
│   └── dq_lambda.py             # DQ gate before Gold
├── step_functions/
│   └── pipeline_orchestration.json
├── scripts/                     # helper scripts (budget, upload, teardown)
├── data/                        # local historical CSVs (gitignored, 514 MB)
├── iam/                         # IAM policy documents
├── .env.example                 # environment template (copy to .env)
└── README.md
```

---

## Build progress

- [x] Stage 0 — Setup (budget alarm, API key, scaffold)
- [x] Stage 1 — S3, Glue DBs, SNS, IAM
- [x] Stage 2 — Bronze ingestion
- [x] Stage 3 — Silver transforms
- [x] Stage 4 — Data quality gate
- [x] Stage 5 — Gold analytics
- [x] Stage 6 — Step Functions + EventBridge
- [x] Stage 7 — Athena validation
- [x] Stage 8 — Docs + publish
- [ ] Stage 9 — Teardown

---

## Sample results (Athena on Gold)

Top trending US channels (blended historical + live):

| Channel | Total views | Times trending |
|---|---|---|
| ChildishGambinoVEVO | 3.76B | 25 |
| ibighit (BTS) | 2.24B | 80 |
| Dude Perfect | 1.87B | 131 |
| Marvel Entertainment | 1.81B | 125 |

US category view-share: Music 36%, Entertainment 22%, Film & Animation 8%.
Regional engagement varies widely: Russia 6.5% and Mexico 5.7% vs Japan 2.5%
and India 2.6% — a real behavioral difference across markets.

See `athena_queries.sql` for the full query set.

## Setup

Prerequisites: AWS account + CLI, Python 3.9+, a YouTube Data API v3 key.

> **Note:** committed IAM policies, the Glue job definitions, and the Step
> Functions definition use `<ACCOUNT_ID>` as a placeholder. Replace it with your
> own 12-digit AWS account ID before deploying. S3 bucket names include a
> uniqueness suffix (S3 names are globally unique) — adjust to your own.

```bash
cp .env.example .env      # then fill in real values (never committed)
```

Build order: Bronze ingestion Lambda → Silver (Glue backfill + live/reference
Lambdas) → DQ gate Lambda → Gold Glue job → Step Functions orchestration →
Athena validation. The EventBridge schedule ships **disabled** to avoid
recurring Glue charges.
