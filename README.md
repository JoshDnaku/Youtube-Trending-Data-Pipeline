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
- [ ] Stage 2 — Bronze ingestion
- [x] Stage 3 — Silver transforms
- [x] Stage 4 — Data quality gate
- [ ] Stage 5 — Gold analytics
- [ ] Stage 6 — Step Functions + EventBridge
- [ ] Stage 7 — Athena validation
- [ ] Stage 8 — Docs + publish
- [ ] Stage 9 — Teardown

---

## Setup

Prerequisites: AWS account + CLI, Python 3.9+, a YouTube Data API v3 key.

```bash
cp .env.example .env      # then fill in real values (never committed)
```

_Full setup and run instructions are added as the pipeline is built._
