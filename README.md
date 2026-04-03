# Amazon Reviews Intelligence Platform

> A full end-to-end data pipeline and analytics project built on 44 million real,
> raw Amazon customer reviews, from ingestion to business intelligence dashboards.

---

## Why I built this

I wanted a portfolio project that actually reflects what data analysts and engineers
do inside a company, not a cleaned Kaggle CSV with a Jupyter notebook on top of it.

The problem I chose to solve is real: Amazon sellers, category managers, and marketplace
teams are sitting on millions of customer reviews but most of them have no systematic
way to answer basic questions like, which product attributes actually correlate with
higher sales rank? Where are the fake reviews concentrated? What does a customer mean
when they give something 3 stars vs 5 stars? What is the sentiment gap between what
the star rating says and what the review text actually says?

These questions matter because the answers directly affect revenue. A product with
4.8 stars and terrible sentiment in the review text is a ticking churn problem.
A product with 3.9 stars but overwhelmingly positive review sentiment is probably
underpriced and underselling. That gap is where the business value lives.

So I built a pipeline that ingests the raw data, cleans it properly, runs NLP on
the text, and surfaces the business insights in a dashboard that a category manager
or product analyst could actually use week to week.

---

## Why two data sources?

This is a deliberate engineering decision, not just collecting more data for the
sake of it.

**UCSD Amazon Reviews 2023 (5-core)**, 41 million reviews across all product
categories. The JSON format includes rich metadata: reviewer ID, product ASIN,
rating, review text, helpful votes. This is the primary NLP and sentiment dataset
because of its breadth across categories.

**Kaggle / AWS Customer Reviews (Electronics)**, 3 million Electronics-specific
reviews in TSV format. This dataset has fields the UCSD one does not, `vine`
(whether the reviewer is in Amazon's Vine program), `helpful_votes` separated
from `total_votes`, and `marketplace` (country). It also uses completely different
column names for the same concepts.

The reason both exist in this pipeline is that in any real company, data comes from
multiple systems that were never designed to talk to each other. The UCSD source
calls the rating field `overall`. Kaggle calls it `star_rating`. UCSD uses `True/False`
for verified purchases. Kaggle uses `Y/N`. These conflicts are exactly what the Silver
layer is built to resolve. Building a pipeline that handles one perfectly clean source
is not a useful skill. Building one that reconciles two messy, structurally different
sources into a single unified schema — that is what real data work looks like.

---

## What this pipeline actually does

```
Raw data (2 sources, different formats, different schemas)
    ↓
Bronze layer — store everything exactly as received, no changes
    ↓
Silver layer — clean, deduplicate, enforce types, route bad rows to dead-letter queue
    ↓
Gold layer — NLP sentiment scores, fake review flags, product-level KPIs
    ↓
Dashboards — business intelligence views connected live to PostgreSQL
```

---

## Architecture — Medallion Pattern

```
┌──────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                    │
│  UCSD Amazon Reviews 2023 (JSON.gz)  +  Kaggle Electronics TSV  │
└─────────────────────────┬────────────────────────────────────────┘
                          │  load_bronze.py
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (bronze schema — PostgreSQL)                      │
│  Raw data · Zero transformation · Full audit trail              │
│  Tables: ucsd_reviews · aws_reviews · pipeline_log              │
└─────────────────────────┬────────────────────────────────────────┘
                          │  Phase 2 — Silver pipeline
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (silver schema)                                   │
│  Cleaned · Deduplicated · Type-enforced · Dead-letter queue      │
│  Tables: reviews · rejected_reviews · pipeline_log              │
└─────────────────────────┬────────────────────────────────────────┘
                          │  Phase 3 — dbt Gold models
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (gold schema)                                       │
│  Sentiment scores · Fake review flags · Business KPIs           │
│  Tables: product_metrics · category_benchmarks · sentiment_trends│
└─────────────────────────┬────────────────────────────────────────┘
                          │  Power BI live connection
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│  DASHBOARDS                                                      │
│  Sentiment Overview · Conversion Intel · Fake Review Radar      │
│  Category Benchmarks · Pipeline Observability                   │
└──────────────────────────────────────────────────────────────────┘
```

---

## Phase 1 — Bronze Layer (Complete ✓)

**What was built:** Raw ingestion pipeline loading 44.2 million reviews into
PostgreSQL from two structurally different sources.

**Bronze layer row counts (verified):**

| Source | Rows Loaded | Status |
|--------|-------------|--------|
| UCSD Amazon Reviews 2023 (5-core) | 41,135,700 | ✓ Success |
| Kaggle Electronics Reviews (TSV) | 3,091,024 | ✓ Success |
| **Total** | **44,226,724** | |

> _[Add screenshot of pgAdmin row count query here]_

**Real data quality issues discovered and resolved during ingestion:**

| Issue | Where Found | Root Cause | Fix Applied |
|-------|-------------|------------|-------------|
| Compression mismatch | Kaggle TSV | File was `.tsv.zip` not `.tsv.gz` — pandas routes ZIP through gzip internally and fails | Used Python `zipfile` module to open ZIP and hand inner TSV directly to pandas |
| Field name mismatch | UCSD JSON | UCSD uses `overall`, `reviewText`, `reviewerID` — schema expected `rating`, `text`, `user_id` | Inspected raw file first with `inspect.py`, then mapped actual field names |
| NUL byte characters | UCSD JSON | Some review texts contain `\x00` zero bytes that PostgreSQL refuses to store | Strip `\x00` before insert in `flush_chunk()` |

**Key design decision — why Bronze stores everything as TEXT:**

Every column in the Bronze tables is `TEXT` type, even ratings and timestamps.
This is intentional. If type casting happens at Bronze and the logic is wrong,
the only fix is re-downloading 10GB of data. If it happens at Silver, fixing a
bug means re-running a Python script against Bronze — which is already loaded.
Bronze is the audit trail. It never changes.

**Pipeline log captures every run:**

The `bronze.pipeline_log` table records every execution with row counts, status,
timestamps, and error messages. Failed runs are preserved alongside successful ones.
Every problem that happened during ingestion is permanently documented.

---

## Phase 2 — Silver Layer _(in progress)_

- [ ] Schema normalization — unified `silver.reviews` table across both sources
- [ ] Type enforcement — ratings cast to NUMERIC, timestamps to TIMESTAMPTZ
- [ ] Deduplication — composite key on reviewer_id + asin + timestamp
- [ ] Dead-letter queue — rejected rows routed to `silver.rejected_reviews` with reason tags
- [ ] Pipeline observability — rows in vs rows out at every step
- [ ] Great Expectations validation suite

---

## Phase 3 — Gold Layer and NLP _(planned)_

- [ ] VADER sentiment scoring on all review texts
- [ ] Aspect-based sentiment with spaCy (battery, shipping, quality, price)
- [ ] Fake review risk score per reviewer
- [ ] TF-IDF keyword extraction — what drives 1-star vs 5-star reviews
- [ ] Gold aggregation tables: product_metrics, category_benchmarks, sentiment_trends

---

## Phase 4 — Business Analysis _(planned)_

- [ ] Sentiment-to-sales-rank correlation model
- [ ] Pricing sweet spot analysis by rating band
- [ ] Review velocity as a leading indicator of rank change
- [ ] Category health scorecard

**Key findings will be documented here as Phase 4 completes.**

---

## Phase 5 — Orchestration and Monitoring _(planned)_

- [ ] Apache Airflow DAG — Bronze to Silver to Gold scheduled pipeline
- [ ] Monitoring checks — null rate alerts, row count anomaly detection
- [ ] Great Expectations test suite at Silver ingestion
- [ ] Pipeline alerts table with failure logging

---

## Phase 6 — Dashboards and Delivery _(planned)_

- [ ] Power BI live connection to PostgreSQL Gold layer
- [ ] Sentiment Overview dashboard
- [ ] Conversion Intelligence dashboard
- [ ] Fake Review Radar dashboard
- [ ] Category Benchmarks dashboard
- [ ] Pipeline Observability dashboard
- [ ] Executive summary framed for a category management team

---

## Project Structure

```
amazon-reviews-intelligence-platform/
├── data/
│   └── raw/                    # Source files — gitignored, never committed
│       ├── ucsd/               # kcore_5.json.gz (9.9 GB)
│       └── kaggle/             # amazon_reviews_us_Electronics_v1_00.tsv.zip
├── sql/
│   └── 01_create_schemas.sql   # Creates bronze/silver/gold schemas and tables
├── src/
│   ├── config.py               # All env vars, paths, and constants
│   ├── ingestion/
│   │   ├── load_bronze.py      # Main ingestion pipeline
│   │   ├── download_data.py    # Optional: auto-download raw files
│   │   └── audit_bronze.py     # Post-load data quality audit
│   └── utils/
│       └── logger.py           # Structured logger — console and rotating file
├── logs/                       # Pipeline logs and audit reports (gitignored)
├── inspect.py                  # Quick raw file inspector — run before scripting
├── requirements.txt
├── .env.example
└── README.md
```

---

## Setup

### 1. Clone and install

```bash
git clone https://github.com/YOUR_GITHUB_USERNAME/amazon-reviews-intelligence-platform.git
cd amazon-reviews-intelligence-platform

# Windows
python -m venv venv
venv\Scripts\activate

# Mac / Linux
python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Open .env and fill in your PostgreSQL password
```

### 3. Create PostgreSQL database and schemas

Create a database called `Amazon_Intelligence` in pgAdmin, then open the Query Tool
on that database and run the full contents of `sql/01_create_schemas.sql`.

This creates the `bronze`, `silver`, and `gold` schemas and all tables.

### 4. Download data sources

**Source 1 — UCSD Amazon Reviews 2023:**
Go to `https://amazon-reviews-2023.github.io/` and download the **5-core** file
(9.9 GB). Place it in `data/raw/ucsd/`.

**Source 2 — Kaggle Electronics Reviews:**
Go to `https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset`
and download `amazon_reviews_us_Electronics_v1_00.tsv.zip`. Place it in `data/raw/kaggle/`.

Do not extract the files. The pipeline reads them compressed.

### 5. Inspect raw files before loading

```bash
# Windows
venv\Scripts\python.exe inspect.py

# Mac / Linux
python inspect.py
```

Always inspect before scripting. Field names in documentation are often wrong.
The file never lies.

### 6. Load into Bronze

```bash
# Load Kaggle Electronics first — smaller, good for testing the pipeline
venv\Scripts\python.exe src/ingestion/load_bronze.py --source aws

# Load UCSD 5-core — takes around 40 minutes
venv\Scripts\python.exe src/ingestion/load_bronze.py --source ucsd
```

### 7. Verify Bronze layer in pgAdmin

```sql
SELECT 'ucsd' as source, COUNT(*) as rows FROM bronze.ucsd_reviews
UNION ALL
SELECT 'aws', COUNT(*) FROM bronze.aws_reviews
UNION ALL
SELECT 'pipeline_log entries', COUNT(*) FROM bronze.pipeline_log;
```

---

## Data Sources

| Source | Format | Size | Rows | Link |
|--------|--------|------|------|------|
| UCSD Amazon Reviews 2023 — 5-core | JSON.gz | 9.9 GB | 41,135,700 | https://amazon-reviews-2023.github.io/ |
| Kaggle — Amazon US Electronics | TSV.zip | ~1.2 GB | 3,091,024 | https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset |

---

## Tech Stack

| Layer | Tool | Why this tool |
|-------|------|---------------|
| Database | PostgreSQL | Free, production-grade, Power BI connects natively |
| Ingestion | Python — psycopg2, pandas | Chunked reads handle multi-GB files without running out of memory |
| Transformation | dbt | Version-controlled SQL models, lineage tracking, used in real data teams |
| Orchestration | Apache Airflow | Industry standard for scheduling and monitoring pipelines |
| Data quality | Great Expectations | Automated tests that run inside the pipeline, not after |
| Dashboards | Power BI | Live PostgreSQL connection — not a CSV export |
| NLP | VADER + spaCy | VADER is fast and accurate on consumer review text; spaCy handles aspect extraction |

---

## Tech Decisions Worth Explaining

**Why PostgreSQL and not a cloud warehouse?**
Fully reproducible locally at zero cost. PostgreSQL SQL is nearly identical to
BigQuery and Redshift, migrating to cloud is a config change, not a rewrite.

**Why two datasets with overlapping content?**
Because real companies never have one clean source. The schema conflicts between
UCSD and Kaggle, different field names, different boolean encoding, different
timestamp formats, are exactly what a Silver layer is designed to solve.
That normalization work is the actual skill being demonstrated.

**Why store Bronze as raw TEXT with no type casting?**
If the casting logic at Bronze is wrong, the fix requires re-downloading 10GB.
If it fails at Silver, the fix is re-running a script against Bronze that is
already there. Bronze is the audit trail. Silver is where decisions happen.

**Why a dead-letter queue instead of dropping bad rows?**
Silently dropping rows hides problems. The `silver.rejected_reviews` table
captures every rejected row with a tagged reason, null rating, invalid type,
duplicate key. That table is what you check when downstream metrics suddenly shift.
