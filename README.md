# Amazon Reviews Intelligence Platform

> End-to-end data pipeline and analytics project built on 44 million real, raw
> Amazon customer reviews — from messy source files to a business intelligence
> dashboard a category manager could actually use.

---

## Why I built this

I wanted a portfolio project that reflects what data analysts and engineers
actually do inside a company — not a cleaned Kaggle CSV loaded into a Jupyter
notebook with three charts.

The problem is real: Amazon sellers, category managers, and marketplace teams
sit on millions of customer reviews but have no systematic way to answer the
questions that matter — which product attributes correlate with higher sales
rank? Where are the fake reviews concentrated? What is the gap between what a
star rating says and what the review text actually says? A product with 4.8
stars and terrible review sentiment is a ticking churn problem. A product with
3.9 stars and overwhelmingly positive review text is probably underpriced and
underselling. That gap is where the business value lives.

So I built a pipeline that ingests the raw data, cleans it properly, runs NLP
on the text, and surfaces the insights in a dashboard a category manager would
actually use week to week.

---

## Why two data sources?

This is a deliberate engineering decision, not just collecting more data.

**UCSD Amazon Reviews 2023 (5-core)** — 41 million reviews across all product
categories in JSON format. Rich metadata: reviewer ID, ASIN, rating, review
text, helpful votes. Primary NLP and sentiment dataset because of its breadth.

**Kaggle / AWS Customer Reviews (Electronics)** — 3 million Electronics reviews
in TSV format. Has fields UCSD lacks: `vine` reviewer flag, separated
`helpful_votes` and `total_votes`, and `product_title`. Uses completely different
column names for the same concepts.

In any real company, data comes from multiple systems that were never designed
to talk to each other. UCSD calls the rating field `overall`. Kaggle calls it
`star_rating`. UCSD stores verified purchases as `True/False`. Kaggle uses `Y/N`.
UCSD timestamps are Unix epoch integers. Kaggle dates are `YYYY-MM-DD` strings.
These conflicts are exactly what the Silver layer resolves. Building a pipeline
that handles two messy, structurally different sources into one unified schema
is the actual skill being demonstrated.

---

## Architecture — Medallion Pattern

```
┌──────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                    │
│  UCSD Amazon Reviews 2023 (JSON.gz) + Kaggle Electronics TSV    │
└─────────────────────────┬────────────────────────────────────────┘
                          │  src/ingestion/load_bronze.py
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (bronze schema — PostgreSQL)                      │
│  Raw data · Zero transformation · Full audit trail              │
│  Tables: ucsd_reviews · aws_reviews · pipeline_log              │
└─────────────────────────┬────────────────────────────────────────┘
                          │  src/silver/load_silver.py
                          ▼
┌──────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (silver schema)                                   │
│  Cleaned · Typed · Deduplicated · Dead-letter queue             │
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

## Phase 1 — Bronze Layer ✓ Complete

Raw ingestion pipeline loading 44.2 million reviews into PostgreSQL from two
structurally different sources. Nothing cleaned, nothing transformed —
everything stored exactly as received.

**Final row counts:**

| Source | Rows Loaded | Status |
|--------|-------------|--------|
| UCSD Amazon Reviews 2023 (5-core JSON) | 41,135,700 | ✓ Success |
| Kaggle Electronics Reviews (TSV.zip) | 3,091,024 | ✓ Success |
| **Total Bronze** | **44,226,724** | |

> _[Screenshot: pgAdmin showing bronze row counts]_

**Real data quality issues discovered and resolved during Bronze ingestion:**

| Issue | Source | Root Cause | Fix |
|-------|--------|------------|-----|
| Compression mismatch | Kaggle | File was `.tsv.zip` — pandas routes ZIP through gzip and fails | Used Python `zipfile` module, extracted inner TSV, handed directly to pandas |
| Field name mismatch | UCSD | UCSD uses `overall`, `reviewText`, `reviewerID` — schema expected `rating`, `text`, `user_id` | Ran `inspect.py` on raw file first, mapped actual field names |
| NUL byte characters | UCSD | Some review texts contain `\x00` that PostgreSQL refuses to store | Strip `\x00` in `flush_chunk()` before insert |
| Column misalignment | Kaggle | 24 rows had tab characters inside review text — columns shifted right in TSV | Rows loaded with NULL review_date, documented and excluded from NLP |

**Why Bronze stores everything as TEXT:**

If type casting at Bronze has a bug, the fix is re-downloading 10GB of data.
If it fails at Silver, the fix is re-running a Python script against Bronze
which is already there. Bronze is the immutable audit trail. Silver is where
decisions happen.

---

## Phase 2 — Silver Layer ✓ Complete

Cleaning, validation, schema unification, and dead-letter queue.
Two separate database connections per source — one for reading Bronze via a
named server-side cursor, one for writing to Silver — preventing transaction
conflicts during 44M row streaming.

**Field mapping from real source files (confirmed by inspect.py):**

| UCSD field | Kaggle field | Silver column | Notes |
|------------|--------------|---------------|-------|
| reviewerID | customer_id | reviewer_id | Different names, same concept |
| asin | product_id | asin | Different names, same concept |
| overall (float) | star_rating (str) | rating NUMERIC(2,1) | Different names AND types |
| reviewText | review_body | review_text | Different names |
| summary | review_headline | review_title | Different names |
| unixReviewTime (int) | review_date (str) | review_date DATE | Completely different formats |
| helpful[0] (list) | helpful_votes | helpful_votes INTEGER | UCSD list vs Kaggle string |
| helpful[1] (list) | total_votes | total_votes INTEGER | Same list, different index |
| (not present) | verified_purchase | verified_purchase BOOLEAN | NULL for all UCSD rows |
| (not present) | vine | vine_reviewer BOOLEAN | NULL for all UCSD rows |
| (not present) | product_title | product_title TEXT | NULL for all UCSD rows |

**Final Silver layer numbers:**

| Metric | Value |
|--------|-------|
| Total rows in silver.reviews | 44,219,395 |
| AWS (Kaggle) accepted | 3,084,789 |
| UCSD accepted | 41,134,606 |
| Total rejected to dead-letter queue | 7,329 |

**Dead-letter queue breakdown — silver.rejected_reviews:**

| Rejection Reason | Count | What it means |
|-----------------|-------|---------------|
| REVIEW_TOO_SHORT | 6,618 | Review text under 3 characters — useless for NLP |
| DUPLICATE | 711 | Same reviewer + ASIN + date already in Silver |
| NULL_RATING | 0 | All ratings present — clean source |
| INVALID_RATING | 0 | All ratings valid 1.0–5.0 |
| NULL_REVIEWER_ID | 0 | No orphaned reviews |
| NULL_ASIN | 0 | No unlinked products |

**Key findings from Phase 2:**

Rating distribution across 44M reviews shows strong positive bias — 59.1% of
all reviews are 5 stars. This skew is critical context for Phase 3 sentiment
analysis. A 3-star review in this dataset is actually negative relative to the
baseline.

UCSD had zero duplicates (pre-filtered by researchers). Kaggle had 711
duplicates — Amazon does not fully deduplicate their own TSV exports.

verified_purchase is NULL for all 41M UCSD rows — this field simply does not
exist in the UCSD source data. Documented, not a data quality bug.

**Date range of the combined dataset: 1996-05-20 to 2015-08-31**

Null rates in Silver: review_text 0.02%, review_date 0.00%.

> _[Screenshot: Silver audit output showing row counts and rejection breakdown]_

---

## Phase 3 — Gold Layer & NLP _(in progress)_

- [ ] VADER sentiment scoring on all review texts
- [ ] Aspect-based sentiment with spaCy (battery, shipping, quality, price)
- [ ] Fake review risk score per reviewer
- [ ] TF-IDF keyword extraction — what actually drives 1-star vs 5-star reviews
- [ ] Gold aggregation tables: product_metrics, category_benchmarks, sentiment_trends

---

## Phase 4 — Business Analysis _(planned)_

- [ ] Sentiment-to-sales-rank correlation model
- [ ] Pricing sweet spot analysis by rating band
- [ ] Review velocity as a leading indicator of rank change
- [ ] Category health scorecard with $ impact estimates

**Key findings will be documented here as Phase 4 completes.**

---

## Phase 5 — Orchestration & Monitoring _(planned)_

- [ ] Apache Airflow DAG — scheduled Bronze to Silver to Gold pipeline
- [ ] Monitoring checks — null rate alerts, row count anomaly detection
- [ ] Great Expectations test suite running at Silver ingestion
- [ ] Pipeline alerts table with failure logging

---

## Phase 6 — Dashboards & Delivery _(planned)_

- [ ] Power BI live connection to PostgreSQL Gold layer (no CSV exports)
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
│   └── raw/                          # Source files — gitignored
│       ├── ucsd/                     # kcore_5.json.gz (9.9 GB)
│       └── kaggle/                   # amazon_reviews_us_Electronics_v1_00.tsv.zip
├── sql/
│   ├── 01_create_schemas.sql         # Creates bronze/silver/gold schemas
│   └── 02_create_silver.sql          # Silver layer tables and indexes
├── src/
│   ├── config.py                     # All env vars, paths, constants
│   ├── ingestion/
│   │   ├── load_bronze.py            # Phase 1: raw ingestion pipeline
│   │   ├── audit_bronze.py           # Post-load data quality audit
│   │   └── download_data.py          # Optional: auto-download raw files
│   ├── silver/
│   │   └── load_silver.py            # Phase 2: cleaning, typing, dedup, dead-letter
│   └── utils/
│       └── logger.py                 # Structured logger — console + rotating file
├── screenshots/                      # Evidence screenshots for README
├── logs/                             # Pipeline logs (gitignored)
├── preview_sources.py                        # Inspect raw files before scripting — always run first
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
# Open .env and fill in your PostgreSQL credentials
```

### 3. Create PostgreSQL database and schemas

Create a database called `Amazon_Intelligence` in pgAdmin. Then run each SQL
file in the Query Tool:

```
sql/01_create_schemas.sql   -- Bronze, Silver, Gold schemas + Bronze tables
sql/02_create_silver.sql    -- Silver tables and indexes
```

### 4. Inspect raw files before loading

```bash
# Always run this before touching the ingestion scripts
# It reads the actual field names from both source files
venv\Scripts\python.exe preview_sources.py
```

The rule: never write an ingestion script without inspecting the real file
first. Documentation lies. The file never does.

### 5. Download data sources

**UCSD Amazon Reviews 2023:**
Go to `https://amazon-reviews-2023.github.io/` and download the **5-core** file
(9.9 GB). Place in `data/raw/ucsd/`.

**Kaggle Electronics Reviews:**
Go to `https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset`
and download `amazon_reviews_us_Electronics_v1_00.tsv.zip`. Place in `data/raw/kaggle/`.

Do not extract the files. The pipeline reads them compressed.

### 6. Load Bronze layer

```bash
# Load Kaggle first — smaller, faster to test the pipeline
venv\Scripts\python.exe src/ingestion/load_bronze.py --source aws

# Load UCSD — takes around 40 minutes
venv\Scripts\python.exe src/ingestion/load_bronze.py --source ucsd
```

### 7. Load Silver layer

```bash
# Load Kaggle through Silver first
venv\Scripts\python.exe src/silver/load_silver.py --source aws

# Load UCSD through Silver — takes around 90 minutes
venv\Scripts\python.exe src/silver/load_silver.py --source ucsd
```

### 8. Verify in pgAdmin

```sql
-- Bronze verification
SELECT 'ucsd' as source, COUNT(*) as rows FROM bronze.ucsd_reviews
UNION ALL
SELECT 'aws', COUNT(*) FROM bronze.aws_reviews;

-- Silver verification
SELECT source, COUNT(*) as rows,
       ROUND(AVG(rating), 2) as avg_rating,
       MIN(review_date) as earliest,
       MAX(review_date) as latest
FROM silver.reviews
GROUP BY source;

-- Rejection breakdown
SELECT rejection_reason, COUNT(*) as cnt
FROM silver.rejected_reviews
GROUP BY rejection_reason
ORDER BY cnt DESC;
```

---

## Data Sources

| Source | Format | Size | Rows | Link |
|--------|--------|------|------|------|
| UCSD Amazon Reviews 2023 — 5-core | JSON.gz | 9.9 GB | 41,135,700 | https://amazon-reviews-2023.github.io/ |
| Kaggle Amazon US Electronics | TSV.zip | ~1.2 GB | 3,091,024 | https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset |

---

## Tech Stack

| Layer | Tool | Why |
|-------|------|-----|
| Database | PostgreSQL | Free, production-grade, Power BI connects natively, same SQL as BigQuery |
| Ingestion | Python — psycopg2, pandas | Chunked server-side cursors handle 44M rows without OOM |
| Transformation | dbt | Version-controlled SQL models, lineage, used in real teams |
| Orchestration | Apache Airflow | Industry standard scheduling and monitoring |
| Data quality | Great Expectations | Automated tests inside the pipeline, not after |
| Dashboards | Power BI | Live PostgreSQL connection — no CSV export |
| NLP | VADER + spaCy | VADER fast and accurate on consumer text; spaCy for aspect extraction |

---

## Tech Decisions

**Why PostgreSQL over a cloud warehouse?**
Fully reproducible locally at zero cost. PostgreSQL SQL is nearly identical to
BigQuery and Redshift — migrating to cloud is a config change, not a rewrite.

**Why two connections per Silver process?**
Named server-side cursors in psycopg2 require their transaction to stay open
for the entire stream. `conn.commit()` inside the write loop would invalidate
the cursor mid-stream. Separating read and write connections lets each do its
job without conflicting. One connection reads Bronze rows continuously, another
commits Silver rows in chunks of 50,000.

**Why Bronze stores everything as TEXT?**
If type casting at Bronze is wrong, the fix requires re-downloading 10GB.
If it fails at Silver, the fix is re-running a script against Bronze which is
already loaded. Bronze is immutable. Silver is where all decisions happen.

**Why a dead-letter queue instead of dropping bad rows?**
Silently dropping rows hides problems. `silver.rejected_reviews` captures every
rejected row with a specific reason code. When downstream metrics shift
unexpectedly, that table is the first place you look.

**Why inspect the raw file before writing ingestion code?**
Every dataset has its own field names, types, and quirks. Source documentation
is frequently wrong or outdated. Running `inspect.py` on the actual file before
writing a single line of ingestion code saved hours of debugging — discovered
that UCSD uses `overall` not `rating`, `reviewerID` not `user_id`, and stores
helpful votes as a list `[2, 3]` rather than two separate fields.
