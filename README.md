# Amazon Review Intelligence
### Sentiment & Conversion Analytics Pipeline

A production-grade end-to-end data pipeline that ingests 130M+ real Amazon
customer reviews, applies NLP sentiment analysis, detects fake reviews, and
delivers business intelligence dashboards for category managers and product teams.

**Built with:** Python · PostgreSQL · Apache Airflow · dbt · VADER/spaCy · Power BI

---

## Architecture — Medallion Pattern

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                             │
│   UCSD Amazon Reviews 2023 (JSONL.gz)  +  AWS TSV Reviews       │
└────────────────────────┬────────────────────────────────────────┘
                         │  download_data.py
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (bronze schema — PostgreSQL)                     │
│  Raw data, zero transformation, full audit trail                │
│  Tables: ucsd_reviews · aws_reviews · pipeline_log             │
└────────────────────────┬────────────────────────────────────────┘
                         │  load_bronze.py → Silver pipeline (Phase 2)
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (silver schema)                                  │
│  Cleaned · Deduplicated · Type-enforced · Dead-letter queue     │
│  Tables: reviews · rejected_reviews · pipeline_log             │
└────────────────────────┬────────────────────────────────────────┘
                         │  dbt models (Phase 3)
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (gold schema)                                      │
│  Business aggregations · NLP scores · KPIs                     │
│  Tables: product_metrics · category_benchmarks · sentiment_trends│
└────────────────────────┬────────────────────────────────────────┘
                         │  Power BI live connection
                         ▼
┌─────────────────────────────────────────────────────────────────┐
│  DASHBOARDS                                                     │
│  Sentiment Overview · Conversion Intel · Fake Review Radar     │
│  Category Benchmarks · Pipeline Observability                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
amazon-review-intelligence/
├── data/raw/               # Downloaded source files (gitignored)
│   ├── ucsd/               # UCSD 2023 JSONL.gz files
│   └── aws/                # AWS TSV.gz files
├── sql/
│   └── 01_create_schemas.sql
├── src/
│   ├── config.py           # All env vars and constants
│   ├── ingestion/
│   │   ├── download_data.py
│   │   ├── load_bronze.py
│   │   └── audit_bronze.py
│   └── utils/
│       └── logger.py
├── logs/                   # Pipeline logs and audit reports
├── requirements.txt
├── .env.example
└── README.md
```

---

## Setup

### 1. Clone and install

```bash
git clone https://github.com/YOUR_USERNAME/amazon-review-intelligence
cd amazon-review-intelligence
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

### 3. Create PostgreSQL database

```bash
psql -U postgres -c "CREATE DATABASE amazon_intelligence;"
psql -U postgres -d amazon_intelligence -f sql/01_create_schemas.sql
```

### 4. Download raw data

```bash
# Download a single category first to test (Electronics is ~4GB)
python src/ingestion/download_data.py --source ucsd --category Electronics

# Download all categories when ready
python src/ingestion/download_data.py --source all
```

### 5. Load into Bronze

```bash
python src/ingestion/load_bronze.py --source ucsd --category Electronics
python src/ingestion/load_bronze.py --source all
```

### 6. Run Bronze audit

```bash
python src/ingestion/audit_bronze.py
# → generates logs/bronze_audit_YYYYMMDD.md
```

---

## Data Sources

| Source | Format | Size | URL |
|--------|--------|------|-----|
| UCSD Amazon Reviews 2023 | JSONL.gz per category | ~50GB total | https://amazon-reviews-2023.github.io/ |
| AWS Customer Reviews | TSV.gz | ~20GB total | https://registry.opendata.aws/amazon-reviews/ |

---

## Key Findings (Phase 4 — updated as analysis progresses)

_To be populated after Phase 4 analysis._

---

## Tech Decisions

**Why PostgreSQL over cloud warehouse?**
Reproducible locally with zero cost. Identical SQL syntax to BigQuery/Redshift — trivial to migrate. Power BI connects natively.

**Why store Bronze as raw text strings?**
Preserves the original data exactly as received. Type casting happens at Silver, not Bronze — this means if our Silver logic is wrong, we can re-run it from Bronze without re-downloading 50GB.

**Why dead-letter queue instead of dropping bad rows?**
Dropping silently hides data quality problems. The rejected_reviews table tells you exactly what broke and why — essential for debugging and for the observability dashboard.
