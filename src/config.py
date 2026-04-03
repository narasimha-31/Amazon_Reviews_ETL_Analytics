"""
config.py — Central configuration for the Amazon Review Intelligence pipeline.
All environment variables and path constants live here.
Import this module in every script instead of hardcoding values.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# ── Load .env file ────────────────────────────────────────────
load_dotenv()

# ── Project root (works regardless of where script is called from) ──
ROOT_DIR   = Path(__file__).resolve().parent.parent
DATA_DIR   = ROOT_DIR / "data" / "raw"
LOG_DIR    = ROOT_DIR / "logs"
SQL_DIR    = ROOT_DIR / "sql"

# Create directories if they don't exist yet
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# ── PostgreSQL connection string ─────────────────────────────
DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "localhost"),
    "port":     os.getenv("DB_PORT",     "5432"),
    "dbname":   os.getenv("DB_NAME",     "amazon_intelligence"),
    "user":     os.getenv("DB_USER",     "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

DATABASE_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

# ── Data source URLs ─────────────────────────────────────────

# ── SOURCE 1: UCSD Amazon Reviews 2023 ───────────────────────
# Site: https://amazon-reviews-2023.github.io/
# We use the "5-core" file: every user & product has at least 5 reviews.
# 41 million reviews, 9.9 GB — best balance of size and data quality.
# This is a SINGLE file (not split by category like the raw data).
# Download it manually from the site and place it in data/raw/ucsd/
UCSD_5CORE_URL = (
    "https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/"
    "benchmark/5core/All_Amazon_Review_5core.jsonl.gz"
)

# Category-level files (optional — only if you want specific categories)
UCSD_BASE_URL = "https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/raw/review_categories"
UCSD_CATEGORIES = {
    "Electronics":              f"{UCSD_BASE_URL}/Electronics.jsonl.gz",
    "Home_and_Kitchen":         f"{UCSD_BASE_URL}/Home_and_Kitchen.jsonl.gz",
    "Beauty_and_Personal_Care": f"{UCSD_BASE_URL}/Beauty_and_Personal_Care.jsonl.gz",
}

# ── SOURCE 2: Kaggle — Amazon US Customer Reviews ─────────────
# Site: https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset
# Download individual .tsv.gz files manually from Kaggle (requires free account).
# Place downloaded files in data/raw/kaggle/
# Recommended starting file: amazon_reviews_us_Electronics_v1_00.tsv.gz
#
# The loader auto-detects all .tsv.gz files in data/raw/kaggle/
# so just drop whichever files you downloaded there.
KAGGLE_DATA_DIR = DATA_DIR / "kaggle"

# ── Ingestion settings ────────────────────────────────────────
CHUNK_SIZE         = 50_000    # rows per DB insert batch — tune down if memory is tight
DOWNLOAD_TIMEOUT   = 60        # seconds before HTTP timeout
LOG_LEVEL          = os.getenv("LOG_LEVEL", "INFO")
