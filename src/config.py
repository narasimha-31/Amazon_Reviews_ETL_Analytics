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


KAGGLE_DATA_DIR = DATA_DIR / "kaggle"

# ── Ingestion settings ────────────────────────────────────────
CHUNK_SIZE         = 50_000    # rows per DB insert batch — tune down if memory is tight
DOWNLOAD_TIMEOUT   = 60        # seconds before HTTP timeout
LOG_LEVEL          = os.getenv("LOG_LEVEL", "INFO")
