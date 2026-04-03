"""
load_bronze.py — Phase 1, Step 2
Reads raw .gz files from data/raw/ and loads them into the
PostgreSQL bronze schema, chunk by chunk, with full audit logging.

This script deliberately does ZERO cleaning.
Every bad value, null, weird string — it all goes in as-is.
The Bronze layer is your immutable audit trail.

Usage:
    python src/ingestion/load_bronze.py --source all
    python src/ingestion/load_bronze.py --source ucsd
    python src/ingestion/load_bronze.py --source aws
    python src/ingestion/load_bronze.py --source ucsd --category Electronics
"""

import argparse
import gzip
import json
import sys
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import zipfile
import io
import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.config import DATA_DIR, DATABASE_URL, DB_CONFIG, CHUNK_SIZE, LOG_DIR
from src.utils.logger import get_logger

logger = get_logger("load_bronze", LOG_DIR)

# ── Columns we pull from each source (in order) ──────────────
UCSD_COLUMNS = [
    "rating", "title", "text", "images",
    "asin", "parent_asin", "user_id",
    "timestamp", "verified_purchase",
]

AWS_COLUMNS = [
    "marketplace", "customer_id", "review_id", "product_id",
    "product_parent", "product_title", "product_category",
    "star_rating", "helpful_votes", "total_votes", "vine",
    "verified_purchase", "review_headline", "review_body", "review_date",
]


# ─────────────────────────────────────────────────────────────
def get_connection():
    """Open a raw psycopg2 connection (faster than SQLAlchemy for bulk loads)."""
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


def log_pipeline_run(conn, run_id, table, source_file, rows_attempted,
                     rows_loaded, rows_rejected, status, error=None, started_at=None):
    """Write a row to bronze.pipeline_log after every file load."""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO bronze.pipeline_log
                (run_id, phase, table_name, source_file,
                 rows_attempted, rows_loaded, rows_rejected,
                 status, error_message, started_at)
            VALUES (%s, 'bronze', %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            run_id, table, str(source_file),
            rows_attempted, rows_loaded, rows_rejected,
            status, error, started_at,
        ))
    conn.commit()


# ─────────────────────────────────────────────────────────────
def load_ucsd_file(conn, gz_path: Path, category: str, run_id: str):
    """
    Parse a UCSD .jsonl.gz file line by line and bulk-insert into
    bronze.ucsd_reviews in CHUNK_SIZE batches.

    Why line-by-line and not pd.read_json?
    The files are JSONL (one JSON object per line), not a JSON array.
    pd.read_json on a large JSONL file loads everything into RAM — bad.
    Line-by-line lets us stream and chunk even multi-GB files.
    """
    logger.info(f"Loading UCSD [{category}] — {gz_path.name}")
    started_at  = datetime.now()
    total_rows  = 0
    loaded_rows = 0
    bad_rows    = 0
    chunk       = []

    INSERT_SQL = """
        INSERT INTO bronze.ucsd_reviews
            (rating, title, text, images, asin, parent_asin,
             user_id, timestamp, verified_purchase, source_category)
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    def clean(val):
        """Strip NUL bytes (0x00) that PostgreSQL refuses to store.
        These appear in ~0.01% of raw Amazon review texts.
        We clean at insert time so Bronze still reflects real source data
        minus only the characters that make storage impossible."""
        if val is None:
            return None
        if isinstance(val, str):
            return val.replace("\x00", "").replace("\u0000", "")
        return val

    def flush_chunk(rows):
        nonlocal loaded_rows
        # Strip NUL bytes from every string field in every row before inserting
        cleaned = [tuple(clean(v) for v in row) for row in rows]
        with conn.cursor() as cur:
            execute_values(cur, INSERT_SQL, cleaned, page_size=1000)
        conn.commit()
        loaded_rows += len(cleaned)

    try:
        with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                total_rows += 1
                try:
                    obj = json.loads(line.strip())

                    # UCSD 5-core field name mapping:
                    # "overall"         → rating
                    # "reviewText"      → text
                    # "summary"         → title
                    # "reviewerID"      → user_id
                    # "unixReviewTime"  → timestamp
                    # "verified"        → verified_purchase
                    # "image"           → images
                    # "asin"            → asin  (same in both sources)
                    # "style"           → ignored (variant metadata)
                    rating_val = obj.get("overall") or obj.get("rating")
                    row = (
                        str(rating_val)                        if rating_val is not None else None,
                        obj.get("summary")  or obj.get("title"),
                        obj.get("reviewText") or obj.get("text"),
                        json.dumps(obj.get("image") or obj.get("images")) if (obj.get("image") or obj.get("images")) is not None else None,
                        obj.get("asin"),
                        obj.get("parent_asin"),
                        obj.get("reviewerID") or obj.get("user_id"),
                        str(obj.get("unixReviewTime") or obj.get("timestamp")) if (obj.get("unixReviewTime") or obj.get("timestamp")) is not None else None,
                        str(obj.get("verified") or obj.get("verified_purchase")) if (obj.get("verified") or obj.get("verified_purchase")) is not None else None,
                        category,
                    )
                    chunk.append(row)

                    if len(chunk) >= CHUNK_SIZE:
                        flush_chunk(chunk)
                        chunk = []
                        logger.info(
                            f"  {category}: {loaded_rows:,} rows loaded "
                            f"(~{loaded_rows/total_rows*100:.1f}% good so far)"
                        )

                except (json.JSONDecodeError, KeyError) as e:
                    bad_rows += 1
                    if bad_rows <= 5:   # log first 5 bad lines, not thousands
                        logger.warning(f"  Bad JSON line #{total_rows}: {e}")

        # flush remaining rows
        if chunk:
            flush_chunk(chunk)

        status = "success" if bad_rows == 0 else "partial"
        logger.info(
            f"  DONE {category}: {loaded_rows:,} loaded, "
            f"{bad_rows:,} bad lines, {total_rows:,} total"
        )

    except Exception as e:
        status = "failed"
        logger.error(f"  FAILED {category}: {e}")
        bad_rows = total_rows - loaded_rows
        log_pipeline_run(
            conn, run_id, "bronze.ucsd_reviews", gz_path,
            total_rows, loaded_rows, bad_rows, status, str(e), started_at,
        )
        raise

    log_pipeline_run(
        conn, run_id, "bronze.ucsd_reviews", gz_path,
        total_rows, loaded_rows, bad_rows, status, None, started_at,
    )
    return loaded_rows, bad_rows


# ─────────────────────────────────────────────────────────────
def load_aws_file(conn, gz_path: Path, run_id: str):
    """
    Parse an AWS TSV .gz file using pandas read_csv in chunks.
    TSV is tabular so chunked read_csv is safe and memory-efficient.
    """
    logger.info(f"Loading AWS — {gz_path.name}")
    started_at  = datetime.now()
    total_rows  = 0
    loaded_rows = 0
    bad_rows    = 0

    INSERT_SQL = """
        INSERT INTO bronze.aws_reviews
            (marketplace, customer_id, review_id, product_id,
             product_parent, product_title, product_category,
             star_rating, helpful_votes, total_votes, vine,
             verified_purchase, review_headline, review_body, review_date)
        VALUES %s
        ON CONFLICT DO NOTHING
    """


    try:
        ext = gz_path.suffix.lower()

        if ext == ".zip":
            # Kaggle ships .tsv.zip — open with zipfile, hand inner TSV to pandas
            # Do NOT use compression="zip" in pandas — it routes through gzip and fails
            import zipfile, io
            zf       = zipfile.ZipFile(gz_path, "r")
            tsv_name = next(n for n in zf.namelist() if n.endswith(".tsv"))
            logger.info(f"  ZIP contains: {tsv_name}")
            file_obj = io.TextIOWrapper(zf.open(tsv_name), encoding="utf-8", errors="replace")
            reader   = pd.read_csv(
                file_obj,
                sep="\t",
                chunksize=CHUNK_SIZE,
                dtype=str,
                on_bad_lines="warn",
                low_memory=False,
            )
        else:
            # Standard .tsv.gz
            reader = pd.read_csv(
                gz_path,
                sep="\t",
                compression="gzip",
                chunksize=CHUNK_SIZE,
                dtype=str,
                on_bad_lines="warn",
                encoding="utf-8",
                encoding_errors="replace",
                low_memory=False,
            )


        for chunk_df in reader:
            total_rows += len(chunk_df)

            # Normalise column names to lowercase
            chunk_df.columns = [c.lower().strip() for c in chunk_df.columns]

            # Only keep the columns we care about; fill any missing cols with None
            rows = []
            for _, row in chunk_df.iterrows():
                rows.append(tuple(
                    row.get(col, None) for col in AWS_COLUMNS
                ))

            try:
                with conn.cursor() as cur:
                    execute_values(cur, INSERT_SQL, rows, page_size=1000)
                conn.commit()
                loaded_rows += len(rows)
            except Exception as e:
                bad_rows += len(rows)
                logger.warning(f"  Chunk insert error: {e} — skipping chunk")
                conn.rollback()

            logger.info(f"  AWS: {loaded_rows:,} rows loaded so far")

        status = "success" if bad_rows == 0 else "partial"
        logger.info(f"  DONE AWS: {loaded_rows:,} loaded, {bad_rows} bad rows")

    except Exception as e:
        status = "failed"
        logger.error(f"  FAILED AWS: {e}")
        log_pipeline_run(
            conn, run_id, "bronze.aws_reviews", gz_path,
            total_rows, loaded_rows, bad_rows, status, str(e), started_at,
        )
        raise

    log_pipeline_run(
        conn, run_id, "bronze.aws_reviews", gz_path,
        total_rows, loaded_rows, bad_rows, status, None, started_at,
    )
    return loaded_rows, bad_rows


# ─────────────────────────────────────────────────────────────
def audit_bronze(conn):
    """
    After loading: print a full audit of what's in the Bronze layer.
    This is what you screenshot for your README and portfolio.
    """
    logger.info("\n" + "═"*55)
    logger.info("BRONZE LAYER AUDIT")
    logger.info("═"*55)

    queries = {
        "ucsd_reviews total rows":     "SELECT COUNT(*) FROM bronze.ucsd_reviews",
        "ucsd_reviews by category":    """
            SELECT source_category, COUNT(*) as rows
            FROM bronze.ucsd_reviews
            GROUP BY source_category ORDER BY rows DESC
        """,
        "ucsd null rating %":          """
            SELECT ROUND(SUM(CASE WHEN rating IS NULL THEN 1 ELSE 0 END)::numeric
                   / COUNT(*) * 100, 2) AS null_pct
            FROM bronze.ucsd_reviews
        """,
        "ucsd null review text %":     """
            SELECT ROUND(SUM(CASE WHEN text IS NULL OR text = '' THEN 1 ELSE 0 END)::numeric
                   / COUNT(*) * 100, 2) AS null_pct
            FROM bronze.ucsd_reviews
        """,
        "aws_reviews total rows":      "SELECT COUNT(*) FROM bronze.aws_reviews",
        "aws_reviews by category":     """
            SELECT product_category, COUNT(*) as rows
            FROM bronze.aws_reviews
            GROUP BY product_category ORDER BY rows DESC
        """,
        "pipeline log summary":        """
            SELECT phase, table_name, status,
                   SUM(rows_loaded) as total_loaded,
                   SUM(rows_rejected) as total_rejected,
                   MAX(finished_at) as last_run
            FROM bronze.pipeline_log
            GROUP BY phase, table_name, status
        """,
    }

    with conn.cursor() as cur:
        for label, sql in queries.items():
            logger.info(f"\n── {label} ──")
            try:
                cur.execute(sql)
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
                logger.info("  " + " | ".join(cols))
                for r in rows:
                    logger.info("  " + " | ".join(str(v) for v in r))
            except Exception as e:
                logger.error(f"  Query failed: {e}")
                conn.rollback()

    logger.info("\n" + "═"*55)


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load raw files into Bronze layer")
    parser.add_argument("--source",   choices=["ucsd", "aws", "all"], default="all")
    parser.add_argument("--category", default=None,
                        help="Load only one UCSD category, e.g. Electronics")
    args = parser.parse_args()

    run_id = str(uuid.uuid4())
    logger.info(f"Pipeline run ID: {run_id}")

    conn = get_connection()

    try:
        if args.source in ("ucsd", "all"):
            ucsd_dir = DATA_DIR / "ucsd"
            # Match both .jsonl.gz (category files) and .json.gz (5-core file)
            all_gz = sorted(list(ucsd_dir.glob("*.jsonl.gz")) + list(ucsd_dir.glob("*.json.gz")))
            files = (
                [f for f in all_gz if args.category.lower() in f.name.lower()]
                if args.category
                else all_gz
            )
            if not files:
                logger.warning(f"No .json.gz or .jsonl.gz files found in {ucsd_dir}")
            for gz_path in files:
                # For 5-core file (kcore_5.json.gz) use "5core" as category label
                # For category files (Electronics.jsonl.gz) use the filename as label
                stem = gz_path.name.replace(".jsonl.gz", "").replace(".json.gz", "")
                category = stem if stem else "5core"
                load_ucsd_file(conn, gz_path, category, run_id)

        if args.source in ("aws", "all"):
            # Kaggle TSV files live in data/raw/kaggle/
            kaggle_dir = DATA_DIR / "kaggle"
            # Match both .tsv.gz and .tsv.zip (Kaggle sometimes ships as .zip)
            tsv_files = sorted(
                list(kaggle_dir.glob("*.tsv.gz")) +
                list(kaggle_dir.glob("*.tsv.zip")) +
                list(kaggle_dir.glob("*.tsv"))
            ) if kaggle_dir.exists() else []

            if not tsv_files:
                logger.warning("No TSV files found in data/raw/kaggle/")
                logger.warning("Download Electronics TSV from Kaggle and place it there.")
            else:
                for gz_path in tsv_files:
                    load_aws_file(conn, gz_path, run_id)

        audit_bronze(conn)

    finally:
        conn.close()
        logger.info("Connection closed.")
