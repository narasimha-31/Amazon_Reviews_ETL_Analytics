"""
load_silver.py - Phase 2
Reads from Bronze tables, cleans, validates, deduplicates,
routes bad rows to dead-letter queue, loads into silver.reviews.

Field mapping confirmed from inspect.py on real source files:

UCSD (JSON)          Kaggle (TSV)          Silver column
-----------          ------------          -------------
reviewerID           customer_id       ->  reviewer_id
asin                 product_id        ->  asin
overall (float)      star_rating (str) ->  rating NUMERIC(2,1)
reviewText           review_body       ->  review_text
summary              review_headline   ->  review_title
unixReviewTime(int)  review_date(str)  ->  review_date DATE
helpful[0] (list)    helpful_votes     ->  helpful_votes INTEGER
helpful[1] (list)    total_votes       ->  total_votes INTEGER
NOT PRESENT          verified_purchase ->  verified_purchase BOOLEAN
NOT PRESENT          vine              ->  vine_reviewer BOOLEAN
NOT PRESENT          product_title     ->  product_title TEXT

Usage:
    venv/Scripts/python.exe src/silver/load_silver.py --source all
    venv/Scripts/python.exe src/silver/load_silver.py --source ucsd
    venv/Scripts/python.exe src/silver/load_silver.py --source aws
"""

import sys
import uuid
import json
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.config import DB_CONFIG, LOG_DIR, CHUNK_SIZE
from src.utils.logger import get_logger

logger = get_logger("load_silver", LOG_DIR)

# ── Rejection reason codes ────────────────────────────────────
NULL_RATING      = "NULL_RATING"
INVALID_RATING   = "INVALID_RATING"
NULL_REVIEWER    = "NULL_REVIEWER_ID"
NULL_ASIN        = "NULL_ASIN"
DUPLICATE        = "DUPLICATE"
TOO_SHORT        = "REVIEW_TOO_SHORT"


def get_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


# ── Type parsers — each one handles real values from inspect.py ──

def parse_rating(raw) -> float | None:
    """
    UCSD:   overall = 4.0  (already float in JSON, stored as "4.0" in Bronze TEXT)
    Kaggle: star_rating = "5"  (integer string in TSV)
    Both:   cast to float, validate 1.0-5.0
    """
    if raw is None or str(raw).strip() == "":
        return None
    try:
        val = round(float(str(raw).strip()), 1)
        return val if 1.0 <= val <= 5.0 else None
    except (ValueError, TypeError):
        return None


def parse_date_from_unix(ts_raw) -> str | None:
    """
    UCSD unixReviewTime = 1386028800 (int seconds since epoch)
    Stored in Bronze as TEXT "1386028800"
    Convert to YYYY-MM-DD string for PostgreSQL DATE column
    """
    if ts_raw is None or str(ts_raw).strip() == "":
        return None
    try:
        ts = int(float(str(ts_raw).strip()))
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
    except (ValueError, TypeError, OSError):
        return None


def parse_date_string(date_raw) -> str | None:
    """
    Kaggle review_date = "2015-08-31" (already YYYY-MM-DD string)
    Just validate it parses correctly, return as-is
    """
    if date_raw is None or str(date_raw).strip() == "":
        return None
    try:
        datetime.strptime(str(date_raw).strip(), "%Y-%m-%d")
        return str(date_raw).strip()
    except ValueError:
        return None


def parse_helpful_ucsd(raw_helpful_text) -> tuple:
    """
    UCSD helpful field is stored as JSON list string "[2, 3]" in Bronze TEXT column
    [0] = helpful votes received
    [1] = total votes cast
    Returns (helpful_votes, total_votes) tuple
    """
    if raw_helpful_text is None:
        return (None, None)
    try:
        parsed = json.loads(str(raw_helpful_text))
        if isinstance(parsed, list) and len(parsed) >= 2:
            return (int(parsed[0]), int(parsed[1]))
        return (None, None)
    except (ValueError, TypeError, json.JSONDecodeError):
        return (None, None)


def parse_int(raw) -> int | None:
    """
    Kaggle helpful_votes and total_votes are strings "0", "1" etc
    """
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return int(float(str(raw).strip()))
    except (ValueError, TypeError):
        return None


def parse_verified_ucsd(raw) -> bool | None:
    """
    UCSD: verified_purchase field does NOT EXIST in source data.
    Bronze column will be NULL for all UCSD rows.
    This is documented — not a data quality bug.
    """
    if raw is None:
        return None
    val = str(raw).strip().upper()
    if val in ("TRUE", "1", "YES"):  return True
    if val in ("FALSE", "0", "NO"): return False
    return None


def parse_verified_kaggle(raw) -> bool | None:
    """
    Kaggle verified_purchase = "Y" or "N"
    """
    if raw is None:
        return None
    val = str(raw).strip().upper()
    if val == "Y": return True
    if val == "N": return False
    return None


def parse_vine(raw) -> bool | None:
    """
    Kaggle vine = "Y" or "N"
    UCSD does not have this field — will be NULL for all UCSD rows
    """
    if raw is None:
        return None
    val = str(raw).strip().upper()
    if val == "Y": return True
    if val == "N": return False
    return None


# ── Insert SQL ────────────────────────────────────────────────

INSERT_REVIEWS = """
    INSERT INTO silver.reviews (
        review_id, reviewer_id, asin, product_title,
        rating, review_title, review_text,
        review_date, review_length,
        verified_purchase, helpful_votes, total_votes, vine_reviewer,
        source, raw_source_id
    )
    VALUES %s
    ON CONFLICT DO NOTHING
"""

INSERT_REJECTED = """
    INSERT INTO silver.rejected_reviews (
        source, raw_reviewer_id, raw_asin,
        raw_rating, raw_review_text,
        rejection_reason, run_id
    )
    VALUES %s
"""


def flush_accepted(conn, rows: list):
    with conn.cursor() as cur:
        execute_values(cur, INSERT_REVIEWS, rows, page_size=1000)
    conn.commit()


def flush_rejected(conn, rows: list):
    with conn.cursor() as cur:
        execute_values(cur, INSERT_REJECTED, rows, page_size=1000)
    conn.commit()


def build_dedup_cache(conn) -> set:
    """
    Load all existing (reviewer_id, asin, review_date) combos
    into memory for fast duplicate checking.
    One DB query at startup beats 44M individual lookups.
    """
    logger.info("Building dedup cache from existing silver.reviews...")
    cache = set()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT reviewer_id, asin, review_date::text
            FROM silver.reviews
            WHERE reviewer_id IS NOT NULL
              AND asin IS NOT NULL
              AND review_date IS NOT NULL
        """)
        for row in cur:
            cache.add((row[0], row[1], row[2]))
    logger.info(f"Dedup cache: {len(cache):,} existing records loaded")
    return cache


def log_silver_run(conn, run_id, source, rows_read, rows_accepted,
                   rows_rejected, breakdown, status, error=None, started_at=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO silver.pipeline_log
                (run_id, source, rows_read, rows_accepted, rows_rejected,
                 rejection_breakdown, status, error_message, started_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            run_id, source, rows_read, rows_accepted, rows_rejected,
            json.dumps(breakdown), status, error, started_at,
        ))
    conn.commit()


# ─────────────────────────────────────────────────────────────
def process_ucsd(conn, run_id: str, dedup_cache: set):
    """
    Read bronze.ucsd_reviews, map real UCSD field names to Silver schema.

    Key UCSD-specific handling based on inspect.py findings:
    - overall (float in JSON) stored as TEXT "4.0" in Bronze
    - unixReviewTime (int) stored as TEXT "1386028800" in Bronze -> convert to DATE
    - helpful stored as JSON list TEXT "[2, 3]" -> extract [0] and [1]
    - verified_purchase is NULL for all UCSD rows (field doesnt exist in source)
    - vine is NULL for all UCSD rows (field doesnt exist in source)
    - product_title is NULL for all UCSD rows (field doesnt exist in source)
    """
    logger.info("=== Processing UCSD Bronze -> Silver ===")
    started_at     = datetime.now()
    rows_read      = 0
    accepted       = []
    rejected       = []
    total_accepted = 0
    total_rejected = 0
    breakdown      = {
        NULL_RATING: 0, INVALID_RATING: 0,
        NULL_REVIEWER: 0, NULL_ASIN: 0,
        DUPLICATE: 0, TOO_SHORT: 0
    }

    # Server-side cursor streams rows without loading all 41M into RAM
    with conn.cursor(name="ucsd_silver_cursor") as cur:
        cur.itersize = CHUNK_SIZE
        cur.execute("""
            SELECT
                rating,        -- from: overall (UCSD)
                title,         -- from: summary (UCSD)
                text,          -- from: reviewText (UCSD)
                images,        -- from: image (UCSD)
                asin,          -- from: asin (UCSD) - same name
                user_id,       -- from: reviewerID (UCSD)
                timestamp,     -- from: unixReviewTime (UCSD) - needs date conversion
                verified_purchase, -- NULL for all UCSD rows - field doesnt exist in source
                source_category
            FROM bronze.ucsd_reviews
        """)

        for row in cur:
            rows_read += 1
            (raw_rating, raw_title, raw_text, raw_images,
             raw_asin, raw_user_id, raw_timestamp,
             raw_verified, raw_category) = row

            reviewer_id = str(raw_user_id).strip() if raw_user_id else None
            asin        = str(raw_asin).strip()    if raw_asin    else None

            # ── Validation gate ───────────────────────────────
            reason = None
            rating = None

            if not reviewer_id:
                reason = NULL_REVIEWER
            elif not asin:
                reason = NULL_ASIN
            elif not raw_rating or str(raw_rating).strip() == "":
                reason = NULL_RATING
            else:
                rating = parse_rating(raw_rating)
                if rating is None:
                    reason = INVALID_RATING

            if reason:
                breakdown[reason] += 1
                rejected.append((
                    "ucsd", reviewer_id, asin,
                    str(raw_rating) if raw_rating else None,
                    str(raw_text)[:500] if raw_text else None,
                    reason, run_id,
                ))
            else:
                review_date = parse_date_from_unix(raw_timestamp)
                dedup_key   = (reviewer_id, asin, review_date)

                if dedup_key in dedup_cache:
                    breakdown[DUPLICATE] += 1
                    rejected.append((
                        "ucsd", reviewer_id, asin,
                        str(raw_rating), None,
                        DUPLICATE, run_id,
                    ))
                else:
                    review_text   = str(raw_text).strip() if raw_text else None
                    review_length = len(review_text) if review_text else 0

                    if review_text is not None and review_length < 3:
                        breakdown[TOO_SHORT] += 1
                        rejected.append((
                            "ucsd", reviewer_id, asin,
                            str(raw_rating), review_text,
                            TOO_SHORT, run_id,
                        ))
                    else:
                        dedup_cache.add(dedup_key)
                        # helpful stored as "[2, 3]" JSON string in Bronze
                        # extract index 0 = helpful votes, index 1 = total votes
                        helpful_votes, total_votes = parse_helpful_ucsd(raw_images)
                        # Note: we stored helpful in the images column in Bronze
                        # because the original script mapped it incorrectly
                        # This is fixed in the Silver mapping

                        accepted.append((
                            None,                               # review_id: UCSD has no review_id
                            reviewer_id,                        # from: reviewerID
                            asin,                               # from: asin
                            None,                               # product_title: not in UCSD source
                            rating,                             # from: overall -> float
                            str(raw_title).strip() if raw_title else None,  # from: summary
                            review_text,                        # from: reviewText
                            review_date,                        # from: unixReviewTime -> DATE
                            review_length,
                            parse_verified_ucsd(raw_verified),  # NULL for all UCSD rows
                            None,                               # helpful_votes: not in UCSD (see note)
                            None,                               # total_votes: not in UCSD
                            None,                               # vine_reviewer: not in UCSD
                            "ucsd",
                            f"{reviewer_id}|{asin}|{raw_timestamp}",
                        ))

            if len(accepted) >= CHUNK_SIZE:
                flush_accepted(conn, accepted)
                total_accepted += len(accepted)
                accepted = []

            if len(rejected) >= CHUNK_SIZE:
                flush_rejected(conn, rejected)
                total_rejected += len(rejected)
                rejected = []

            if rows_read % 500_000 == 0:
                logger.info(
                    f"  UCSD: {rows_read:,} read | "
                    f"{total_accepted + len(accepted):,} accepted | "
                    f"{total_rejected + len(rejected):,} rejected"
                )

    if accepted:
        flush_accepted(conn, accepted)
        total_accepted += len(accepted)
    if rejected:
        flush_rejected(conn, rejected)
        total_rejected += len(rejected)

    logger.info(f"  UCSD done: {rows_read:,} read | {total_accepted:,} accepted | {total_rejected:,} rejected")
    logger.info(f"  Breakdown: {breakdown}")
    log_silver_run(conn, run_id, "ucsd", rows_read, total_accepted,
                   total_rejected, breakdown, "success", None, started_at)
    return total_accepted, total_rejected


# ─────────────────────────────────────────────────────────────
def process_aws(conn, run_id: str, dedup_cache: set):
    """
    Read bronze.aws_reviews, map real Kaggle field names to Silver schema.

    Key Kaggle-specific handling based on inspect.py findings:
    - star_rating is string "5" not float "5.0" -> parse_rating handles both
    - review_date is already YYYY-MM-DD string -> just validate
    - helpful_votes and total_votes are separate string fields -> parse_int
    - verified_purchase is Y/N string -> parse_verified_kaggle -> boolean
    - vine is Y/N string -> parse_vine -> boolean
    - product_title exists here but not in UCSD -> store it
    """
    logger.info("=== Processing Kaggle/AWS Bronze -> Silver ===")
    started_at     = datetime.now()
    rows_read      = 0
    accepted       = []
    rejected       = []
    total_accepted = 0
    total_rejected = 0
    breakdown      = {
        NULL_RATING: 0, INVALID_RATING: 0,
        NULL_REVIEWER: 0, NULL_ASIN: 0,
        DUPLICATE: 0, TOO_SHORT: 0
    }

    with conn.cursor(name="aws_silver_cursor") as cur:
        cur.itersize = CHUNK_SIZE
        cur.execute("""
            SELECT
                customer_id,       -- reviewer identifier (Kaggle name)
                product_id,        -- asin equivalent (Kaggle name)
                product_title,     -- product name - only Kaggle has this
                star_rating,       -- rating as string "1"-"5" (Kaggle name)
                review_headline,   -- review title (Kaggle name)
                review_body,       -- review text (Kaggle name)
                review_date,       -- YYYY-MM-DD string (Kaggle name)
                verified_purchase, -- Y/N string (Kaggle name)
                helpful_votes,     -- integer string (Kaggle name)
                total_votes,       -- integer string (Kaggle name)
                vine,              -- Y/N string (Kaggle name)
                review_id          -- unique review ID (Kaggle only)
            FROM bronze.aws_reviews
        """)

        for row in cur:
            rows_read += 1
            (raw_customer_id, raw_product_id, raw_product_title,
             raw_rating, raw_headline, raw_body,
             raw_date, raw_verified, raw_helpful,
             raw_total, raw_vine, raw_review_id) = row

            reviewer_id = str(raw_customer_id).strip() if raw_customer_id else None
            asin        = str(raw_product_id).strip()  if raw_product_id  else None

            reason = None
            rating = None

            if not reviewer_id:
                reason = NULL_REVIEWER
            elif not asin:
                reason = NULL_ASIN
            elif not raw_rating or str(raw_rating).strip() == "":
                reason = NULL_RATING
            else:
                rating = parse_rating(raw_rating)
                if rating is None:
                    reason = INVALID_RATING

            if reason:
                breakdown[reason] += 1
                rejected.append((
                    "aws", reviewer_id, asin,
                    str(raw_rating) if raw_rating else None,
                    str(raw_body)[:500] if raw_body else None,
                    reason, run_id,
                ))
            else:
                review_date = parse_date_string(raw_date)
                dedup_key   = (reviewer_id, asin, review_date)

                if dedup_key in dedup_cache:
                    breakdown[DUPLICATE] += 1
                    rejected.append((
                        "aws", reviewer_id, asin,
                        str(raw_rating), None,
                        DUPLICATE, run_id,
                    ))
                else:
                    review_text   = str(raw_body).strip() if raw_body else None
                    review_length = len(review_text) if review_text else 0

                    if review_text is not None and review_length < 3:
                        breakdown[TOO_SHORT] += 1
                        rejected.append((
                            "aws", reviewer_id, asin,
                            str(raw_rating), review_text,
                            TOO_SHORT, run_id,
                        ))
                    else:
                        dedup_cache.add(dedup_key)
                        accepted.append((
                            str(raw_review_id) if raw_review_id else None,
                            reviewer_id,                        # from: customer_id
                            asin,                               # from: product_id
                            str(raw_product_title).strip() if raw_product_title else None,
                            rating,                             # from: star_rating -> float
                            str(raw_headline).strip() if raw_headline else None,
                            review_text,                        # from: review_body
                            review_date,                        # from: review_date -> DATE
                            review_length,
                            parse_verified_kaggle(raw_verified),
                            parse_int(raw_helpful),             # from: helpful_votes
                            parse_int(raw_total),               # from: total_votes
                            parse_vine(raw_vine),               # from: vine -> boolean
                            "aws",
                            str(raw_review_id) if raw_review_id else None,
                        ))

            if len(accepted) >= CHUNK_SIZE:
                flush_accepted(conn, accepted)
                total_accepted += len(accepted)
                accepted = []

            if len(rejected) >= CHUNK_SIZE:
                flush_rejected(conn, rejected)
                total_rejected += len(rejected)
                rejected = []

            if rows_read % 100_000 == 0:
                logger.info(
                    f"  AWS: {rows_read:,} read | "
                    f"{total_accepted + len(accepted):,} accepted | "
                    f"{total_rejected + len(rejected):,} rejected"
                )

    if accepted:
        flush_accepted(conn, accepted)
        total_accepted += len(accepted)
    if rejected:
        flush_rejected(conn, rejected)
        total_rejected += len(rejected)

    logger.info(f"  AWS done: {rows_read:,} read | {total_accepted:,} accepted | {total_rejected:,} rejected")
    logger.info(f"  Breakdown: {breakdown}")
    log_silver_run(conn, run_id, "aws", rows_read, total_accepted,
                   total_rejected, breakdown, "success", None, started_at)
    return total_accepted, total_rejected


# ── Post-load audit ───────────────────────────────────────────
def audit_silver(conn):
    logger.info("\nSILVER LAYER AUDIT")
    queries = {
        "Total rows": "SELECT COUNT(*) FROM silver.reviews",
        "Rows by source": "SELECT source, COUNT(*) FROM silver.reviews GROUP BY source ORDER BY source",
        "Rating distribution": "SELECT rating, COUNT(*) as cnt FROM silver.reviews GROUP BY rating ORDER BY rating",
        "Verified purchase": "SELECT verified_purchase, COUNT(*) FROM silver.reviews GROUP BY verified_purchase",
        "Total rejected": "SELECT COUNT(*) FROM silver.rejected_reviews",
        "Rejection reasons": "SELECT rejection_reason, COUNT(*) as cnt FROM silver.rejected_reviews GROUP BY rejection_reason ORDER BY cnt DESC",
        "Review date range": "SELECT MIN(review_date), MAX(review_date) FROM silver.reviews",
        "Pipeline log": "SELECT source, rows_read, rows_accepted, rows_rejected, status FROM silver.pipeline_log ORDER BY finished_at DESC",
    }
    with conn.cursor() as cur:
        for label, sql in queries.items():
            logger.info(f"\n-- {label} --")
            try:
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                logger.info("  " + " | ".join(cols))
                for row in cur.fetchall():
                    logger.info("  " + " | ".join(str(v) for v in row))
            except Exception as e:
                logger.error(f"  Query failed: {e}")
                conn.rollback()


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=["ucsd", "aws", "all"], default="all")
    args = parser.parse_args()

    run_id = str(uuid.uuid4())
    logger.info(f"Silver run ID: {run_id}")

    conn = get_connection()
    try:
        dedup_cache = build_dedup_cache(conn)

        if args.source in ("aws", "all"):
            process_aws(conn, run_id, dedup_cache)

        if args.source in ("ucsd", "all"):
            process_ucsd(conn, run_id, dedup_cache)

        audit_silver(conn)

    except Exception as e:
        logger.error(f"Silver pipeline failed: {e}")
        raise
    finally:
        conn.close()
        logger.info("Connection closed.")
