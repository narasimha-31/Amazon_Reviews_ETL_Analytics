import sys
import uuid
import json
import argparse
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.config import DB_CONFIG, LOG_DIR, CHUNK_SIZE
from src.utils.logger import get_logger

logger = get_logger("load_silver", LOG_DIR)


# ── Source identifiers ────────────────────────────────────────
SOURCE_UCSD = "ucsd"
SOURCE_AWS  = "aws"

# ── Rejection reason codes ────────────────────────────────────
NULL_RATING    = "NULL_RATING"
INVALID_RATING = "INVALID_RATING"
NULL_REVIEWER  = "NULL_REVIEWER_ID"
NULL_ASIN      = "NULL_ASIN"
DUPLICATE      = "DUPLICATE"
TOO_SHORT      = "REVIEW_TOO_SHORT"

# ── Tuning constants ────────
PAGE_SIZE              = 1000     
MIN_REVIEW_LENGTH      = 3        
MAX_REJECTION_TEXT     = 500      
DATE_FORMAT            = "%Y-%m-%d"
UCSD_PROGRESS_INTERVAL = 500_000  
AWS_PROGRESS_INTERVAL  = 100_000  

# ── SQL statements ───────────
INSERT_REVIEWS = """
    INSERT INTO silver.reviews (
        review_id, reviewer_id, asin, product_title,
        rating, review_title, review_text,
        review_date, review_length,
        verified_purchase, helpful_votes, total_votes, vine_reviewer,
        source, raw_source_id
    ) VALUES %s
    ON CONFLICT DO NOTHING
"""

INSERT_REJECTED = """
    INSERT INTO silver.rejected_reviews (
        source, raw_reviewer_id, raw_asin,
        raw_rating, raw_review_text,
        rejection_reason, run_id
    ) VALUES %s
"""

INSERT_LOG = """
    INSERT INTO silver.pipeline_log (
        run_id, source, rows_read, rows_accepted, rows_rejected,
        rejection_breakdown, status, error_message, started_at
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""


# ── Connection factory ──────────────
def make_connection():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


# ── Type parsers ─────────────────
def parse_rating(raw):
    """
    UCSD overall = 4.0 stored as TEXT "4.0" in Bronze.
    Kaggle star_rating = "5" integer string.
    Cast to float, validate range 1.0 to 5.0.
    """
    if raw is None or str(raw).strip() == "":
        return None
    try:
        val = round(float(str(raw).strip()), 1)
        return val if 1.0 <= val <= 5.0 else None
    except (ValueError, TypeError):
        return None


def parse_unix_to_date(ts_raw):
    """
    UCSD unixReviewTime = 1386028800 stored as TEXT in Bronze.
    Convert epoch seconds to YYYY-MM-DD string.
    """
    if ts_raw is None or str(ts_raw).strip() == "":
        return None
    try:
        ts = int(float(str(ts_raw).strip()))
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(DATE_FORMAT)
    except (ValueError, TypeError, OSError):
        return None


def parse_date_string(raw):
    """
    Kaggle review_date = "2015-08-31" already in YYYY-MM-DD format.
    Validate it parses correctly, return as-is.
    """
    if raw is None or str(raw).strip() == "":
        return None
    try:
        datetime.strptime(str(raw).strip(), DATE_FORMAT)
        return str(raw).strip()
    except ValueError:
        return None


def parse_helpful_list(raw):
    """
    UCSD helpful field stored as JSON list string "[2, 3]" in Bronze.
    Index 0 = helpful votes received, index 1 = total votes cast.
    Returns (helpful_votes, total_votes).
    """
    if raw is None:
        return None, None
    try:
        parsed = json.loads(str(raw))
        if isinstance(parsed, list) and len(parsed) >= 2:
            return int(parsed[0]), int(parsed[1])
        return None, None
    except (ValueError, TypeError, json.JSONDecodeError):
        return None, None


def parse_int(raw):
    """Kaggle helpful_votes and total_votes are integer strings."""
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return int(float(str(raw).strip()))
    except (ValueError, TypeError):
        return None


def parse_bool_truefalse(raw):
    """
    UCSD verified field: "True" / "False" / None.
    Note: verified_purchase does NOT exist in UCSD source —
    this will always return None for UCSD rows. Documented, not a bug.
    """
    if raw is None:
        return None
    val = str(raw).strip().upper()
    if val in ("TRUE",  "1", "YES"): return True
    if val in ("FALSE", "0", "NO"):  return False
    return None


def parse_bool_yn(raw):
    """Kaggle verified_purchase and vine fields: "Y" / "N"."""
    if raw is None:
        return None
    val = str(raw).strip().upper()
    if val == "Y": return True
    if val == "N": return False
    return None


def clean_text(raw):
    """Strip whitespace and return None for empty strings."""
    if raw is None:
        return None
    val = str(raw).strip()
    return val if val else None


# ── Write helpers (use write_conn which commits freely) ───────
def flush_accepted(write_conn, rows):
    with write_conn.cursor() as cur:
        execute_values(cur, INSERT_REVIEWS, rows, page_size=PAGE_SIZE)
    write_conn.commit()


def flush_rejected(write_conn, rows):
    with write_conn.cursor() as cur:
        execute_values(cur, INSERT_REJECTED, rows, page_size=PAGE_SIZE)
    write_conn.commit()


def write_pipeline_log(write_conn, run_id, source, rows_read,
                       rows_accepted, rows_rejected, breakdown,
                       status, error, started_at):
    with write_conn.cursor() as cur:
        cur.execute(INSERT_LOG, (
            run_id, source, rows_read, rows_accepted, rows_rejected,
            json.dumps(breakdown), status, error, started_at,
        ))
    write_conn.commit()


# ── Dedup cache ───────────────────────────────────────────────
def build_dedup_cache(conn):
    """
    Load existing (reviewer_id, asin, review_date) tuples into a Python set.
    One query at startup vs 44M individual lookups = massive speed difference.
    Memory: ~150 bytes per tuple. Safe for 44M rows on a 12GB machine.
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
    conn.commit()
    logger.info(f"Dedup cache: {len(cache):,} existing records loaded")
    return cache


# ── Core validation logic (shared by both sources) ───────────
def validate_row(reviewer_id, asin, raw_rating):
    """
    Returns (rating_float, rejection_reason).
    If rejection_reason is not None, the row goes to dead-letter queue.
    """
    if not reviewer_id:
        return None, NULL_REVIEWER
    if not asin:
        return None, NULL_ASIN
    if raw_rating is None or str(raw_rating).strip() == "":
        return None, NULL_RATING
    rating = parse_rating(raw_rating)
    if rating is None:
        return None, INVALID_RATING
    return rating, None


# ── Process AWS (Kaggle) ──────────────────────────────────────
def process_aws(run_id, dedup_cache):
    """
    Two connections:
      read_conn  - named cursor streams bronze.aws_reviews, never commits
      write_conn - inserts into silver tables, commits per chunk
    """
    logger.info("=== Processing AWS/Kaggle Bronze -> Silver ===")
    started_at     = datetime.now()
    rows_read      = 0
    accepted       = []
    rejected       = []
    total_accepted = 0
    total_rejected = 0
    breakdown      = {k: 0 for k in [NULL_RATING, INVALID_RATING,
                                      NULL_REVIEWER, NULL_ASIN,
                                      DUPLICATE, TOO_SHORT]}

    read_conn  = make_connection()
    write_conn = make_connection()

    try:
        with read_conn.cursor(name="aws_bronze_cursor") as cur:
            cur.itersize = CHUNK_SIZE
            cur.execute("""
                SELECT
                    customer_id,
                    product_id,
                    product_title,
                    star_rating,
                    review_headline,
                    review_body,
                    review_date,
                    verified_purchase,
                    helpful_votes,
                    total_votes,
                    vine,
                    review_id
                FROM bronze.aws_reviews
            """)

            for row in cur:
                rows_read += 1
                (raw_customer_id, raw_product_id, raw_product_title,
                 raw_rating, raw_headline, raw_body,
                 raw_date, raw_verified, raw_helpful,
                 raw_total, raw_vine, raw_review_id) = row

                reviewer_id = clean_text(raw_customer_id)
                asin        = clean_text(raw_product_id)
                rating, reason = validate_row(reviewer_id, asin, raw_rating)

                if reason:
                    breakdown[reason] += 1
                    rejected.append((
                        SOURCE_AWS, reviewer_id, asin,
                        str(raw_rating) if raw_rating else None,
                        str(raw_body)[:MAX_REJECTION_TEXT] if raw_body else None,
                        reason, run_id,
                    ))
                else:
                    review_date = parse_date_string(raw_date)
                    dedup_key   = (reviewer_id, asin, review_date)

                    if dedup_key in dedup_cache:
                        breakdown[DUPLICATE] += 1
                        rejected.append((
                            SOURCE_AWS, reviewer_id, asin,
                            str(raw_rating), None, DUPLICATE, run_id,
                        ))
                    else:
                        review_text   = clean_text(raw_body)
                        review_length = len(review_text) if review_text else 0

                        if review_text is not None and review_length < MIN_REVIEW_LENGTH:
                            breakdown[TOO_SHORT] += 1
                            rejected.append((
                                SOURCE_AWS, reviewer_id, asin,
                                str(raw_rating), review_text, TOO_SHORT, run_id,
                            ))
                        else:
                            dedup_cache.add(dedup_key)
                            accepted.append((
                                clean_text(raw_review_id),
                                reviewer_id,
                                asin,
                                clean_text(raw_product_title),
                                rating,
                                clean_text(raw_headline),
                                review_text,
                                review_date,
                                review_length,
                                parse_bool_yn(raw_verified),
                                parse_int(raw_helpful),
                                parse_int(raw_total),
                                parse_bool_yn(raw_vine),
                                SOURCE_AWS,
                                clean_text(raw_review_id),
                            ))

                if len(accepted) >= CHUNK_SIZE:
                    flush_accepted(write_conn, accepted)
                    total_accepted += len(accepted)
                    accepted = []

                if len(rejected) >= CHUNK_SIZE:
                    flush_rejected(write_conn, rejected)
                    total_rejected += len(rejected)
                    rejected = []

                if rows_read % AWS_PROGRESS_INTERVAL == 0:
                    logger.info(
                        f"  AWS: {rows_read:,} read | "
                        f"{total_accepted + len(accepted):,} accepted | "
                        f"{total_rejected + len(rejected):,} rejected"
                    )

        # flush remainder
        if accepted:
            flush_accepted(write_conn, accepted)
            total_accepted += len(accepted)
        if rejected:
            flush_rejected(write_conn, rejected)
            total_rejected += len(rejected)

        write_pipeline_log(
            write_conn, run_id, SOURCE_AWS, rows_read,
            total_accepted, total_rejected, breakdown,
            "success", None, started_at,
        )
        logger.info(f"  AWS done: {rows_read:,} read | {total_accepted:,} accepted | {total_rejected:,} rejected")
        logger.info(f"  Breakdown: {breakdown}")

    except Exception as e:
        write_pipeline_log(
            write_conn, run_id, SOURCE_AWS, rows_read,
            total_accepted, total_rejected, breakdown,
            "failed", str(e), started_at,
        )
        raise
    finally:
        read_conn.close()
        write_conn.close()


# ── Process UCSD ──────────────────
def process_ucsd(run_id, dedup_cache):
    """
    Same two-connection pattern as process_aws.
    UCSD-specific field differences noted in comments.
    """
    logger.info("=== Processing UCSD Bronze -> Silver ===")
    started_at     = datetime.now()
    rows_read      = 0
    accepted       = []
    rejected       = []
    total_accepted = 0
    total_rejected = 0
    breakdown      = {k: 0 for k in [NULL_RATING, INVALID_RATING,
                                      NULL_REVIEWER, NULL_ASIN,
                                      DUPLICATE, TOO_SHORT]}

    read_conn  = make_connection()
    write_conn = make_connection()

    try:
        with read_conn.cursor(name="ucsd_bronze_cursor") as cur:
            cur.itersize = CHUNK_SIZE
            cur.execute("""
                SELECT
                    user_id,
                    asin,
                    rating,
                    title,
                    text,
                    timestamp,
                    verified_purchase,
                    images
                FROM bronze.ucsd_reviews
            """)

            for row in cur:
                rows_read += 1
                (raw_user_id, raw_asin, raw_rating,
                 raw_title, raw_text, raw_timestamp,
                 raw_verified, raw_images) = row

                reviewer_id = clean_text(raw_user_id)
                asin        = clean_text(raw_asin)
                rating, reason = validate_row(reviewer_id, asin, raw_rating)

                if reason:
                    breakdown[reason] += 1
                    rejected.append((
                        SOURCE_UCSD, reviewer_id, asin,
                        str(raw_rating) if raw_rating else None,
                        str(raw_text)[:MAX_REJECTION_TEXT] if raw_text else None,
                        reason, run_id,
                    ))
                else:
                    review_date = parse_unix_to_date(raw_timestamp)
                    dedup_key   = (reviewer_id, asin, review_date)

                    if dedup_key in dedup_cache:
                        breakdown[DUPLICATE] += 1
                        rejected.append((
                            SOURCE_UCSD, reviewer_id, asin,
                            str(raw_rating), None, DUPLICATE, run_id,
                        ))
                    else:
                        review_text   = clean_text(raw_text)
                        review_length = len(review_text) if review_text else 0

                        if review_text is not None and review_length < MIN_REVIEW_LENGTH:
                            breakdown[TOO_SHORT] += 1
                            rejected.append((
                                SOURCE_UCSD, reviewer_id, asin,
                                str(raw_rating), review_text, TOO_SHORT, run_id,
                            ))
                        else:
                            dedup_cache.add(dedup_key)

                            helpful_votes, total_votes = parse_helpful_list(raw_images)
                            accepted.append((
                                None,                           
                                reviewer_id,                   
                                asin,                          
                                None,                          
                                rating,                        
                                clean_text(raw_title),         
                                review_text,                    
                                review_date,                    
                                review_length,
                                parse_bool_truefalse(raw_verified),  
                                helpful_votes,                  
                                total_votes,                    
                                None,                           
                                SOURCE_UCSD,
                                f"{reviewer_id}|{asin}|{raw_timestamp}",
                            ))

                if len(accepted) >= CHUNK_SIZE:
                    flush_accepted(write_conn, accepted)
                    total_accepted += len(accepted)
                    accepted = []

                if len(rejected) >= CHUNK_SIZE:
                    flush_rejected(write_conn, rejected)
                    total_rejected += len(rejected)
                    rejected = []

                if rows_read % UCSD_PROGRESS_INTERVAL == 0:
                    logger.info(
                        f"  UCSD: {rows_read:,} read | "
                        f"{total_accepted + len(accepted):,} accepted | "
                        f"{total_rejected + len(rejected):,} rejected"
                    )

        if accepted:
            flush_accepted(write_conn, accepted)
            total_accepted += len(accepted)
        if rejected:
            flush_rejected(write_conn, rejected)
            total_rejected += len(rejected)

        write_pipeline_log(
            write_conn, run_id, SOURCE_UCSD, rows_read,
            total_accepted, total_rejected, breakdown,
            "success", None, started_at,
        )
        logger.info(f"  UCSD done: {rows_read:,} read | {total_accepted:,} accepted | {total_rejected:,} rejected")
        logger.info(f"  Breakdown: {breakdown}")

    except Exception as e:
        write_pipeline_log(
            write_conn, run_id, SOURCE_UCSD, rows_read,
            total_accepted, total_rejected, breakdown,
            "failed", str(e), started_at,
        )
        raise
    finally:
        read_conn.close()
        write_conn.close()


# ── Post-load audit ──────────────
def audit_silver():
    conn = make_connection()
    logger.info("\nSILVER LAYER AUDIT")
    queries = {
        "Total rows in silver.reviews":
            "SELECT COUNT(*) FROM silver.reviews",
        "Rows by source":
            "SELECT source, COUNT(*) FROM silver.reviews GROUP BY source ORDER BY source",
        "Rating distribution":
            "SELECT rating, COUNT(*) AS cnt FROM silver.reviews GROUP BY rating ORDER BY rating",
        "Verified purchase split":
            "SELECT verified_purchase, COUNT(*) FROM silver.reviews GROUP BY verified_purchase",
        "Total rejected rows":
            "SELECT COUNT(*) FROM silver.rejected_reviews",
        "Rejection breakdown":
            "SELECT rejection_reason, COUNT(*) AS cnt FROM silver.rejected_reviews GROUP BY rejection_reason ORDER BY cnt DESC",
        "Review date range":
            "SELECT MIN(review_date), MAX(review_date) FROM silver.reviews",
        "Null rates":
            """SELECT
                ROUND(100.0 * SUM(CASE WHEN review_text IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS text_null_pct,
                ROUND(100.0 * SUM(CASE WHEN review_date IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS date_null_pct
               FROM silver.reviews""",
        "Pipeline log":
            "SELECT source, rows_read, rows_accepted, rows_rejected, status, finished_at FROM silver.pipeline_log ORDER BY finished_at DESC",
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
    conn.close()


# ── Entry point ───────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver layer pipeline")
    parser.add_argument(
        "--source",
        choices=[SOURCE_AWS, SOURCE_UCSD, "all"],
        default="all",
        help="Which Bronze source to process"
    )
    args = parser.parse_args()

    run_id = str(uuid.uuid4())
    logger.info(f"Silver pipeline run ID: {run_id}")


    cache_conn   = make_connection()
    dedup_cache  = build_dedup_cache(cache_conn)
    cache_conn.close()

    if args.source in (SOURCE_AWS, "all"):
        process_aws(run_id, dedup_cache)

    if args.source in (SOURCE_UCSD, "all"):
        process_ucsd(run_id, dedup_cache)

    audit_silver()
    logger.info("Silver pipeline complete.")
