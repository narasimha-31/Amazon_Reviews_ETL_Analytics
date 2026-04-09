"""
score_sentiment.py - Phase 3, Step 1
VADER sentiment scoring with keyset pagination checkpoint.

Architecture:
    read_conn  - named server-side cursor streams silver.reviews
    write_conn - inserts scored rows into gold.review_sentiment

Checkpoint system (keyset pagination):
    On startup: reads the LAST scored row's (source, review_date, reviewer_id)
    Uses WHERE (source, review_date, reviewer_id) > (last_src, last_date, last_rev)
    → O(log N) index seek — instant resume at ANY offset
    → No more OFFSET slowdown at 5M, 10M, 20M rows

Requires index:
    CREATE INDEX idx_silver_nlp_order
    ON silver.reviews(source, review_date, reviewer_id)
    WHERE review_text IS NOT NULL;

Usage:
    python src/gold/score_sentiment.py              -- scores all rows
    python src/gold/score_sentiment.py --limit 100000  -- test run
    python src/gold/score_sentiment.py --reset      -- clears gold and restarts
"""

import sys
import uuid
import argparse
from datetime import datetime
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.config import DB_CONFIG, LOG_DIR, CHUNK_SIZE
from src.utils.logger import get_logger

logger = get_logger("score_sentiment", LOG_DIR)


# ── Constants ─────────────────────────────────────────────────
POSITIVE_THRESHOLD  =  0.05
NEGATIVE_THRESHOLD  = -0.05
PROGRESS_INTERVAL   = 500_000
INSERT_BATCH_SIZE   = CHUNK_SIZE

INSERT_SENTIMENT = """
    INSERT INTO gold.review_sentiment (
        reviewer_id, asin, review_date, source,
        rating,
        vader_compound, vader_positive, vader_negative, vader_neutral,
        sentiment_label, rating_sentiment_agree,
        review_length, verified_purchase, helpful_votes
    ) VALUES %s
    ON CONFLICT DO NOTHING
"""

INSERT_RUN_LOG = """
    INSERT INTO gold.sentiment_run_log
        (run_id, started_at, rows_scored, rows_skipped,
         rows_failed, avg_score, pct_positive, pct_negative,
         pct_neutral, status)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (run_id) DO UPDATE SET
        finished_at  = NOW(),
        rows_scored  = EXCLUDED.rows_scored,
        rows_failed  = EXCLUDED.rows_failed,
        avg_score    = EXCLUDED.avg_score,
        pct_positive = EXCLUDED.pct_positive,
        pct_negative = EXCLUDED.pct_negative,
        pct_neutral  = EXCLUDED.pct_neutral,
        status       = EXCLUDED.status
"""


def make_conn():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


# ── Classification helpers ────────────────────────────────────
def classify(compound: float) -> str:
    if compound >= POSITIVE_THRESHOLD:
        return "POSITIVE"
    if compound <= NEGATIVE_THRESHOLD:
        return "NEGATIVE"
    return "NEUTRAL"


def rating_agrees(rating, label: str):
    if rating is None:
        return None
    r = float(rating)
    if r >= 4.0 and label == "POSITIVE":  return True
    if r <= 2.0 and label == "NEGATIVE":  return True
    if r == 3.0 and label == "NEUTRAL":   return True
    return False


# ── Checkpoint via keyset pagination ──────────────────────────
def get_checkpoint(conn):
    """
    Returns (count, last_source, last_review_date, last_reviewer_id).
    last_review_date is returned as a native date — NO text cast.
    If nothing scored yet, returns (0, None, None, None).
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM gold.review_sentiment")
        count = cur.fetchone()[0]

        if count == 0:
            conn.commit()
            return 0, None, None, None

        # Get the MAX row in sort order — this is where we resume AFTER
        cur.execute("""
            SELECT source, review_date, reviewer_id
            FROM gold.review_sentiment
            ORDER BY source DESC, review_date DESC, reviewer_id DESC
            LIMIT 1
        """)
        row = cur.fetchone()
    conn.commit()
    return count, row[0], row[1], row[2]


# ── Main scoring function ─────────────────────────────────────
def score_sentiment(limit=None):
    run_id     = str(uuid.uuid4())
    started_at = datetime.now()
    analyzer   = SentimentIntensityAnalyzer()

    rows_scored  = 0
    rows_failed  = 0
    batch        = []
    sum_compound = 0.0
    count_pos = count_neg = count_neu = 0

    read_conn  = make_conn()
    write_conn = make_conn()

    try:
        # ── Checkpoint ────────────────────────────────────────
        already_scored, last_src, last_date, last_rev = get_checkpoint(write_conn)
        logger.info(f"Checkpoint: {already_scored:,} rows already scored")

        if already_scored > 0:
            logger.info(
                f"Resuming AFTER: source={last_src}  "
                f"date={last_date}  reviewer={last_rev}"
            )
        else:
            logger.info("Starting from row 1")

        if limit:
            logger.info(f"Test mode: scoring {limit:,} rows only")

        # Log run start
        with write_conn.cursor() as cur:
            cur.execute(INSERT_RUN_LOG, (
                run_id, started_at, 0, already_scored,
                0, None, None, None, None, "running"
            ))
        write_conn.commit()

        # ── Build query ───────────────────────────────────────
        lim_clause = f"LIMIT {limit}" if limit else ""

        if already_scored > 0:
            # KEYSET PAGINATION — index seek, O(log N), instant at any offset
            query = f"""
                SELECT reviewer_id, asin, review_date, source, rating,
                       review_text, review_length, verified_purchase, helpful_votes
                FROM silver.reviews
                WHERE review_text IS NOT NULL
                  AND (source, review_date, reviewer_id) > (%s, %s, %s)
                ORDER BY source, review_date, reviewer_id
                {lim_clause}
            """
            query_params = (last_src, last_date, last_rev)
        else:
            # Fresh start — no WHERE filter needed
            query = f"""
                SELECT reviewer_id, asin, review_date, source, rating,
                       review_text, review_length, verified_purchase, helpful_votes
                FROM silver.reviews
                WHERE review_text IS NOT NULL
                ORDER BY source, review_date, reviewer_id
                {lim_clause}
            """
            query_params = None

        logger.info("Opening cursor...")

        with read_conn.cursor(name="gold_vader_cursor") as cur:
            cur.itersize = INSERT_BATCH_SIZE
            if query_params:
                cur.execute(query, query_params)
            else:
                cur.execute(query)
            logger.info("Cursor open — rows flowing in...")

            t_start = datetime.now()

            for row in cur:
                (reviewer_id, asin, review_date, source,
                 rating, review_text, review_length,
                 verified_purchase, helpful_votes) = row

                try:
                    scores   = analyzer.polarity_scores(review_text)
                    compound = scores["compound"]
                    label    = classify(compound)
                    agrees   = rating_agrees(rating, label)

                    batch.append((
                        reviewer_id, asin, review_date, source, rating,
                        round(compound,       4),
                        round(scores["pos"],  4),
                        round(scores["neg"],  4),
                        round(scores["neu"],  4),
                        label, agrees,
                        review_length, verified_purchase, helpful_votes,
                    ))

                    rows_scored  += 1
                    sum_compound += compound
                    if   label == "POSITIVE": count_pos += 1
                    elif label == "NEGATIVE": count_neg += 1
                    else:                     count_neu += 1

                except Exception as e:
                    rows_failed += 1
                    if rows_failed <= 5:
                        logger.warning(f"Row failed: {e}")

                # ── Flush batch ───────────────────────────────
                if len(batch) >= INSERT_BATCH_SIZE:
                    with write_conn.cursor() as wcur:
                        execute_values(wcur, INSERT_SENTIMENT, batch, page_size=1000)
                    write_conn.commit()
                    batch.clear()

                # ── Progress log ──────────────────────────────
                total_so_far = already_scored + rows_scored
                if rows_scored > 0 and rows_scored % PROGRESS_INTERVAL == 0:
                    elapsed = (datetime.now() - t_start).total_seconds()
                    rate    = rows_scored / max(elapsed, 1)
                    remaining = (44_200_000 - total_so_far) / max(rate, 1)
                    eta_h   = remaining / 3600

                    pct_p = round(count_pos / rows_scored * 100, 1)
                    pct_n = round(count_neg / rows_scored * 100, 1)

                    logger.info(
                        f"  {total_so_far:,} total | "
                        f"{rate:.0f} rows/sec | "
                        f"ETA ~{eta_h:.1f}h | "
                        f"pos={pct_p}% neg={pct_n}%"
                    )

            # ── Flush remaining ───────────────────────────────
            if batch:
                with write_conn.cursor() as wcur:
                    execute_values(wcur, INSERT_SENTIMENT, batch, page_size=1000)
                write_conn.commit()

        # ── Final summary ─────────────────────────────────────
        total   = max(rows_scored, 1)
        avg_c   = round(sum_compound / total, 4)
        pct_pos = round(count_pos / total * 100, 2)
        pct_neg = round(count_neg / total * 100, 2)
        pct_neu = round(count_neu / total * 100, 2)

        with write_conn.cursor() as cur:
            cur.execute(INSERT_RUN_LOG, (
                run_id, started_at, rows_scored, already_scored,
                rows_failed, avg_c, pct_pos, pct_neg, pct_neu, "complete"
            ))
        write_conn.commit()

        logger.info("=" * 55)
        logger.info("VADER SCORING COMPLETE")
        logger.info("=" * 55)
        logger.info(f"  This run scored:  {rows_scored:,} rows")
        logger.info(f"  Skipped (cached): {already_scored:,} rows")
        logger.info(f"  Failed:           {rows_failed:,} rows")
        logger.info(f"  Avg compound:     {avg_c:+.4f}")
        logger.info(f"  POSITIVE:         {pct_pos:.1f}%")
        logger.info(f"  NEGATIVE:         {pct_neg:.1f}%")
        logger.info(f"  NEUTRAL:          {pct_neu:.1f}%")
        logger.info("=" * 55)

    except Exception as e:
        logger.error(f"Scoring failed: {e}")
        with write_conn.cursor() as cur:
            cur.execute(INSERT_RUN_LOG, (
                run_id, started_at, rows_scored, already_scored,
                rows_failed, None, None, None, None, "failed"
            ))
        write_conn.commit()
        raise

    finally:
        read_conn.close()
        write_conn.close()
        logger.info("Connections closed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VADER sentiment scoring")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    if args.reset:
        conn = make_conn()
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE gold.review_sentiment")
            cur.execute("TRUNCATE TABLE gold.sentiment_run_log")
        conn.commit()
        conn.close()
        logger.info("Gold sentiment tables cleared — starting fresh.")

    score_sentiment(limit=args.limit)