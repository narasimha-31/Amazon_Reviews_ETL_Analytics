"""
audit_bronze.py — Phase 1, Step 3
Runs a comprehensive data quality audit on the Bronze layer
and writes a markdown report to logs/bronze_audit_YYYYMMDD.md

This is the "data quality issues" document that the LinkedIn post
and the project PDF both say you MUST produce. Screenshot this.
Put it in your README. Talk about it in interviews.

Usage:
    python src/ingestion/audit_bronze.py
"""

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.config import DB_CONFIG, LOG_DIR
from src.utils.logger import get_logger
import psycopg2

logger = get_logger("audit_bronze", LOG_DIR)


# ─────────────────────────────────────────────────────────────
AUDIT_QUERIES = {
    # ── UCSD dataset checks ───────────────────────────────────
    "ucsd_total_rows": {
        "sql": "SELECT COUNT(*) AS total FROM bronze.ucsd_reviews",
        "label": "UCSD: Total rows loaded",
    },
    "ucsd_rows_by_category": {
        "sql": """
            SELECT source_category, COUNT(*) AS rows
            FROM bronze.ucsd_reviews
            GROUP BY source_category
            ORDER BY rows DESC
        """,
        "label": "UCSD: Row count per category",
    },
    "ucsd_null_rates": {
        "sql": """
            SELECT
                ROUND(100.0 * SUM(CASE WHEN rating IS NULL THEN 1 ELSE 0 END)            / COUNT(*), 2) AS rating_null_pct,
                ROUND(100.0 * SUM(CASE WHEN text IS NULL OR text = '' THEN 1 ELSE 0 END) / COUNT(*), 2) AS text_null_pct,
                ROUND(100.0 * SUM(CASE WHEN asin IS NULL THEN 1 ELSE 0 END)              / COUNT(*), 2) AS asin_null_pct,
                ROUND(100.0 * SUM(CASE WHEN user_id IS NULL THEN 1 ELSE 0 END)           / COUNT(*), 2) AS user_id_null_pct,
                ROUND(100.0 * SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END)         / COUNT(*), 2) AS timestamp_null_pct
            FROM bronze.ucsd_reviews
        """,
        "label": "UCSD: Null rates per critical field (%)",
    },
    "ucsd_rating_distribution": {
        "sql": """
            SELECT rating, COUNT(*) AS cnt,
                   ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct
            FROM bronze.ucsd_reviews
            GROUP BY rating
            ORDER BY cnt DESC
            LIMIT 15
        """,
        "label": "UCSD: Rating value distribution (top 15 — reveals weird values)",
    },
    "ucsd_duplicate_reviews": {
        "sql": """
            SELECT COUNT(*) AS total_rows,
                   COUNT(DISTINCT user_id || '|' || asin || '|' || timestamp) AS unique_combos,
                   COUNT(*) - COUNT(DISTINCT user_id || '|' || asin || '|' || timestamp) AS duplicates
            FROM bronze.ucsd_reviews
        """,
        "label": "UCSD: Duplicate check (user_id + asin + timestamp)",
    },
    "ucsd_review_length_stats": {
        "sql": """
            SELECT
                MIN(LENGTH(text))   AS min_chars,
                MAX(LENGTH(text))   AS max_chars,
                ROUND(AVG(LENGTH(text))) AS avg_chars,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY LENGTH(text)) AS median_chars,
                SUM(CASE WHEN LENGTH(text) < 10 THEN 1 ELSE 0 END) AS suspiciously_short
            FROM bronze.ucsd_reviews
            WHERE text IS NOT NULL
        """,
        "label": "UCSD: Review text length stats (short reviews = fake review signal)",
    },
    "ucsd_verified_purchase_dist": {
        "sql": """
            SELECT verified_purchase, COUNT(*) AS cnt
            FROM bronze.ucsd_reviews
            GROUP BY verified_purchase
            ORDER BY cnt DESC
        """,
        "label": "UCSD: verified_purchase value distribution (note inconsistent formats)",
    },

    # ── AWS dataset checks ────────────────────────────────────
    "aws_total_rows": {
        "sql": "SELECT COUNT(*) AS total FROM bronze.aws_reviews",
        "label": "AWS: Total rows loaded",
    },
    "aws_null_rates": {
        "sql": """
            SELECT
                ROUND(100.0 * SUM(CASE WHEN star_rating IS NULL THEN 1 ELSE 0 END)  / COUNT(*), 2) AS rating_null_pct,
                ROUND(100.0 * SUM(CASE WHEN review_body IS NULL THEN 1 ELSE 0 END)  / COUNT(*), 2) AS text_null_pct,
                ROUND(100.0 * SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)   / COUNT(*), 2) AS asin_null_pct,
                ROUND(100.0 * SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)  / COUNT(*), 2) AS user_id_null_pct
            FROM bronze.aws_reviews
        """,
        "label": "AWS: Null rates per critical field (%)",
    },
    "aws_star_rating_dist": {
        "sql": """
            SELECT star_rating, COUNT(*) AS cnt
            FROM bronze.aws_reviews
            GROUP BY star_rating
            ORDER BY cnt DESC
        """,
        "label": "AWS: Star rating distribution (note: stored as text, check for '1'-'5' only)",
    },
    "aws_verified_purchase_dist": {
        "sql": """
            SELECT verified_purchase, COUNT(*) AS cnt
            FROM bronze.aws_reviews
            GROUP BY verified_purchase
            ORDER BY cnt DESC
        """,
        "label": "AWS: verified_purchase dist — AWS uses Y/N vs UCSD uses True/False",
    },

    # ── Cross-source schema differences ──────────────────────
    "schema_conflict_verified": {
        "sql": """
            SELECT 'UCSD' AS source, verified_purchase, COUNT(*) AS cnt
            FROM bronze.ucsd_reviews GROUP BY verified_purchase
            UNION ALL
            SELECT 'AWS'  AS source, verified_purchase, COUNT(*) AS cnt
            FROM bronze.aws_reviews  GROUP BY verified_purchase
            ORDER BY source, cnt DESC
        """,
        "label": "SCHEMA CONFLICT: verified_purchase — UCSD (True/False) vs AWS (Y/N) — Silver must normalise this",
    },
    "schema_conflict_rating_format": {
        "sql": """
            SELECT 'UCSD' AS source, rating AS rating_value, COUNT(*) AS cnt
            FROM bronze.ucsd_reviews GROUP BY rating
            UNION ALL
            SELECT 'AWS'  AS source, star_rating AS rating_value, COUNT(*) AS cnt
            FROM bronze.aws_reviews  GROUP BY star_rating
            ORDER BY source, cnt DESC
            LIMIT 20
        """,
        "label": "SCHEMA CONFLICT: rating format — UCSD (float '4.0') vs AWS (int '4') — Silver must normalise",
    },
}


# ─────────────────────────────────────────────────────────────
def run_audit():
    conn = psycopg2.connect(**{k: v for k, v in {
        "host": DB_CONFIG["host"], "port": DB_CONFIG["port"],
        "dbname": DB_CONFIG["dbname"], "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
    }.items()})

    today    = datetime.now().strftime("%Y%m%d_%H%M")
    md_path  = LOG_DIR / f"bronze_audit_{today}.md"
    findings = []

    logger.info("Running Bronze layer audit...")

    with conn.cursor() as cur:
        for key, spec in AUDIT_QUERIES.items():
            label = spec["label"]
            sql   = spec["sql"]

            logger.info(f"  {label}")
            try:
                cur.execute(sql)
                rows = cur.fetchall()
                cols = [desc[0] for desc in cur.description]
                findings.append({"label": label, "cols": cols, "rows": rows})
            except Exception as e:
                logger.error(f"  Query failed ({key}): {e}")
                conn.rollback()
                findings.append({"label": label, "cols": [], "rows": [], "error": str(e)})

    conn.close()
    _write_markdown_report(md_path, findings)
    logger.info(f"\nAudit report written → {md_path}")
    return md_path


def _write_markdown_report(path: Path, findings: list):
    lines = [
        "# Bronze Layer — Data Quality Audit",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "This report documents every data quality issue found in the raw Bronze layer.",
        "These issues are what the Silver layer pipeline is designed to handle.",
        "Issues flagged here become test cases in the Great Expectations suite (Phase 5).",
        "",
    ]

    for f in findings:
        lines.append(f"## {f['label']}")
        if "error" in f:
            lines.append(f"> ⚠ Query failed: {f['error']}")
        elif f["rows"]:
            # Markdown table
            lines.append("| " + " | ".join(f["cols"]) + " |")
            lines.append("| " + " | ".join(["---"] * len(f["cols"])) + " |")
            for row in f["rows"]:
                lines.append("| " + " | ".join(str(v) for v in row) + " |")
        else:
            lines.append("_No rows returned._")
        lines.append("")

    lines += [
        "---",
        "## Key Issues to Fix in Silver Layer",
        "",
        "Based on the above audit, the Silver pipeline must handle:",
        "",
        "1. **Null ratings** — rows with NULL rating → dead-letter queue",
        "2. **verified_purchase format mismatch** — UCSD uses `True/False`, AWS uses `Y/N` → normalise to boolean",
        "3. **Rating format mismatch** — UCSD stores `'4.0'` (float string), AWS stores `'4'` (int string) → cast to NUMERIC(2,1)",
        "4. **Suspiciously short reviews** — text under 10 chars → flag as low-quality, do not drop",
        "5. **Duplicate reviews** — same user_id + asin + timestamp → deduplicate in Silver",
        "6. **NULL review text** — keep the row but flag; sentiment analysis will skip NULLs",
        "",
        "_These issues are documented here so the Silver pipeline decisions are explainable, not arbitrary._",
    ]

    path.write_text("\n".join(lines), encoding="utf-8")


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_audit()
