import json
import sys
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from src.config import DB_CONFIG
from src.utils.logger import get_logger
from src.config import LOG_DIR

logger = get_logger("load_categories", LOG_DIR)

BATCH_SIZE = 50_000

def main():
    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )

    # Step 1: Load JSON
    json_path = "data/raw/ucsd/asin2category.json"
    logger.info(f"Loading {json_path}...")
    with open(json_path, "r", encoding="utf-8") as f:
        asin_map = json.load(f)
    logger.info(f"Loaded {len(asin_map):,} ASIN-category mappings")

    # Step 2: Rebuild category lookup table
    logger.info("Rebuilding gold.product_category_lookup...")
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS gold.product_category_lookup")
        cur.execute("""
            CREATE TABLE gold.product_category_lookup (
                asin TEXT PRIMARY KEY,
                category TEXT NOT NULL
            )
        """)
    conn.commit()

    # Step 3: Batch insert
    batch = []
    inserted = 0
    for asin, category in asin_map.items():
        batch.append((asin, category))
        if len(batch) >= BATCH_SIZE:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO gold.product_category_lookup (asin, category) VALUES %s ON CONFLICT DO NOTHING",
                    batch
                )
            conn.commit()
            inserted += len(batch)
            batch.clear()
            if inserted % 500_000 == 0:
                logger.info(f"  {inserted:,} inserted...")

    # Flush remaining
    if batch:
        with conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO gold.product_category_lookup (asin, category) VALUES %s ON CONFLICT DO NOTHING",
                batch
            )
        conn.commit()
        inserted += len(batch)

    logger.info(f"Inserted {inserted:,} rows into gold.product_category_lookup")

    # Step 4: Create index
    with conn.cursor() as cur:
        cur.execute("CREATE INDEX idx_cat_lookup_asin ON gold.product_category_lookup(asin)")
        cur.execute("CREATE INDEX idx_cat_lookup_cat ON gold.product_category_lookup(category)")
    conn.commit()

    # Step 5: Verify
    with conn.cursor() as cur:
        cur.execute("SELECT category, COUNT(*) FROM gold.product_category_lookup GROUP BY category ORDER BY COUNT(*) DESC LIMIT 15")
        rows = cur.fetchall()
    
    logger.info("Top categories:")
    for cat, cnt in rows:
        logger.info(f"  {cat}: {cnt:,}")

    conn.close()
    logger.info("Done. Now re-run 04_gold_aggregations.sql in pgAdmin (skip the category lookup section).")

if __name__ == "__main__":
    main()