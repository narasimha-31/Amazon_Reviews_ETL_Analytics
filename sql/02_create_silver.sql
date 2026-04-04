-- ============================================================
-- Amazon Review Intelligence - Silver Layer Schema
-- Run this ONCE in pgAdmin before running load_silver.py
-- ============================================================

-- silver.reviews: unified, cleaned, type-enforced table
-- One row = one valid review from either UCSD or Kaggle source
-- All column names, types, and formats are normalised here

DROP TABLE IF EXISTS silver.reviews;
CREATE TABLE silver.reviews (

    -- identifiers
    review_id           TEXT,
    reviewer_id         TEXT        NOT NULL,   -- UCSD: reviewerID | Kaggle: customer_id
    asin                TEXT        NOT NULL,   -- UCSD: asin | Kaggle: product_id
    product_title       TEXT,                   -- Kaggle only, NULL for UCSD rows

    -- review content
    rating              NUMERIC(2,1) NOT NULL,  -- UCSD: overall(float) | Kaggle: star_rating(str)
    review_title        TEXT,                   -- UCSD: summary | Kaggle: review_headline
    review_text         TEXT,                   -- UCSD: reviewText | Kaggle: review_body
    review_date         DATE,                   -- UCSD: unixReviewTime->DATE | Kaggle: review_date
    review_length       INTEGER,                -- computed: char count of review_text

    -- trust signals
    verified_purchase   BOOLEAN,                -- UCSD: NULL (not in source) | Kaggle: Y/N->bool
    helpful_votes       INTEGER,                -- UCSD: NULL (not in source) | Kaggle: helpful_votes
    total_votes         INTEGER,                -- UCSD: NULL (not in source) | Kaggle: total_votes
    vine_reviewer       BOOLEAN,                -- UCSD: NULL (not in source) | Kaggle: vine Y/N->bool

    -- pipeline tracking
    source              TEXT        NOT NULL,   -- "ucsd" or "aws"
    raw_source_id       TEXT,
    loaded_at           TIMESTAMPTZ DEFAULT NOW()
);

-- silver.rejected_reviews: dead-letter queue
-- Every row that FAILED validation lands here with a reason tag
-- Never drop bad data silently - always explain why it was rejected

DROP TABLE IF EXISTS silver.rejected_reviews;
CREATE TABLE silver.rejected_reviews (
    rejection_id        SERIAL PRIMARY KEY,
    source              TEXT,
    raw_reviewer_id     TEXT,
    raw_asin            TEXT,
    raw_rating          TEXT,
    raw_review_text     TEXT,
    rejection_reason    TEXT NOT NULL,
    -- Reason codes:
    -- NULL_RATING       rating field is NULL or empty
    -- INVALID_RATING    rating not castable or outside 1.0-5.0
    -- NULL_REVIEWER_ID  reviewer_id is NULL
    -- NULL_ASIN         asin is NULL
    -- DUPLICATE         same reviewer_id + asin + review_date already exists
    -- REVIEW_TOO_SHORT  review_text under 3 characters
    run_id              TEXT,
    rejected_at         TIMESTAMPTZ DEFAULT NOW()
);

-- silver.pipeline_log: Silver phase run history
DROP TABLE IF EXISTS silver.pipeline_log;
CREATE TABLE silver.pipeline_log (
    log_id              SERIAL PRIMARY KEY,
    run_id              TEXT NOT NULL,
    source              TEXT NOT NULL,
    rows_read           INTEGER,
    rows_accepted       INTEGER,
    rows_rejected       INTEGER,
    rejection_breakdown JSONB,
    status              TEXT,
    error_message       TEXT,
    started_at          TIMESTAMPTZ,
    finished_at         TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_silver_asin       ON silver.reviews(asin);
CREATE INDEX IF NOT EXISTS idx_silver_reviewer   ON silver.reviews(reviewer_id);
CREATE INDEX IF NOT EXISTS idx_silver_rating     ON silver.reviews(rating);
CREATE INDEX IF NOT EXISTS idx_silver_date       ON silver.reviews(review_date);
CREATE INDEX IF NOT EXISTS idx_silver_source     ON silver.reviews(source);
CREATE INDEX IF NOT EXISTS idx_silver_dedup      ON silver.reviews(reviewer_id, asin, review_date);
CREATE INDEX IF NOT EXISTS idx_rejected_reason   ON silver.rejected_reviews(rejection_reason);

-- Verify
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'silver'
ORDER BY table_name;
