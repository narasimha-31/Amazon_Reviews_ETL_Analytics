

DROP TABLE IF EXISTS gold.review_sentiment;
CREATE TABLE gold.review_sentiment (

    -- identity — matches silver.reviews exactly
    reviewer_id         TEXT        NOT NULL,
    asin                TEXT        NOT NULL,
    review_date         DATE,
    source              TEXT        NOT NULL,   -- "ucsd" or "aws"

    -- original rating from Silver — used for VADER vs rating comparison
    rating              NUMERIC(2,1),

    -- VADER raw scores (all 0.0 to 1.0 except compound)
    vader_compound      NUMERIC(5,4),   -- -1.0 to +1.0  (main score)
    vader_positive      NUMERIC(5,4),   --  0.0 to  1.0
    vader_negative      NUMERIC(5,4),   --  0.0 to  1.0
    vader_neutral       NUMERIC(5,4),   --  0.0 to  1.0

    -- Classification thresholds (industry standard for VADER):
    --   compound >= 0.05  → POSITIVE
    --   compound <= -0.05 → NEGATIVE
    --   between           → NEUTRAL
    sentiment_label     TEXT,           -- "POSITIVE", "NEGATIVE", "NEUTRAL"

    -- Does VADER agree with the star rating?
    -- Positive label + rating >= 4  → TRUE
    -- Negative label + rating <= 2  → TRUE
    -- Otherwise                     → FALSE  (disagreement = interesting case)
    rating_sentiment_agree  BOOLEAN,

    -- review metadata kept here for easy dashboard queries
    -- (avoids joining back to Silver for every dashboard view)
    review_length       INTEGER,
    verified_purchase   BOOLEAN,
    helpful_votes       INTEGER,

    -- pipeline audit
    scored_at           TIMESTAMPTZ DEFAULT NOW()
);

-- ── 2. gold.product_metrics ──────────────────────────────────
-- One row per ASIN — the product-level intelligence table.
-- This is what the Sentiment Overview and Conversion dashboards read.

DROP TABLE IF EXISTS gold.product_metrics;
CREATE TABLE gold.product_metrics (
    asin                    TEXT        PRIMARY KEY,
    product_title           TEXT,       -- from Kaggle rows; NULL for UCSD-only products

    -- review volume
    total_reviews           INTEGER,
    verified_reviews        INTEGER,
    unverified_reviews      INTEGER,

    -- rating stats
    avg_rating              NUMERIC(3,2),
    rating_stddev           NUMERIC(4,3),   -- high stddev = polarising product

    -- sentiment stats (from VADER scores)
    avg_compound_score      NUMERIC(5,4),
    pct_positive            NUMERIC(5,2),   -- percentage 0-100
    pct_negative            NUMERIC(5,2),
    pct_neutral             NUMERIC(5,2),

    -- the gap between what stars say and what text says
    -- positive gap = text more positive than stars suggest
    -- negative gap = text more negative than stars suggest
    sentiment_rating_gap    NUMERIC(5,4),

    -- review velocity
    first_review_date       DATE,
    latest_review_date      DATE,
    reviews_per_month       NUMERIC(8,2),

    -- fake review signals
    unverified_pct          NUMERIC(5,2),
    avg_review_length       NUMERIC(8,1),

    -- pipeline audit
    computed_at             TIMESTAMPTZ DEFAULT NOW()
);

-- ── 3. gold.category_benchmarks ──────────────────────────────
-- Sentiment benchmarks per source/category.
-- Used in Category Benchmarks dashboard.

DROP TABLE IF EXISTS gold.category_benchmarks;
CREATE TABLE gold.category_benchmarks (
    source                  TEXT,
    rating_band             TEXT,       -- "1-2 stars", "3 stars", "4-5 stars"
    total_reviews           INTEGER,
    avg_compound_score      NUMERIC(5,4),
    pct_positive            NUMERIC(5,2),
    pct_negative            NUMERIC(5,2),
    pct_neutral             NUMERIC(5,2),
    avg_review_length       NUMERIC(8,1),
    computed_at             TIMESTAMPTZ DEFAULT NOW()
);

-- ── 4. gold.fake_review_signals ──────────────────────────────
-- Reviewer-level suspicion scoring.
-- High score = reviewer shows patterns associated with fake reviews.

DROP TABLE IF EXISTS gold.fake_review_signals;
CREATE TABLE gold.fake_review_signals (
    reviewer_id             TEXT        PRIMARY KEY,
    source                  TEXT,
    total_reviews           INTEGER,

    -- Fake review signals (each contributes to suspicion_score):
    -- 1. High volume of reviews in short time
    -- 2. Overwhelmingly 5-star (never critical)
    -- 3. Short reviews (under 20 chars)
    -- 4. Unverified purchases only
    -- 5. Extreme sentiment variance (all positive or all negative)
    pct_five_star           NUMERIC(5,2),
    pct_unverified          NUMERIC(5,2),
    avg_review_length       NUMERIC(8,1),
    pct_short_reviews       NUMERIC(5,2),   -- reviews under 20 chars
    avg_compound_score      NUMERIC(5,4),
    sentiment_variance      NUMERIC(7,4),

    -- composite score 0-100, higher = more suspicious
    -- computed as weighted sum of above signals
    suspicion_score         NUMERIC(5,2),
    suspicion_label         TEXT,           -- "LOW", "MEDIUM", "HIGH"

    computed_at             TIMESTAMPTZ DEFAULT NOW()
);

-- ── 5. gold.sentiment_run_log ────────────────────────────────
-- Tracks every VADER scoring run — how many rows, how long, etc.
-- This feeds the Pipeline Observability dashboard.

DROP TABLE IF EXISTS gold.sentiment_run_log;
CREATE TABLE gold.sentiment_run_log (
    run_id          TEXT PRIMARY KEY,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    rows_scored     INTEGER,
    rows_skipped    INTEGER,    -- already scored in previous run (checkpoint)
    rows_failed     INTEGER,
    avg_score       NUMERIC(5,4),
    pct_positive    NUMERIC(5,2),
    pct_negative    NUMERIC(5,2),
    pct_neutral     NUMERIC(5,2),
    status          TEXT        -- "running", "complete", "failed"
);

-- ── Indexes for dashboard query patterns ─────────────────────
CREATE INDEX IF NOT EXISTS idx_sentiment_asin
    ON gold.review_sentiment(asin);

CREATE INDEX IF NOT EXISTS idx_sentiment_label
    ON gold.review_sentiment(sentiment_label);

CREATE INDEX IF NOT EXISTS idx_sentiment_source
    ON gold.review_sentiment(source);

CREATE INDEX IF NOT EXISTS idx_sentiment_date
    ON gold.review_sentiment(review_date);

CREATE INDEX IF NOT EXISTS idx_sentiment_compound
    ON gold.review_sentiment(vader_compound);

-- Composite index for dedup checking on resume
CREATE INDEX IF NOT EXISTS idx_sentiment_dedup
    ON gold.review_sentiment(reviewer_id, asin, review_date);

CREATE INDEX IF NOT EXISTS idx_fake_score
    ON gold.fake_review_signals(suspicion_score DESC);

-- ── Verify ───────────────────────────────────────────────────
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'gold'
ORDER BY table_name;
