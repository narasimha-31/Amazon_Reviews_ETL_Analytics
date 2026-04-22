-- ============================================================
-- Amazon Review Intelligence — Gold Aggregation Tables
-- Run AFTER score_sentiment.py completes (44.2M rows scored)
--
-- This script:
--   1. Creates a category lookup from Bronze
--   2. Populates product_metrics (one row per ASIN)
--   3. Populates category_benchmarks (per category + rating band)
--   4. Populates fake_review_signals (per reviewer, 3+ reviews)
--   5. Creates sentiment_trends (monthly time-series)
--
-- Safe to re-run. Does NOT touch review_sentiment.
-- ============================================================


-- ═══════════════════════════════════════════════════════════
-- 0. CATEGORY LOOKUP
-- ═══════════════════════════════════════════════════════════

DROP TABLE IF EXISTS gold.product_category_lookup;

CREATE TABLE gold.product_category_lookup AS
WITH combined AS (
    SELECT DISTINCT asin, source_category AS category
    FROM bronze.ucsd_reviews
    WHERE source_category IS NOT NULL AND asin IS NOT NULL
    UNION
    SELECT DISTINCT product_id, product_category
    FROM bronze.aws_reviews
    WHERE product_category IS NOT NULL AND product_id IS NOT NULL
),
deduped AS (
    SELECT DISTINCT ON (asin) asin, category
    FROM combined
    ORDER BY asin, category
)
SELECT * FROM deduped;

CREATE INDEX idx_cat_lookup_asin ON gold.product_category_lookup(asin);

SELECT 'category_lookup' AS step, COUNT(*) AS asins,
       COUNT(DISTINCT category) AS categories
FROM gold.product_category_lookup;


-- ═══════════════════════════════════════════════════════════
-- 1. PRODUCT METRICS
-- ═══════════════════════════════════════════════════════════

DROP TABLE IF EXISTS gold.product_metrics;
CREATE TABLE gold.product_metrics (
    asin                    TEXT PRIMARY KEY,
    category                TEXT,
    total_reviews           INTEGER,
    verified_reviews        INTEGER,
    unverified_reviews      INTEGER,
    avg_rating              NUMERIC(3,2),
    rating_stddev           NUMERIC(4,3),
    avg_compound_score      NUMERIC(5,4),
    pct_positive            NUMERIC(5,2),
    pct_negative            NUMERIC(5,2),
    pct_neutral             NUMERIC(5,2),
    sentiment_rating_gap    NUMERIC(5,4),
    pct_disagreement        NUMERIC(5,2),
    first_review_date       DATE,
    latest_review_date      DATE,
    reviews_per_month       NUMERIC(8,2),
    unverified_pct          NUMERIC(5,2),
    avg_review_length       NUMERIC(8,1),
    total_helpful_votes     INTEGER,
    computed_at             TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO gold.product_metrics
SELECT
    rs.asin,
    cl.category,
    COUNT(*),
    COUNT(*) FILTER (WHERE rs.verified_purchase = TRUE),
    COUNT(*) FILTER (WHERE rs.verified_purchase = FALSE),
    ROUND(AVG(rs.rating), 2),
    ROUND(COALESCE(STDDEV(rs.rating), 0), 3),
    ROUND(AVG(rs.vader_compound), 4),
    ROUND(COUNT(*) FILTER (WHERE rs.sentiment_label = 'POSITIVE') * 100.0 / COUNT(*), 2),
    ROUND(COUNT(*) FILTER (WHERE rs.sentiment_label = 'NEGATIVE') * 100.0 / COUNT(*), 2),
    ROUND(COUNT(*) FILTER (WHERE rs.sentiment_label = 'NEUTRAL')  * 100.0 / COUNT(*), 2),
    ROUND(AVG(rs.vader_compound) - (AVG(rs.rating) - 3.0) / 2.0, 4),
    ROUND(COUNT(*) FILTER (WHERE rs.rating_sentiment_agree = FALSE) * 100.0
          / NULLIF(COUNT(*) FILTER (WHERE rs.rating_sentiment_agree IS NOT NULL), 0), 2),
    MIN(rs.review_date),
    MAX(rs.review_date),
    CASE WHEN MIN(rs.review_date) = MAX(rs.review_date) THEN COUNT(*)::NUMERIC
         ELSE ROUND(COUNT(*) / (GREATEST((MAX(rs.review_date) - MIN(rs.review_date)), 1) / 30.0), 2)
    END,
    ROUND(COUNT(*) FILTER (WHERE rs.verified_purchase = FALSE) * 100.0
          / NULLIF(COUNT(*) FILTER (WHERE rs.verified_purchase IS NOT NULL), 0), 2),
    ROUND(AVG(rs.review_length), 1),
    COALESCE(SUM(rs.helpful_votes), 0),
    NOW()
FROM gold.review_sentiment rs
LEFT JOIN gold.product_category_lookup cl ON cl.asin = rs.asin
GROUP BY rs.asin, cl.category;

CREATE INDEX idx_pm_cat ON gold.product_metrics(category);

SELECT 'product_metrics' AS step, COUNT(*) AS products FROM gold.product_metrics;


-- ═══════════════════════════════════════════════════════════
-- 2. CATEGORY BENCHMARKS
-- ═══════════════════════════════════════════════════════════

DROP TABLE IF EXISTS gold.category_benchmarks;
CREATE TABLE gold.category_benchmarks (
    category                TEXT,
    rating_band             TEXT,
    product_count           INTEGER,
    total_reviews           INTEGER,
    avg_rating              NUMERIC(3,2),
    avg_compound_score      NUMERIC(5,4),
    pct_positive            NUMERIC(5,2),
    pct_negative            NUMERIC(5,2),
    pct_neutral             NUMERIC(5,2),
    avg_review_length       NUMERIC(8,1),
    avg_pct_disagreement    NUMERIC(5,2),
    computed_at             TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO gold.category_benchmarks
SELECT
    COALESCE(cl.category, 'Unknown'),
    CASE WHEN rs.rating <= 2 THEN '1-2 stars'
         WHEN rs.rating = 3  THEN '3 stars'
         ELSE '4-5 stars' END,
    COUNT(DISTINCT rs.asin),
    COUNT(*),
    ROUND(AVG(rs.rating), 2),
    ROUND(AVG(rs.vader_compound), 4),
    ROUND(COUNT(*) FILTER (WHERE rs.sentiment_label = 'POSITIVE') * 100.0 / COUNT(*), 2),
    ROUND(COUNT(*) FILTER (WHERE rs.sentiment_label = 'NEGATIVE') * 100.0 / COUNT(*), 2),
    ROUND(COUNT(*) FILTER (WHERE rs.sentiment_label = 'NEUTRAL')  * 100.0 / COUNT(*), 2),
    ROUND(AVG(rs.review_length), 1),
    ROUND(COUNT(*) FILTER (WHERE rs.rating_sentiment_agree = FALSE) * 100.0
          / NULLIF(COUNT(*) FILTER (WHERE rs.rating_sentiment_agree IS NOT NULL), 0), 2),
    NOW()
FROM gold.review_sentiment rs
LEFT JOIN gold.product_category_lookup cl ON cl.asin = rs.asin
GROUP BY COALESCE(cl.category, 'Unknown'),
         CASE WHEN rs.rating <= 2 THEN '1-2 stars'
              WHEN rs.rating = 3  THEN '3 stars'
              ELSE '4-5 stars' END;

SELECT 'category_benchmarks' AS step, COUNT(*) FROM gold.category_benchmarks;


-- ═══════════════════════════════════════════════════════════
-- 3. FAKE REVIEW SIGNALS
-- ═══════════════════════════════════════════════════════════

DROP TABLE IF EXISTS gold.fake_review_signals;
CREATE TABLE gold.fake_review_signals (
    reviewer_id         TEXT PRIMARY KEY,
    review_count        INTEGER,
    avg_rating          NUMERIC(3,2),
    rating_stddev       NUMERIC(4,3),
    avg_compound        NUMERIC(5,4),
    pct_positive        NUMERIC(5,2),
    avg_review_length   NUMERIC(8,1),
    length_stddev       NUMERIC(8,1),
    pct_verified        NUMERIC(5,2),
    distinct_products   INTEGER,
    reviews_per_product NUMERIC(6,2),
    risk_score          INTEGER,
    risk_tier           TEXT,
    computed_at         TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO gold.fake_review_signals
WITH scored AS (
    SELECT
        reviewer_id,
        COUNT(*)                                               AS review_count,
        ROUND(AVG(rating), 2)                                  AS avg_rating,
        ROUND(COALESCE(STDDEV(rating), 0), 3)                  AS rating_stddev,
        ROUND(AVG(vader_compound), 4)                          AS avg_compound,
        ROUND(COUNT(*) FILTER (WHERE sentiment_label = 'POSITIVE')
              * 100.0 / COUNT(*), 2)                           AS pct_positive,
        ROUND(AVG(review_length), 1)                           AS avg_review_length,
        ROUND(COALESCE(STDDEV(review_length), 0), 1)           AS length_stddev,
        ROUND(COUNT(*) FILTER (WHERE verified_purchase = TRUE)
              * 100.0 / NULLIF(COUNT(*) FILTER (
                  WHERE verified_purchase IS NOT NULL), 0), 2) AS pct_verified,
        COUNT(DISTINCT asin)                                   AS distinct_products,
        ROUND(COUNT(*) * 1.0 / NULLIF(COUNT(DISTINCT asin), 0), 2) AS reviews_per_product
    FROM gold.review_sentiment
    GROUP BY reviewer_id
    HAVING COUNT(*) >= 3
),
with_risk AS (
    SELECT *,
        LEAST(100, GREATEST(0,
            CASE WHEN rating_stddev < 0.3 AND review_count > 5 THEN 30
                 ELSE GREATEST(0, (30 - LEAST(rating_stddev, 2) * 15)::INT) END
            + CASE WHEN avg_review_length < 50 THEN 20
                   WHEN avg_review_length < 100 THEN 10 ELSE 0 END
            + CASE WHEN review_count > 50 THEN 20
                   WHEN review_count > 20 THEN 10
                   WHEN review_count > 10 THEN 5 ELSE 0 END
            + CASE WHEN pct_positive > 98 AND review_count > 5 THEN 15 ELSE 0 END
            + CASE WHEN length_stddev < 20 AND review_count > 5 THEN 15 ELSE 0 END
        )) AS risk_score
    FROM scored
)
SELECT *,
    CASE WHEN risk_score >= 70 THEN 'CRITICAL'
         WHEN risk_score >= 50 THEN 'HIGH'
         WHEN risk_score >= 30 THEN 'MEDIUM'
         ELSE 'LOW' END AS risk_tier,
    NOW() AS computed_at
FROM with_risk;

CREATE INDEX idx_frs_risk ON gold.fake_review_signals(risk_score DESC);

SELECT 'fake_review_signals' AS step, COUNT(*) AS reviewers,
       COUNT(*) FILTER (WHERE risk_tier = 'CRITICAL') AS critical,
       COUNT(*) FILTER (WHERE risk_tier = 'HIGH') AS high,
       COUNT(*) FILTER (WHERE risk_tier = 'MEDIUM') AS medium
FROM gold.fake_review_signals;


-- ═══════════════════════════════════════════════════════════
-- 4. SENTIMENT TRENDS — monthly for line charts
-- ═══════════════════════════════════════════════════════════

DROP TABLE IF EXISTS gold.sentiment_trends;
CREATE TABLE gold.sentiment_trends (
    year_month       TEXT,
    source           TEXT,
    review_count     INTEGER,
    avg_rating       NUMERIC(3,2),
    avg_compound     NUMERIC(5,4),
    pct_positive     NUMERIC(5,2),
    pct_negative     NUMERIC(5,2),
    pct_neutral      NUMERIC(5,2),
    pct_disagreement NUMERIC(5,2)
);

INSERT INTO gold.sentiment_trends
SELECT
    TO_CHAR(review_date, 'YYYY-MM'),
    source,
    COUNT(*),
    ROUND(AVG(rating), 2),
    ROUND(AVG(vader_compound), 4),
    ROUND(COUNT(*) FILTER (WHERE sentiment_label = 'POSITIVE') * 100.0 / COUNT(*), 2),
    ROUND(COUNT(*) FILTER (WHERE sentiment_label = 'NEGATIVE') * 100.0 / COUNT(*), 2),
    ROUND(COUNT(*) FILTER (WHERE sentiment_label = 'NEUTRAL')  * 100.0 / COUNT(*), 2),
    ROUND(COUNT(*) FILTER (WHERE rating_sentiment_agree = FALSE) * 100.0
          / NULLIF(COUNT(*) FILTER (WHERE rating_sentiment_agree IS NOT NULL), 0), 2)
FROM gold.review_sentiment
WHERE review_date IS NOT NULL
GROUP BY TO_CHAR(review_date, 'YYYY-MM'), source
ORDER BY 1, 2;

CREATE INDEX idx_st_ym ON gold.sentiment_trends(year_month);


-- ═══════════════════════════════════════════════════════════
-- FINAL VERIFICATION
-- ═══════════════════════════════════════════════════════════
SELECT '1. product_category_lookup' AS tbl, COUNT(*) AS rows FROM gold.product_category_lookup
UNION ALL SELECT '2. product_metrics',      COUNT(*) FROM gold.product_metrics
UNION ALL SELECT '3. category_benchmarks',  COUNT(*) FROM gold.category_benchmarks
UNION ALL SELECT '4. fake_review_signals',  COUNT(*) FROM gold.fake_review_signals
UNION ALL SELECT '5. sentiment_trends',     COUNT(*) FROM gold.sentiment_trends
UNION ALL SELECT '6. review_sentiment',     COUNT(*) FROM gold.review_sentiment
ORDER BY tbl;
