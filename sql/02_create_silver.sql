DROP TABLE IF EXISTS silver.reviews;
CREATE TABLE silver.reviews (

    -- identifiers
    review_id           TEXT,
    reviewer_id         TEXT        NOT NULL,   
    asin                TEXT        NOT NULL,   
    product_title       TEXT,                   

    -- review content
    rating              NUMERIC(2,1) NOT NULL,  
    review_title        TEXT,                   
    review_text         TEXT,                   
    review_date         DATE,                   
    review_length       INTEGER,                

    -- trust signals
    verified_purchase   BOOLEAN,                
    helpful_votes       INTEGER,                
    total_votes         INTEGER,                
    vine_reviewer       BOOLEAN,                

    -- pipeline tracking
    source              TEXT        NOT NULL,   
    raw_source_id       TEXT,
    loaded_at           TIMESTAMPTZ DEFAULT NOW()
);



DROP TABLE IF EXISTS silver.rejected_reviews;
CREATE TABLE silver.rejected_reviews (
    rejection_id        SERIAL PRIMARY KEY,
    source              TEXT,
    raw_reviewer_id     TEXT,
    raw_asin            TEXT,
    raw_rating          TEXT,
    raw_review_text     TEXT,
    rejection_reason    TEXT NOT NULL,

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
