-- Create the three Medallion schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Bronze: UCSD Reviews table
CREATE TABLE IF NOT EXISTS bronze.ucsd_reviews (
    rating            TEXT,
    title             TEXT,
    text              TEXT,
    images            TEXT,
    asin              TEXT,
    parent_asin       TEXT,
    user_id           TEXT,
    timestamp         TEXT,
    verified_purchase TEXT,
    source_category   TEXT,
    raw_source        TEXT DEFAULT 'ucsd_2023',
    loaded_at         TIMESTAMPTZ DEFAULT NOW()
);

-- Bronze: Kaggle/AWS Reviews table
CREATE TABLE IF NOT EXISTS bronze.aws_reviews (
    marketplace       TEXT,
    customer_id       TEXT,
    review_id         TEXT,
    product_id        TEXT,
    product_parent    TEXT,
    product_title     TEXT,
    product_category  TEXT,
    star_rating       TEXT,
    helpful_votes     TEXT,
    total_votes       TEXT,
    vine              TEXT,
    verified_purchase TEXT,
    review_headline   TEXT,
    review_body       TEXT,
    review_date       TEXT,
    raw_source        TEXT DEFAULT 'kaggle_tsv',
    loaded_at         TIMESTAMPTZ DEFAULT NOW()
);

-- Bronze: Pipeline log table
CREATE TABLE IF NOT EXISTS bronze.pipeline_log (
    log_id          SERIAL PRIMARY KEY,
    run_id          TEXT NOT NULL,
    phase           TEXT NOT NULL,
    table_name      TEXT NOT NULL,
    source_file     TEXT,
    rows_attempted  INTEGER,
    rows_loaded     INTEGER,
    rows_rejected   INTEGER,
    status          TEXT,
    error_message   TEXT,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_ucsd_asin     ON bronze.ucsd_reviews(asin);
CREATE INDEX IF NOT EXISTS idx_ucsd_user     ON bronze.ucsd_reviews(user_id);
CREATE INDEX IF NOT EXISTS idx_ucsd_category ON bronze.ucsd_reviews(source_category);
CREATE INDEX IF NOT EXISTS idx_aws_product   ON bronze.aws_reviews(product_id);
CREATE INDEX IF NOT EXISTS idx_aws_category  ON bronze.aws_reviews(product_category);