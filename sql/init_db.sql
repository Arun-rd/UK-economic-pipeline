CREATE DATABASE ons_pipeline;
\c ons_pipeline;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.ons_raw (
    id              SERIAL PRIMARY KEY,
    dataset_id      VARCHAR(50)   NOT NULL,
    series_id       VARCHAR(50)   NOT NULL,
    period          VARCHAR(10)   NOT NULL,
    value           TEXT,
    raw_json        JSONB,
    ingested_at     TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_bronze_dataset_series ON bronze.ons_raw (dataset_id, series_id, period);
COMMENT ON TABLE bronze.ons_raw IS 'Bronze: raw ONS API responses, append-only';

CREATE TABLE IF NOT EXISTS silver.ons_indicators (
    id              SERIAL PRIMARY KEY,
    dataset_id      VARCHAR(50)   NOT NULL,
    series_id       VARCHAR(50)   NOT NULL,
    indicator_name  VARCHAR(200),
    period          VARCHAR(10)   NOT NULL,
    period_date     DATE,
    value_raw       TEXT,
    value_numeric   NUMERIC(18,4),
    unit            VARCHAR(50),
    is_valid        BOOLEAN DEFAULT TRUE,
    loaded_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (dataset_id, series_id, period)
);
CREATE INDEX IF NOT EXISTS idx_silver_series_period ON silver.ons_indicators (series_id, period_date);
COMMENT ON TABLE silver.ons_indicators IS 'Silver: cleaned, typed ONS indicators';
COMMENT ON SCHEMA gold IS 'Gold: business-ready marts built by dbt';
