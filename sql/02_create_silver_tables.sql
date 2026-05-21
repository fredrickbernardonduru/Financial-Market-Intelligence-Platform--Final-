-- Silver layer: cleaned + enriched records from bronze_stock_ticks (batch ETL path)
CREATE TABLE IF NOT EXISTS silver_stock_ticks (
    ticker       TEXT             NOT NULL,
    timestamp    TIMESTAMP        NOT NULL,
    open         DOUBLE PRECISION NOT NULL,
    high         DOUBLE PRECISION NOT NULL,
    low          DOUBLE PRECISION NOT NULL,
    close        DOUBLE PRECISION NOT NULL,
    volume       BIGINT           NOT NULL,
    daily_return DOUBLE PRECISION,
    ingested_at  TIMESTAMP        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, timestamp)
);

-- Index for time-range scans common in analytics
CREATE INDEX IF NOT EXISTS idx_silver_ticks_ticker_ts
    ON silver_stock_ticks (ticker, timestamp DESC);
