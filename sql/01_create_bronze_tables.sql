CREATE TABLE IF NOT EXISTS bronze_stock_ticks (
    ticker TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION,
    volume BIGINT,
    PRIMARY KEY (ticker, timestamp)
);