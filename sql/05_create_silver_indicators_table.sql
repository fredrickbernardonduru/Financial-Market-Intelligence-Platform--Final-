CREATE TABLE IF NOT EXISTS silver_stock_indicators (
    ticker TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    close DOUBLE PRECISION,
    volume BIGINT,
    daily_return DOUBLE PRECISION,
    ma_7 DOUBLE PRECISION,
    ma_20 DOUBLE PRECISION,
    volatility_7 DOUBLE PRECISION,
    volume_ma_7 DOUBLE PRECISION,
    PRIMARY KEY (ticker, timestamp)
);