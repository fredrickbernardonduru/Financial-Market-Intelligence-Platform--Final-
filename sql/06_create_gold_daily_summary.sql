CREATE TABLE IF NOT EXISTS gold_daily_summary (
    ticker TEXT NOT NULL,
    date DATE NOT NULL,
    close DOUBLE PRECISION,
    volume BIGINT,
    daily_return DOUBLE PRECISION,
    ma_7 DOUBLE PRECISION,
    ma_20 DOUBLE PRECISION,
    volatility_7 DOUBLE PRECISION,
    volume_ma_7 DOUBLE PRECISION,
    signal TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date)
);