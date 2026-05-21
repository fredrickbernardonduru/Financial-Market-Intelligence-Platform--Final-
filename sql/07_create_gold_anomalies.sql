CREATE TABLE IF NOT EXISTS gold_anomalies (
    ticker TEXT NOT NULL,
    date DATE NOT NULL,
    close DOUBLE PRECISION,
    volume BIGINT,
    daily_return DOUBLE PRECISION,
    volatility_7 DOUBLE PRECISION,
    volume_ma_7 DOUBLE PRECISION,
    anomaly_type TEXT,
    anomaly_reason TEXT,
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date, anomaly_type)
);