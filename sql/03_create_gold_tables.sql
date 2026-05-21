-- Gold layer: business-level aggregations served to dashboards and analysts

-- Daily price + signal summary (one row per ticker per day)
CREATE TABLE IF NOT EXISTS gold_market_summary (
    ticker          TEXT             NOT NULL,
    date            DATE             NOT NULL,
    open            DOUBLE PRECISION,
    high            DOUBLE PRECISION,
    low             DOUBLE PRECISION,
    close           DOUBLE PRECISION,
    volume          BIGINT,
    avg_close       DOUBLE PRECISION,
    price_range     DOUBLE PRECISION,   -- high - low
    daily_return    DOUBLE PRECISION,
    cumulative_return DOUBLE PRECISION,
    signal          TEXT,               -- bullish / bearish / neutral
    created_at      TIMESTAMP        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, date)
);

CREATE INDEX IF NOT EXISTS idx_gold_market_summary_date
    ON gold_market_summary (date DESC);

-- Volatility + correlation metrics per ticker per week
CREATE TABLE IF NOT EXISTS gold_volatility_metrics (
    ticker              TEXT             NOT NULL,
    week_start          DATE             NOT NULL,
    avg_daily_return    DOUBLE PRECISION,
    std_daily_return    DOUBLE PRECISION,   -- realised weekly volatility
    max_daily_return    DOUBLE PRECISION,
    min_daily_return    DOUBLE PRECISION,
    avg_volume          DOUBLE PRECISION,
    sharpe_proxy        DOUBLE PRECISION,   -- mean_return / std_return
    created_at          TIMESTAMP        DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, week_start)
);

CREATE INDEX IF NOT EXISTS idx_gold_volatility_week
    ON gold_volatility_metrics (week_start DESC);
