-- 1. Latest Gold Summary
SELECT *
FROM gold_daily_summary
ORDER BY date DESC
LIMIT 20;


-- 2. Bullish vs Bearish Signals
SELECT
    signal,
    COUNT(*) AS total
FROM gold_daily_summary
GROUP BY signal;


-- 3. Top Positive Daily Returns
SELECT
    ticker,
    date,
    daily_return
FROM gold_daily_summary
WHERE daily_return IS NOT NULL
ORDER BY daily_return DESC
LIMIT 10;


-- 4. Highest Volatility
SELECT
    ticker,
    date,
    volatility_7
FROM gold_daily_summary
WHERE volatility_7 IS NOT NULL
ORDER BY volatility_7 DESC
LIMIT 10;


-- 5. Largest Volume Spikes
SELECT
    ticker,
    date,
    volume,
    volume_ma_7
FROM gold_daily_summary
ORDER BY volume DESC
LIMIT 10;


-- 6. All Detected Anomalies
SELECT *
FROM gold_anomalies
ORDER BY detected_at DESC;


-- 7. Anomaly Counts by Type
SELECT
    anomaly_type,
    COUNT(*) AS total
FROM gold_anomalies
GROUP BY anomaly_type;


-- 8. Stocks with Most Anomalies
SELECT
    ticker,
    COUNT(*) AS anomaly_count
FROM gold_anomalies
GROUP BY ticker
ORDER BY anomaly_count DESC;