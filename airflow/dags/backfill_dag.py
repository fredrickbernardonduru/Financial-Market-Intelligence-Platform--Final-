"""
backfill_dag.py
Manually-triggered DAG to backfill historical data for one or more symbols.

Usage (Airflow UI → Trigger DAG w/ config):
  {"symbols": ["AAPL", "MSFT", "GOOGL"], "outputsize": "full"}

If no config is supplied, defaults are used.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "ben",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DEFAULT_SYMBOLS = ["AAPL", "MSFT"]


def backfill_extract(**context):
    """Extract full history for each symbol and push to XCom."""
    import json
    from etl.extract import AlphaVantageClient, save_raw_json

    conf = context["dag_run"].conf or {}
    symbols = conf.get("symbols", DEFAULT_SYMBOLS)
    outputsize = conf.get("outputsize", "full")  # 'full' = 20 years of daily data

    client = AlphaVantageClient()
    paths = {}

    for symbol in symbols:
        # Temporarily override outputsize by patching params inside get_intraday
        import os, requests, time
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": os.getenv("ALPHA_VANTAGE_KEY"),
            "outputsize": outputsize,
        }
        time.sleep(12)  # respect free-tier rate limit (5 req/min)
        response = requests.get("https://www.alphavantage.co/query", params=params)
        response.raise_for_status()
        raw = response.json()

        path = save_raw_json(raw, symbol, base_path="data/backfill")
        paths[symbol] = path
        print(f"[backfill] {symbol} → saved to {path}")

    context["ti"].xcom_push(key="raw_paths", value=json.dumps(paths))
    context["ti"].xcom_push(key="symbols", value=symbols)


def backfill_clean_validate(**context):
    """Normalise and validate all extracted symbols."""
    import json
    from etl.clean import normalize_daily_series
    from etl.validate import validate_batch

    symbols = context["ti"].xcom_pull(key="symbols")
    raw_paths = json.loads(context["ti"].xcom_pull(key="raw_paths"))

    total_valid = 0

    for symbol in symbols:
        path = raw_paths[symbol]
        with open(path) as f:
            import json as _json
            raw = _json.load(f)

        records = normalize_daily_series(raw)
        valid_records = validate_batch(records)
        total_valid += len(valid_records)
        print(f"[backfill] {symbol}: {len(records)} raw → {len(valid_records)} valid")

    print(f"[backfill] Total valid records across all symbols: {total_valid}")
    context["ti"].xcom_push(key="total_valid", value=total_valid)


def backfill_load(**context):
    """Load validated records into bronze tables."""
    import json
    from etl.clean import normalize_daily_series
    from etl.validate import validate_batch
    from etl.load import load_to_postgres

    symbols = context["ti"].xcom_pull(key="symbols")
    raw_paths = json.loads(context["ti"].xcom_pull(key="raw_paths"))

    for symbol in symbols:
        path = raw_paths[symbol]
        with open(path) as f:
            import json as _json
            raw = _json.load(f)

        records = normalize_daily_series(raw)
        valid_records = validate_batch(records)

        if valid_records:
            load_to_postgres(valid_records)
            print(f"[backfill] {symbol}: loaded {len(valid_records)} records into bronze_stock_ticks")
        else:
            print(f"[backfill] {symbol}: no valid records to load")


def backfill_indicators(**context):
    from etl.indicators import run_indicators_pipeline

    symbols = context["ti"].xcom_pull(key="symbols")

    for symbol in symbols:
        df = run_indicators_pipeline(ticker=symbol)
        print(f"[backfill] {symbol}: computed indicators for {len(df)} rows")


def backfill_gold(**context):
    from etl.gold_aggregations import run_gold_pipeline

    df = run_gold_pipeline()
    print(f"[backfill] Gold aggregation complete. Rows: {len(df)}")


def backfill_anomalies(**context):
    from etl.anomaly_detection import run_anomaly_pipeline

    df = run_anomaly_pipeline()
    print(f"[backfill] Anomaly detection complete. Anomalies found: {len(df)}")


with DAG(
    dag_id="backfill_dag",
    default_args=default_args,
    description="Manual backfill of historical market data for given symbols",
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,   # manual trigger only
    catchup=False,
    tags=["market", "backfill", "manual"],
) as dag:

    t_extract = PythonOperator(
        task_id="backfill_extract",
        python_callable=backfill_extract,
    )

    t_clean = PythonOperator(
        task_id="backfill_clean_validate",
        python_callable=backfill_clean_validate,
    )

    t_load = PythonOperator(
        task_id="backfill_load",
        python_callable=backfill_load,
    )

    t_indicators = PythonOperator(
        task_id="backfill_indicators",
        python_callable=backfill_indicators,
    )

    t_gold = PythonOperator(
        task_id="backfill_gold",
        python_callable=backfill_gold,
    )

    t_anomalies = PythonOperator(
        task_id="backfill_anomalies",
        python_callable=backfill_anomalies,
    )

    t_extract >> t_clean >> t_load >> t_indicators >> t_gold >> t_anomalies
