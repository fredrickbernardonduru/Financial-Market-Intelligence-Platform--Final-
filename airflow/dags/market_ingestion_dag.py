from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    "owner": "ben",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def run_market_pipeline():
    from etl.pipeline import run_pipeline

    symbols = ["AAPL", "MSFT"]

    total_records = 0

    for symbol in symbols:
        records = run_pipeline(symbol)
        total_records += len(records)

    print(f"Market ingestion completed. Total records processed: {total_records}")


def run_indicators():
    from etl.indicators import run_indicators_pipeline

    df = run_indicators_pipeline()
    print(f"Indicators pipeline completed. Records processed: {len(df)}")


def run_gold_aggregations():
    from etl.gold_aggregations import run_gold_pipeline

    df = run_gold_pipeline()
    print(f"Gold aggregation completed. Records processed: {len(df)}")


def run_anomaly_detection():
    from etl.anomaly_detection import run_anomaly_pipeline

    df = run_anomaly_pipeline()
    print(f"Anomaly detection completed. Anomalies detected: {len(df)}")


def validate_pipeline_outputs():
    import os
    import psycopg2

    required_tables = {
        "bronze_stock_ticks": 1,
        "silver_stock_indicators": 1,
        "gold_daily_summary": 1,
        "gold_anomalies": 0,
    }

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

    try:
        cursor = conn.cursor()

        print("\n========== PIPELINE OUTPUT VALIDATION ==========")

        for table_name, minimum_count in required_tables.items():
            cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
            count = cursor.fetchone()[0]

            print(f"{table_name}: {count} rows")

            if count < minimum_count:
                raise ValueError(
                    f"Validation failed: {table_name} has {count} rows, "
                    f"expected at least {minimum_count}"
                )

        print("Pipeline output validation passed successfully.")
        print("================================================\n")

    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id="market_ingestion_dag",
    default_args=default_args,
    description="Financial Market Intelligence ETL pipeline",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["market", "etl", "kafka", "postgres"],
) as dag:

    extract_validate_load_kafka = PythonOperator(
        task_id="extract_validate_load_kafka",
        python_callable=run_market_pipeline,
    )

    calculate_indicators = PythonOperator(
        task_id="calculate_indicators",
        python_callable=run_indicators,
    )

    gold_aggregations = PythonOperator(
        task_id="gold_aggregations",
        python_callable=run_gold_aggregations,
    )

    anomaly_detection = PythonOperator(
        task_id="anomaly_detection",
        python_callable=run_anomaly_detection,
    )

    validate_outputs = PythonOperator(
        task_id="validate_pipeline_outputs",
        python_callable=validate_pipeline_outputs,
    )

    (
        extract_validate_load_kafka
        >> calculate_indicators
        >> gold_aggregations
        >> anomaly_detection
        >> validate_outputs
    )