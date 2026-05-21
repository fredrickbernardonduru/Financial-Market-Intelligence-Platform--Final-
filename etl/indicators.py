import os
import logging
from typing import Optional

import pandas as pd
import psycopg2
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def fetch_bronze_data(ticker: Optional[str] = None) -> pd.DataFrame:
    query = """
        SELECT
            ticker,
            timestamp,
            open,
            high,
            low,
            close,
            volume
        FROM bronze_kafka_stock_ticks
    """

    params = None

    if ticker:
        query += " WHERE ticker = %s"
        params = (ticker,)

    query += " ORDER BY ticker, timestamp"

    conn = get_connection()

    try:
        df = pd.read_sql(query, conn, params=params)
        logger.info(f"Fetched {len(df)} records from bronze_kafka_stock_ticks")
        return df

    finally:
        conn.close()


def calculate_indicators(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        logger.warning("No data available for indicator calculation")
        return df

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    df = df.sort_values(["ticker", "timestamp"])

    df["daily_return"] = df.groupby("ticker")["close"].pct_change()

    df["ma_7"] = (
        df.groupby("ticker")["close"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
    )

    df["ma_20"] = (
        df.groupby("ticker")["close"]
        .transform(lambda x: x.rolling(window=20, min_periods=1).mean())
    )

    df["volatility_7"] = (
        df.groupby("ticker")["daily_return"]
        .transform(lambda x: x.rolling(window=7, min_periods=2).std())
    )

    df["volume_ma_7"] = (
        df.groupby("ticker")["volume"]
        .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
    )

    logger.info("Calculated indicators: daily_return, ma_7, ma_20, volatility_7, volume_ma_7")

    return df


def save_indicators(df: pd.DataFrame) -> None:
    if df.empty:
        logger.warning("No indicator data to save")
        return

    query = """
        INSERT INTO silver_stock_indicators (
            ticker,
            timestamp,
            close,
            volume,
            daily_return,
            ma_7,
            ma_20,
            volatility_7,
            volume_ma_7
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, timestamp)
        DO UPDATE SET
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            daily_return = EXCLUDED.daily_return,
            ma_7 = EXCLUDED.ma_7,
            ma_20 = EXCLUDED.ma_20,
            volatility_7 = EXCLUDED.volatility_7,
            volume_ma_7 = EXCLUDED.volume_ma_7;
    """

    records = [
        (
            row["ticker"],
            row["timestamp"],
            row["close"],
            int(row["volume"]),
            None if pd.isna(row["daily_return"]) else float(row["daily_return"]),
            None if pd.isna(row["ma_7"]) else float(row["ma_7"]),
            None if pd.isna(row["ma_20"]) else float(row["ma_20"]),
            None if pd.isna(row["volatility_7"]) else float(row["volatility_7"]),
            None if pd.isna(row["volume_ma_7"]) else float(row["volume_ma_7"]),
        )
        for _, row in df.iterrows()
    ]

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.executemany(query, records)
        conn.commit()
        logger.info(f"Saved {len(records)} records into silver_stock_indicators")

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to save indicators: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


def run_indicators_pipeline(ticker: Optional[str] = None) -> pd.DataFrame:
    bronze_df = fetch_bronze_data(ticker)
    indicators_df = calculate_indicators(bronze_df)
    save_indicators(indicators_df)
    return indicators_df


if __name__ == "__main__":
    run_indicators_pipeline()