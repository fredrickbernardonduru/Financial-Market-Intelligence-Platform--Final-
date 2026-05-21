import os
import logging

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


def fetch_silver_data() -> pd.DataFrame:
    query = """
        SELECT
            ticker,
            timestamp,
            close,
            volume,
            daily_return,
            ma_7,
            ma_20,
            volatility_7,
            volume_ma_7
        FROM silver_stock_indicators
        ORDER BY ticker, timestamp;
    """

    conn = get_connection()

    try:
        df = pd.read_sql(query, conn)
        logger.info(f"Fetched {len(df)} records from silver_stock_indicators")
        return df

    finally:
        conn.close()


def generate_signal(row) -> str:
    if pd.isna(row["ma_7"]) or pd.isna(row["ma_20"]) or pd.isna(row["daily_return"]):
        return "neutral"

    if row["ma_7"] > row["ma_20"] and row["daily_return"] > 0:
        return "bullish"

    if row["ma_7"] < row["ma_20"] and row["daily_return"] < 0:
        return "bearish"

    return "neutral"


def build_gold_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        logger.warning("No silver data available for gold aggregation")
        return df

    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date

    df["signal"] = df.apply(generate_signal, axis=1)

    gold_df = df[
        [
            "ticker",
            "date",
            "close",
            "volume",
            "daily_return",
            "ma_7",
            "ma_20",
            "volatility_7",
            "volume_ma_7",
            "signal",
        ]
    ]

    logger.info(f"Built {len(gold_df)} gold summary records")

    return gold_df


def save_gold_summary(df: pd.DataFrame) -> None:
    if df.empty:
        logger.warning("No gold summary data to save")
        return

    query = """
        INSERT INTO gold_daily_summary (
            ticker,
            date,
            close,
            volume,
            daily_return,
            ma_7,
            ma_20,
            volatility_7,
            volume_ma_7,
            signal
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, date)
        DO UPDATE SET
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            daily_return = EXCLUDED.daily_return,
            ma_7 = EXCLUDED.ma_7,
            ma_20 = EXCLUDED.ma_20,
            volatility_7 = EXCLUDED.volatility_7,
            volume_ma_7 = EXCLUDED.volume_ma_7,
            signal = EXCLUDED.signal,
            created_at = CURRENT_TIMESTAMP;
    """

    records = [
        (
            row["ticker"],
            row["date"],
            None if pd.isna(row["close"]) else float(row["close"]),
            None if pd.isna(row["volume"]) else int(row["volume"]),
            None if pd.isna(row["daily_return"]) else float(row["daily_return"]),
            None if pd.isna(row["ma_7"]) else float(row["ma_7"]),
            None if pd.isna(row["ma_20"]) else float(row["ma_20"]),
            None if pd.isna(row["volatility_7"]) else float(row["volatility_7"]),
            None if pd.isna(row["volume_ma_7"]) else float(row["volume_ma_7"]),
            row["signal"],
        )
        for _, row in df.iterrows()
    ]

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.executemany(query, records)
        conn.commit()
        logger.info(f"Saved {len(records)} records into gold_daily_summary")

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to save gold summary: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


def run_gold_pipeline() -> pd.DataFrame:
    silver_df = fetch_silver_data()
    gold_df = build_gold_summary(silver_df)
    save_gold_summary(gold_df)
    return gold_df


if __name__ == "__main__":
    run_gold_pipeline()