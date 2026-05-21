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


def fetch_gold_data() -> pd.DataFrame:
    query = """
        SELECT
            ticker,
            date,
            close,
            volume,
            daily_return,
            volatility_7,
            volume_ma_7
        FROM gold_daily_summary
        ORDER BY ticker, date;
    """

    conn = get_connection()

    try:
        df = pd.read_sql(query, conn)
        logger.info(f"Fetched {len(df)} records from gold_daily_summary")
        return df

    finally:
        conn.close()


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        logger.warning("No gold data available for anomaly detection")
        return pd.DataFrame()

    anomalies = []

    for _, row in df.iterrows():

        # Large positive movement
        if pd.notna(row["daily_return"]) and row["daily_return"] > 0.05:
            anomalies.append(
                {
                    "ticker": row["ticker"],
                    "date": row["date"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "daily_return": row["daily_return"],
                    "volatility_7": row["volatility_7"],
                    "volume_ma_7": row["volume_ma_7"],
                    "anomaly_type": "price_spike",
                    "anomaly_reason": "Daily return exceeded +5%",
                }
            )

        # Large negative movement
        if pd.notna(row["daily_return"]) and row["daily_return"] < -0.05:
            anomalies.append(
                {
                    "ticker": row["ticker"],
                    "date": row["date"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "daily_return": row["daily_return"],
                    "volatility_7": row["volatility_7"],
                    "volume_ma_7": row["volume_ma_7"],
                    "anomaly_type": "price_drop",
                    "anomaly_reason": "Daily return dropped below -5%",
                }
            )

        # Volume spike
        if (
            pd.notna(row["volume_ma_7"])
            and row["volume"] > row["volume_ma_7"] * 2
        ):
            anomalies.append(
                {
                    "ticker": row["ticker"],
                    "date": row["date"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "daily_return": row["daily_return"],
                    "volatility_7": row["volatility_7"],
                    "volume_ma_7": row["volume_ma_7"],
                    "anomaly_type": "volume_spike",
                    "anomaly_reason": "Volume exceeded 2x 7-day average",
                }
            )

        # High volatility
        if (
            pd.notna(row["volatility_7"])
            and row["volatility_7"] > 0.03
        ):
            anomalies.append(
                {
                    "ticker": row["ticker"],
                    "date": row["date"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "daily_return": row["daily_return"],
                    "volatility_7": row["volatility_7"],
                    "volume_ma_7": row["volume_ma_7"],
                    "anomaly_type": "high_volatility",
                    "anomaly_reason": "7-day volatility exceeded threshold",
                }
            )

    anomalies_df = pd.DataFrame(anomalies)

    logger.info(f"Detected {len(anomalies_df)} anomalies")

    return anomalies_df


def save_anomalies(df: pd.DataFrame) -> None:
    if df.empty:
        logger.warning("No anomalies to save")
        return

    query = """
        INSERT INTO gold_anomalies (
            ticker,
            date,
            close,
            volume,
            daily_return,
            volatility_7,
            volume_ma_7,
            anomaly_type,
            anomaly_reason
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, date, anomaly_type)
        DO UPDATE SET
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            daily_return = EXCLUDED.daily_return,
            volatility_7 = EXCLUDED.volatility_7,
            volume_ma_7 = EXCLUDED.volume_ma_7,
            anomaly_reason = EXCLUDED.anomaly_reason,
            detected_at = CURRENT_TIMESTAMP;
    """

    records = [
        (
            row["ticker"],
            row["date"],
            None if pd.isna(row["close"]) else float(row["close"]),
            None if pd.isna(row["volume"]) else int(row["volume"]),
            None if pd.isna(row["daily_return"]) else float(row["daily_return"]),
            None if pd.isna(row["volatility_7"]) else float(row["volatility_7"]),
            None if pd.isna(row["volume_ma_7"]) else float(row["volume_ma_7"]),
            row["anomaly_type"],
            row["anomaly_reason"],
        )
        for _, row in df.iterrows()
    ]

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.executemany(query, records)
        conn.commit()
        logger.info(f"Saved {len(records)} anomalies into gold_anomalies")

    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to save anomalies: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


def run_anomaly_pipeline() -> pd.DataFrame:
    gold_df = fetch_gold_data()
    anomalies_df = detect_anomalies(gold_df)
    save_anomalies(anomalies_df)
    return anomalies_df


if __name__ == "__main__":
    run_anomaly_pipeline()