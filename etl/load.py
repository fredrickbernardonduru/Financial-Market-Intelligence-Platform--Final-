import os
import logging
from typing import List, Dict, Any

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_batch

load_dotenv(dotenv_path=".env")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def get_connection():
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_name = os.getenv("DB_NAME")
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")

    missing = [
        name for name, value in {
            "DB_HOST": db_host,
            "DB_PORT": db_port,
            "DB_NAME": db_name,
            "DB_USER": db_user,
            "DB_PASSWORD": db_password,
        }.items() if not value
    ]

    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return psycopg2.connect(
        host=db_host,
        port=int(db_port),
        dbname=db_name,
        user=db_user,
        password=db_password,
    )


def load_to_postgres(records: List[Dict[str, Any]]) -> None:
    """
    Bulk load records into PostgreSQL with UPSERT logic.
    """

    if not records:
        logger.warning("No records to load")
        return

    query = """
        INSERT INTO bronze_stock_ticks (
            ticker, timestamp, open, high, low, close, volume
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, timestamp)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume;
    """

    data = [
        (
            r["ticker"],
            r["timestamp"],
            r["open"],
            r["high"],
            r["low"],
            r["close"],
            r["volume"],
        )
        for r in records
    ]

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        execute_batch(cursor, query, data, page_size=100)
        conn.commit()

        logger.info(f"Loaded {len(records)} records into PostgreSQL")

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Load failed: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()