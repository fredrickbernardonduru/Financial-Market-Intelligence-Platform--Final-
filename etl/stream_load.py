import os
import json
import logging
from datetime import datetime
from typing import Dict, Any

import psycopg2
from dotenv import load_dotenv
from kafka import KafkaConsumer

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


def insert_stock_tick(record: Dict[str, Any]) -> None:
    query = """
        INSERT INTO bronze_kafka_stock_ticks (
            ticker, timestamp, open, high, low, close, volume
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (ticker, timestamp)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            consumed_at = CURRENT_TIMESTAMP;
    """

    conn = None
    cursor = None

    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute(
            query,
            (
                record["ticker"],
                datetime.fromisoformat(record["timestamp"]),
                record["open"],
                record["high"],
                record["low"],
                record["close"],
                record["volume"],
            ),
        )

        conn.commit()

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Failed to insert Kafka record: {e}")
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def consume_and_load():
    consumer = KafkaConsumer(
        os.getenv("KAFKA_TOPIC"),
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="stock-stream-loader-group",
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    logger.info("Stream loader started. Consuming Kafka messages into PostgreSQL...")

    try:
        for message in consumer:
            record = message.value
            insert_stock_tick(record)
            logger.info(f"Loaded Kafka record into bronze table: {record['ticker']} {record['timestamp']}")

    finally:
        consumer.close()
        logger.info("Stream loader stopped")


if __name__ == "__main__":
    consume_and_load()