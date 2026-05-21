import os
import json
import logging
from typing import List, Dict, Any

from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv(dotenv_path=".env")

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _serialize_record(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ticker": record["ticker"],
        "timestamp": record["timestamp"].isoformat(),
        "open": record["open"],
        "high": record["high"],
        "low": record["low"],
        "close": record["close"],
        "volume": record["volume"],
    }


def get_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=_get_required_env("KAFKA_BOOTSTRAP_SERVERS"),
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )


def publish_to_kafka(records: List[Dict[str, Any]], topic: str | None = None) -> None:
    if not records:
        logger.warning("No records to publish to Kafka")
        return

    kafka_topic = topic or _get_required_env("KAFKA_TOPIC")
    producer = get_producer()

    try:
        for record in records:
            payload = _serialize_record(record)

            producer.send(
                topic=kafka_topic,
                key=payload["ticker"],
                value=payload,
            )

        producer.flush()
        logger.info(f"Published {len(records)} records to Kafka topic '{kafka_topic}'")

    finally:
        producer.close()