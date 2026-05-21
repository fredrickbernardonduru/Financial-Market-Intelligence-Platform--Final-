import os
import json
import logging
from typing import Optional

from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv(dotenv_path=".env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def get_consumer(topic: Optional[str] = None) -> KafkaConsumer:
    bootstrap_servers = _get_required_env("KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic = topic or _get_required_env("KAFKA_TOPIC")

    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",      # read from beginning (good for testing)
        enable_auto_commit=True,
        group_id="stock-consumer-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )

    return consumer


def consume_messages(topic: Optional[str] = None) -> None:
    consumer = None

    try:
        consumer = get_consumer(topic)
        logger.info("Kafka consumer started. Listening for messages...")

        for message in consumer:
            key = message.key
            value = message.value

            logger.info(f"Received message | Key: {key} | Value: {value}")

    except Exception as e:
        logger.error(f"Kafka consumer error: {e}")
        raise

    finally:
        if consumer:
            consumer.close()
            logger.info("Kafka consumer closed")


if __name__ == "__main__":
    consume_messages()