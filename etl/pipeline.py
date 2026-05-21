import logging

from etl.extract import AlphaVantageClient
from etl.clean import normalize_daily_series
from etl.validate import validate_batch
from etl.load import load_to_postgres
from etl.kafka_producer import publish_to_kafka


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_pipeline(symbol: str):
    """
    Run the ETL pipeline for a single ticker symbol:
    extract -> clean -> validate -> load -> kafka
    """
    try:
        logger.info(f"Starting extraction for {symbol}")
        client = AlphaVantageClient()
        raw_data = client.get_intraday(symbol)

        logger.info(f"Normalizing data for {symbol}")
        records = normalize_daily_series(raw_data)
        logger.info(f"Extracted {len(records)} normalized records for {symbol}")

        logger.info(f"Validating data for {symbol}")
        valid_records = validate_batch(records)
        logger.info(f"Valid records for {symbol}: {len(valid_records)}")

        if valid_records:
            logger.info(f"Sample record for {symbol}: {valid_records[0]}")
        else:
            logger.warning(f"No valid records found for {symbol}")
            return []

        logger.info(f"Loading valid records for {symbol} into PostgreSQL")
        load_to_postgres(valid_records)

        logger.info(f"Publishing valid records for {symbol} to Kafka")
        publish_to_kafka(valid_records)

        logger.info(f"Pipeline finished successfully for {symbol}")
        return valid_records

    except Exception as e:
        logger.error(f"Pipeline failed for {symbol}: {e}")
        raise


if __name__ == "__main__":
    symbols = ["AAPL", "MSFT"]

    total_valid_records = 0

    for symbol in symbols:
        results = run_pipeline(symbol)
        total_valid_records += len(results)

    logger.info(f"Pipeline finished for all symbols. Total valid records processed: {total_valid_records}")