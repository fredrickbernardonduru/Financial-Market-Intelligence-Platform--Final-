import logging
from typing import Dict, List, Any
from datetime import datetime


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def normalize_daily_series(raw_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Convert Alpha Vantage DAILY time series JSON into flat records.

    Output:
    [
        {
            "ticker": "AAPL",
            "timestamp": datetime,
            "open": float,
            "high": float,
            "low": float,
            "close": float,
            "volume": int
        },
        ...
    ]
    """

    try:
        metadata = raw_json.get("Meta Data", {})
        time_series = raw_json.get("Time Series (Daily)", {})

        if not metadata or not time_series:
            raise ValueError("Invalid API response format")

        ticker = metadata.get("2. Symbol")
        if not ticker:
            raise ValueError("Ticker symbol missing in metadata")

        records = []

        for date_str, values in time_series.items():
            try:
                record = {
                    "ticker": ticker,
                    "timestamp": datetime.strptime(date_str, "%Y-%m-%d"),
                    "open": float(values["1. open"]),
                    "high": float(values["2. high"]),
                    "low": float(values["3. low"]),
                    "close": float(values["4. close"]),
                    "volume": int(values["5. volume"]),
                }

                records.append(record)

            except Exception as e:
                logger.warning(f"Skipping bad record for {date_str}: {e}")
                continue

        # 🔄 Sort chronologically (important for downstream ML & analytics)
        records.sort(key=lambda x: x["timestamp"])

        logger.info(f"Normalized {len(records)} records for {ticker}")

        return records

    except Exception as e:
        logger.error(f"Normalization failed: {e}")
        raise


def normalize_multiple(raw_responses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Combine multiple API responses into a single list of records.
    Useful for multi-ticker ingestion.
    """
    all_records = []

    for response in raw_responses:
        try:
            records = normalize_daily_series(response)
            all_records.extend(records)
        except Exception as e:
            logger.error(f"Failed to process one response: {e}")

    return all_records

if __name__ == "__main__":
    import json

    with open("data/raw/AAPL_20260414_181133.json") as f:
        raw = json.load(f)

    records = normalize_daily_series(raw)

    for r in records[:5]:
        print(r)
    