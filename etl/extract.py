import os
import time
import requests
from typing import Dict, Any
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ALPHA_VANTAGE_KEY")
BASE_URL = "https://www.alphavantage.co/query"


class AlphaVantageClient:
    def __init__(self, api_key: str = API_KEY, rate_limit_per_min: int = 5):
        self.api_key = api_key
        self.rate_limit_per_min = rate_limit_per_min
        self.request_interval = 60 / rate_limit_per_min

    def _rate_limit(self):
        """Simple sleep-based rate limiter"""
        time.sleep(self.request_interval)

    def _make_request(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Handles API request + basic error handling"""
        try:
            response = requests.get(BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()

            # 🔍 Detect API errors (Alpha Vantage specific)
            if "Error Message" in data:
                raise Exception(f"API Error: {data['Error Message']}")
            if "Note" in data:
                raise Exception(f"Rate limit hit: {data['Note']}")

            return data

        except requests.exceptions.RequestException as e:
            raise Exception(f"HTTP Request failed: {e}")

    def get_intraday(self, symbol: str, interval: str = "5min") -> Dict[str, Any]:
        """
        Fetch daily OHLCV data from Alpha Vantage (TIME_SERIES_DAILY).
        The `interval` parameter is accepted for API compatibility but is
        not used by the DAILY endpoint (it applies to intraday functions).
        Docs: https://www.alphavantage.co/documentation/
        """
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "apikey": self.api_key,
            "outputsize": "compact"  # use 'full' for backfill
        }

        self._rate_limit()
        data = self._make_request(params)

        return data


def save_raw_json(data: Dict[str, Any], symbol: str, base_path: str = "data/raw"):
    """Save raw API response for debugging and replay"""
    os.makedirs(base_path, exist_ok=True)

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"{symbol}_{timestamp}.json"
    filepath = os.path.join(base_path, filename)

    with open(filepath, "w") as f:
        import json
        json.dump(data, f, indent=2)

    return filepath


if __name__ == "__main__":
    client = AlphaVantageClient()

    symbol = "AAPL"  # change this

    data = client.get_intraday(symbol=symbol)

    path = save_raw_json(data, symbol)

    print(f"Saved raw data to: {path}")