"""
Pulls real-time quotes for a fixed list of tickers from Alpha Vantage
(via RapidAPI) and polls on an interval. 
"""

import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://alpha-vantage.p.rapidapi.com/query"
API_HOST = "alpha-vantage.p.rapidapi.com"
API_KEY = os.getenv("RAPIDAPI_KEY")

TICKERS = ["AAPL", "MSFT", "TSLA"]

# Request limit: 5/min
POLL_INTERVAL_SECONDS = 60


def get_quote(symbol: str) -> dict | None:
    """Fetch a single real-time quote. Returns None on failure or rate limit."""
    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": API_HOST,
        "Content-Type": "application/json",
    }
    querystring = {"function": "GLOBAL_QUOTE", "symbol": symbol, "datatype": "json"}

    try:
        response = requests.get(API_URL, headers=headers, params=querystring, timeout=10)
    except requests.exceptions.RequestException as e:
        print(f"[{symbol}] Request failed: {e}")
        return None

    if response.status_code != 200:
        print(f"[{symbol}] Bad status code: {response.status_code}")
        return None

    data = response.json()

 
    if "Note" in data or "Information" in data:
        print(f"[{symbol}] API limit/info message: {data.get('Note') or data.get('Information')}")
        return None

    quote = data.get("Global Quote")
    if not quote or "05. price" not in quote:
        print(f"[{symbol}] Unexpected response shape: {data}")
        return None

    return {
        "symbol": symbol,
        "price": float(quote["05. price"]),
        "volume": int(quote["06. volume"]),
        "latest_trading_day": quote["07. latest trading day"],
    }


def poll_once() -> list[dict]:
    """Fetch quotes for all tracked tickers once. Skips any that fail."""
    results = []
    for symbol in TICKERS:
        quote = get_quote(symbol)
        if quote:
            results.append(quote)
            print(f"[{symbol}] price={quote['price']} volume={quote['volume']}")
    return results


def main():
    if not API_KEY:
        print("ERROR: RAPIDAPI_KEY not found in .env")
        return

    print(f"Polling {TICKERS} every {POLL_INTERVAL_SECONDS}s. Ctrl+C to stop.")
    while True:
        poll_once()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()