"""
Kafka Producer (Intraday)

Fetches real historical intraday data (5-minute intervals) for tickers from
Alpha Vantage and replays the time series step-by-step into the 'stock-quotes'
Kafka topic. This provides realistic price fluctuations and volume movements.
"""

import json
import os
import time

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://alpha-vantage.p.rapidapi.com/query"
API_HOST = "alpha-vantage.p.rapidapi.com"
API_KEY = os.getenv("RAPIDAPI_KEY")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "stock-quotes"

TICKERS = ["AAPL", "MSFT", "TSLA"]

# Replay speed: publish the next historical time step every N seconds
REPLAY_INTERVAL_SECONDS = 10


def fetch_intraday_series(symbol: str) -> list[dict]:
    """Fetch 5-minute intraday historical quotes for a symbol and sort chronologically."""
    headers = {
        "x-rapidapi-key": API_KEY,
        "x-rapidapi-host": API_HOST,
        "Content-Type": "application/json",
    }
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": "5min",
        "outputsize": "compact",
        "datatype": "json",
    }

    try:
        response = requests.get(API_URL, headers=headers, params=params, timeout=15)
        data = response.json()
    except Exception as e:
        print(f"[{symbol}] Failed to fetch intraday data: {e}")
        return []

    # Find the time series key (e.g. 'Time Series (5min)')
    ts_key = next((k for k in data.keys() if "Time Series" in k), None)
    if not ts_key:
        print(f"[{symbol}] Unexpected API response: {data.get('Note') or data.get('Information') or data}")
        return []

    raw_series = data[ts_key]
    
    # Sort timestamps chronologically (oldest to newest)
    sorted_times = sorted(raw_series.keys())
    
    records = []
    for ts in sorted_times:
        point = raw_series[ts]
        records.append({
            "symbol": symbol,
            "price": float(point["4. close"]),
            "volume": int(point["5. volume"]),
            "latest_trading_day": ts.split(" ")[0],
            "historical_time": ts,
        })

    print(f"[{symbol}] Loaded {len(records)} historical data points (from {sorted_times[0]} to {sorted_times[-1]}).")
    return records


def delivery_report(err, msg):
    """Callback for delivery confirmation."""
    if err is not None:
        print(f"Delivery failed for {msg.key()}: {err}")


def build_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})


def main():
    if not API_KEY:
        print("ERROR: RAPIDAPI_KEY not found in .env")
        return

    print("Fetching intraday historical datasets for tickers...")
    ticker_data = {}
    for symbol in TICKERS:
        series = fetch_intraday_series(symbol)
        if series:
            ticker_data[symbol] = series
        time.sleep(1)  # small pause between API calls to respect rate limits

    if not ticker_data:
        print("ERROR: Could not load any intraday data. Check your API key and rate limits.")
        return

    # Find common length across tickers
    min_length = min(len(s) for s in ticker_data.values())
    producer = build_producer()

    print(f"\nStarting replay of {min_length} time-steps to '{KAFKA_TOPIC}' every {REPLAY_INTERVAL_SECONDS}s. Ctrl+C to stop.\n")

    while True:
        for idx in range(min_length):
            current_iso_time = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

            for symbol, series in ticker_data.items():
                point = series[idx].copy()
                historical_time = point.pop("historical_time")
                point["fetched_at"] = current_iso_time

                producer.produce(
                    KAFKA_TOPIC,
                    key=symbol,
                    value=json.dumps(point).encode("utf-8"),
                    callback=delivery_report,
                )
                print(f"Replayed [{symbol}] price={point['price']:.2f} vol={point['volume']} (Hist: {historical_time})")

            producer.poll(0)
            producer.flush()
            time.sleep(REPLAY_INTERVAL_SECONDS)

        print("\n--- Completed full replay cycle. Looping again... ---\n")


if __name__ == "__main__":
    main()