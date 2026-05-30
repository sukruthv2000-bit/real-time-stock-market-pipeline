import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))
from src.utils.config_loader import load_config
import json
import time
from datetime import datetime, timezone

import yfinance as yf
from kafka import KafkaProducer


config = load_config()

KAFKA_TOPIC = config["kafka"]["topic"]
KAFKA_BOOTSTRAP_SERVER = config["kafka"]["bootstrap_server"]
TICKERS = config["stocks"]["tickers"]
POLL_INTERVAL_SECONDS = config["stocks"]["poll_interval_seconds"]

def create_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def fetch_stock_price(ticker):
    stock = yf.Ticker(ticker)
    data = stock.history(period="1d", interval="1m")

    if data.empty:
        return None

    latest = data.tail(1).iloc[0]

    return {
        "ticker": ticker,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "open": float(latest["Open"]),
        "high": float(latest["High"]),
        "low": float(latest["Low"]),
        "close": float(latest["Close"]),
        "volume": int(latest["Volume"]),
    }


def main():
    producer = create_producer()
    print("Stock producer started...")

    while True:
        for ticker in TICKERS:
            try:
                event = fetch_stock_price(ticker)

                if event:
                    producer.send(KAFKA_TOPIC, event)
                    print(f"Sent: {event}")
                else:
                    print(f"No data for {ticker}")

            except Exception as e:
                print(f"Error for {ticker}: {e}")

        producer.flush()
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
