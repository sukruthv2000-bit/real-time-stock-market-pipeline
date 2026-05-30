import json
import time
from datetime import datetime, timezone

import yfinance as yf
from kafka import KafkaProducer


KAFKA_TOPIC = "stock-prices"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]


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
        time.sleep(60)


if __name__ == "__main__":
    main()
