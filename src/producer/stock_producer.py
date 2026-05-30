import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT))

from src.utils.config_loader import load_config
from src.utils.logger import get_logger

import json
import time
from datetime import datetime, timezone

import yfinance as yf
from kafka import KafkaProducer


logger = get_logger(__name__)
config = load_config()

KAFKA_TOPIC = config["kafka"]["topic"]
KAFKA_BOOTSTRAP_SERVER = config["kafka"]["bootstrap_server"]
TICKERS = config["stocks"]["tickers"]
POLL_INTERVAL_SECONDS = config["stocks"]["poll_interval_seconds"]


def create_producer():
    logger.info("Creating Kafka producer")

    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def fetch_stock_price(ticker):
    stock = yf.Ticker(ticker)
    data = stock.history(period="1d", interval="1m")

    if data.empty:
        logger.warning(f"No stock data returned for {ticker}")
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
    logger.info("Stock producer started")

    while True:
        for ticker in TICKERS:
            try:
                event = fetch_stock_price(ticker)

                if event:
                    producer.send(KAFKA_TOPIC, event)
                    logger.info(f"Sent stock event: {event}")
                else:
                    logger.warning(f"No data for ticker: {ticker}")

            except Exception:
                logger.exception(f"Error while processing ticker: {ticker}")

        producer.flush()
        logger.info("Kafka producer flushed messages")

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()