# Real-Time Stock Market Data Pipeline Architecture

```text
                 +------------------+
                 | Yahoo Finance API|
                 +---------+--------+
                           |
                           v
                 +------------------+
                 | Python Producer  |
                 | (yfinance)       |
                 +---------+--------+
                           |
                           v
                 +------------------+
                 | Apache Kafka     |
                 | stock-prices     |
                 +---------+--------+
                           |
                           v
                 +----------------------+
                 | Spark Structured     |
                 | Streaming            |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 | Bronze Layer         |
                 | Raw Parquet Files    |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 | Silver Layer         |
                 | Metrics & KPIs       |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 | Gold Layer           |
                 | Business Aggregates  |
                 +----------+-----------+
                            |
                            v
                 +----------------------+
                 | Streamlit Dashboard  |
                 +----------------------+
```
