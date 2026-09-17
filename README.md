# 📈 MarketPulse Analytics — Real-Time Stock Market Pipeline

[![Python](https://img.shields.io/badge/Python-3.11%20%7C%203.13-blue.svg)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.6.1-black.svg)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5%20%7C%204.1-E25A1C.svg)](https://spark.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791.svg)](https://www.postgresql.org/)
[![Metabase](https://img.shields.io/badge/Metabase-Latest-509EE3.svg)](https://www.metabase.com/)
[![Docker](https://img.shields.io/badge/Docker%20Compose-Ready-2496ED.svg)](https://www.docker.com/)

**MarketPulse Analytics** is an end-to-end real-time data engineering pipeline that pulls stock market quotes, streams them through Apache Kafka, computes rolling window metrics using Apache Spark Structured Streaming, persists raw and aggregated data in PostgreSQL, and serves live, auto-refreshing dashboards in Metabase — with automated pipeline health monitoring.

---

## 🏗️ Architecture Overview

```
 ┌──────────────────────┐
 │  Alpha Vantage API   │  (Real-Time & Intraday Replay)
 └──────────┬───────────┘
            │ HTTP / JSON
            ▼
 ┌──────────────────────┐
 │   Python Producer    │  (ingestion/producer.py)
 └──────────┬───────────┘
            │ Publishes to 'stock-quotes'
            ▼
 ┌──────────────────────┐
 │     Apache Kafka     │  (Zookeeper-backed broker)
 └──────────┬───────────┘
            │ Consumes Stream (Kafka Source)
            ▼
 ┌──────────────────────┐
 │     Apache Spark     │  • JSON Schema Validation & Deserialization
 │ Structured Streaming │  • 5-min Sliding Window Rolling Average
 └────┬────────────┬────┘  • 2-min Late-Data Watermarking
      │            │
      │            |
      ▼            ▼
 ┌────────────────────────────────────────────────────────┐
 │                   PostgreSQL 16                        │
 │  • silver.raw_quotes (Individual quotes & volumes)     │
 │  • silver.processed_metrics (5-min rolling averages)   │
 └──────────┬─────────────────────────────────────────────┘
            │
      ┌─────┴─────────────────────┐
      ▼                           ▼
┌──────────────┐        ┌───────────────────────┐
│   Metabase   │        │     Health Check      │
│  Dashboards  │        │ (Pipeline Monitoring) │
└──────────────┘        └───────────────────────┘
```

---

## 🧰 Tech Stack

| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Data Ingestion** | Python, Requests, Confluent-Kafka | Pulls market quotes and publishes JSON events |
| **Message Broker** | Apache Kafka & Zookeeper | Scalable, decoupled real-time message stream |
| **Stream Processing** | Apache Spark (Structured Streaming, PySpark) | Micro-batch parsing, windowing, and rolling aggregations |
| **Data Storage** | PostgreSQL 16 | Relational persistence with Medallion `silver` schema |
| **Visualization** | Metabase | Live, auto-refreshing interactive dashboards |
| **Monitoring** | Python, Psycopg2, Kafka AdminClient | Data freshness and pipeline staleness health checks |
| **Containerization** | Docker & Docker Compose | Containerized, single-command cold start |

---

## 📂 Project Structure

```
marketpulse_analytics/
├── dashboards/              # Metabase configuration / export files
├── ingestion/               # Ingestion services
│   ├── Dockerfile           # Producer container definition
│   ├── producer.py          # Kafka producer (Intraday historical replay & live quotes)
│   ├── quotes.py            # Alpha Vantage API client wrapper
│   └── test_consumer.py     # Throwaway test consumer for verifying Kafka
├── monitoring/              # Observability & health checks
│   └── health_check.py      # Automated broker, DB & data freshness checker
├── storage/                 # Database schema & migrations
│   └── schema.sql           # PostgreSQL table DDL & index definitions
├── streaming/               # Spark streaming jobs
│   ├── Dockerfile           # Spark processor container definition
│   └── process_quotes.py    # Spark Structured Streaming job (JDBC sink)
├── docker-compose.yml       # Complete multi-service orchestration
├── .env.example             # Template for required environment variables
├── pyproject.toml           # Project metadata & Python configuration
└── README.md
```

---

## Quickstart Guide

### 1. Prerequisites
* [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
* Python 3.10+ (for local scripts/monitoring)
* Free Alpha Vantage API key (via [RapidAPI](https://rapidapi.com/alphavantage/api/alpha-vantage))

### 2. Environment Setup
Clone the repository and create your `.env` file:
```bash
cp .env.example .env
```
Edit `.env` and provide your `RAPIDAPI_KEY`:
```env
RAPIDAPI_KEY=your_actual_rapidapi_key_here
POSTGRES_USER=marketpulse
POSTGRES_PASSWORD=marketpulse
POSTGRES_DB=marketpulse
POSTGRES_PORT=5433
POSTGRES_HOST=localhost
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

### 3. Launch the Full Pipeline (Single Command)
Bring up all containers (Zookeeper, Kafka, Postgres, Metabase, Producer, and Spark Processor):
```bash
docker compose up -d --build
```

To monitor the real-time processing logs:
```bash
docker compose logs -f producer spark-processor
```

---

## 📊 Database Schema (`silver` schema)

The database uses a structured schema initialized on startup:

* **`silver.raw_quotes`**: Immutable record of every stock tick received.
  * Columns: `id`, `symbol`, `price`, `volume`, `latest_trading_day`, `fetched_at`, `created_at`
* **`silver.processed_metrics`**: Windowed aggregations computed by Spark.
  * Columns: `id`, `symbol`, `window_start`, `window_end`, `rolling_avg_price`, `created_at`

---

## 🩺 Pipeline Monitoring & Health Checks

A dedicated health monitor is located in `monitoring/health_check.py`. It inspects:
1. **Kafka Broker & Topic:** Checks cluster responsiveness and topic existence.
2. **PostgreSQL Connectivity:** Verifies database response times.
3. **Data Freshness:** Alerts if raw quotes or processed metrics have not updated within 180 seconds.

Run the health check at any time:
```bash
python monitoring/health_check.py
```

Sample output:
```text
=======================================================
      🔍 MarketPulse Analytics Pipeline Health Check
=======================================================
🟢 [OK] Kafka: Broker reachable, topic 'stock-quotes' exists
🟢 [OK] PostgreSQL: Connected to 'marketpulse' on port 5433
🟢 [OK] Raw Quotes: Fresh (last received 9s ago)
🟢 [OK] Spark Processing: Fresh (last metric 5s ago)
=======================================================
✅ Pipeline Status: ALL SYSTEMS HEALTHY
```

---

## 🖥️ Visualizations (Metabase)

1. Navigate to **`http://localhost:3000`** in your browser.
2. Connect to the PostgreSQL database (`Host: postgres`, `Port: 5432`, `Database: marketpulse`, `User/Password: marketpulse`).
3. Build questions and dashboards to track:
   * **Real-Time Price Tickers:** Latest price per symbol.
   * **Price vs Rolling Average:** Line chart of `price` alongside 5-min `rolling_avg_price`.
   * **Trading Volume Comparison:** Bar chart of volume by ticker.
   * **Auto-Refresh:** Set dashboard auto-refresh to 1 minute for live updates.

---

## 🛑 Reset

* **Stop all containers (retaining data):**
  ```bash
  docker compose down
  ```
* **Full reset (wiping DB & Metabase volumes):**
  ```bash
  docker compose down -v
  rm -rf /tmp/spark-checkpoints
  ```

---
