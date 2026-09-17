"""
Pipeline Health & Freshness Monitor

Checks Kafka connectivity and verifies that live data and processed metrics
in PostgreSQL are fresh (updated within the last N seconds).
"""

import os
import sys
from datetime import datetime, timezone

import psycopg2
from confluent_kafka.admin import AdminClient
from psycopg2.extras import RealDictCursor

# Threshold in seconds before declaring the pipeline stale
STALENESS_THRESHOLD_SECONDS = 180

# DB Connection settings
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5433")
DB_NAME = os.getenv("POSTGRES_DB", "marketpulse")
DB_USER = os.getenv("POSTGRES_USER", "marketpulse")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "marketpulse")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "stock-quotes"


def check_kafka() -> tuple[bool, str]:
    """Check if Kafka broker is reachable and topic exists."""
    try:
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP, "socket.timeout.ms": 3000})
        metadata = admin.list_topics(timeout=3.0)
        if KAFKA_TOPIC in metadata.topics:
            return True, f"Broker reachable, topic '{KAFKA_TOPIC}' exists"
        return False, f"Broker reachable, but topic '{KAFKA_TOPIC}' not found"
    except Exception as e:
        return False, f"Kafka connection failed: {e}"


def check_postgres_and_freshness() -> tuple[bool, dict]:
    """Check Postgres connectivity and calculate data freshness."""
    results = {
        "db_connected": False,
        "raw_quotes_fresh": False,
        "raw_age_seconds": None,
        "metrics_fresh": False,
        "metrics_age_seconds": None,
        "error": None,
    }

    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            connect_timeout=3,
        )
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        results["db_connected"] = True
        now = datetime.now(timezone.utc)

        # Check raw_quotes latest timestamp
        cursor.execute("SELECT MAX(fetched_at) AS latest_raw FROM silver.raw_quotes;")
        row = cursor.fetchone()
        if row and row["latest_raw"]:
            latest_raw = row["latest_raw"]
            age_raw = (now - latest_raw).total_seconds()
            results["raw_age_seconds"] = int(age_raw)
            results["raw_quotes_fresh"] = age_raw <= STALENESS_THRESHOLD_SECONDS

        # Check processed_metrics latest timestamp
        cursor.execute("SELECT MAX(created_at) AS latest_metric FROM silver.processed_metrics;")
        row = cursor.fetchone()
        if row and row["latest_metric"]:
            latest_metric = row["latest_metric"]
            age_metric = (now - latest_metric).total_seconds()
            results["metrics_age_seconds"] = int(age_metric)
            results["metrics_fresh"] = age_metric <= STALENESS_THRESHOLD_SECONDS

        cursor.close()
        conn.close()
        return True, results

    except Exception as e:
        results["error"] = str(e)
        return False, results


def main():
    print("\n" + "=" * 55)
    print("      🔍 MarketPulse Analytics Pipeline Health Check")
    print("=" * 55)

    all_healthy = True

    # 1. Kafka Check
    kafka_ok, kafka_msg = check_kafka()
    status_icon = "🟢 [OK]" if kafka_ok else "🔴 [FAIL]"
    print(f"{status_icon} Kafka: {kafka_msg}")
    if not kafka_ok:
        all_healthy = False

    # 2. Database & Freshness Checks
    db_ok, db_details = check_postgres_and_freshness()
    if not db_ok:
        print(f"🔴 [FAIL] PostgreSQL: Connection error ({db_details.get('error')})")
        all_healthy = False
    else:
        print(f"🟢 [OK] PostgreSQL: Connected to '{DB_NAME}' on port {DB_PORT}")

        # Raw quotes freshness
        if db_details["raw_quotes_fresh"]:
            print(f"🟢 [OK] Raw Quotes: Fresh (last received {db_details['raw_age_seconds']}s ago)")
        else:
            age_str = f"{db_details['raw_age_seconds']}s ago" if db_details['raw_age_seconds'] is not None else "No data"
            print(f"🔴 [WARN] Raw Quotes: STALE (last received {age_str}, threshold: {STALENESS_THRESHOLD_SECONDS}s)")
            all_healthy = False

        # Processed metrics freshness
        if db_details["metrics_fresh"]:
            print(f"🟢 [OK] Spark Processing: Fresh (last metric {db_details['metrics_age_seconds']}s ago)")
        else:
            age_str = f"{db_details['metrics_age_seconds']}s ago" if db_details['metrics_age_seconds'] is not None else "No data"
            print(f"🔴 [WARN] Spark Processing: STALE (last metric {age_str}, threshold: {STALENESS_THRESHOLD_SECONDS}s)")
            all_healthy = False

    print("=" * 55)
    if all_healthy:
        print("✅ Pipeline Status: ALL SYSTEMS HEALTHY")
        sys.exit(0)
    else:
        print("⚠️ Pipeline Status: DEGRADED / ATTENTION REQUIRED")
        sys.exit(1)


if __name__ == "__main__":
    main()