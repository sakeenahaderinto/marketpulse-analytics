"""
Phase 4: Spark Structured Streaming Job.

Reads live quotes from the "stock-quotes" Kafka topic, parses the JSON,
computes a rolling average price (5-min sliding window), and persists both
the raw quotes and processed metrics into PostgreSQL (silver schema).
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, from_json, lag, window
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType
from pyspark.sql.window import Window

# Environment-aware connection settings
# Default to host addresses (localhost:9092 for Kafka, localhost:5433 for Postgres)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "stock-quotes"

POSTGRES_URL = os.getenv("POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:5433/marketpulse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "marketpulse")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "marketpulse")
POSTGRES_DRIVER = "org.postgresql.Driver"

# Schema matching JSON produced by ingestion/producer.py
QUOTE_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", LongType()),
    StructField("latest_trading_day", StringType()),
    StructField("fetched_at", StringType()),
])


def write_raw_quotes(batch_df, batch_id):
    """Writes a micro-batch of raw stock quotes to silver.raw_quotes."""
    if batch_df.rdd.isEmpty():
        return

    to_save = batch_df.select(
        col("symbol"),
        col("price"),
        col("volume"),
        col("latest_trading_day").cast("date"),
        col("event_time").alias("fetched_at"),
    )

    (
        to_save.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "silver.raw_quotes")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .mode("append")
        .save()
    )
    print(f"--- [Batch {batch_id}] Successfully wrote {to_save.count()} raw quotes to silver.raw_quotes ---")


def write_processed_metrics(batch_df, batch_id):
    """Writes a micro-batch of 5-min rolling averages to silver.processed_metrics."""
    if batch_df.rdd.isEmpty():
        return

    (
        batch_df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", "silver.processed_metrics")
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", POSTGRES_DRIVER)
        .mode("append")
        .save()
    )
    print(f"--- [Batch {batch_id}] Successfully wrote {batch_df.count()} metrics to silver.processed_metrics ---")


def main():
    spark = (
        SparkSession.builder
        .appName("MarketPulseQuoteProcessor")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Ingest streaming stream from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # Parse JSON payload and prepare event time
    parsed = (
        raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), QUOTE_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", col("fetched_at").cast("timestamp"))
        .filter(col("symbol").isNotNull() & col("price").isNotNull())
    )

    # Compute 5-minute rolling average (sliding every 1 minute) per symbol
    rolling_avg = (
        parsed
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes", "1 minute"),
            col("symbol"),
        )
        .agg(avg("price").alias("rolling_avg_price"))
        .select(
            col("symbol"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("rolling_avg_price"),
        )
    )

    # Stream 1: Persist raw quotes to PostgreSQL
    raw_quotes_query = (
        parsed.writeStream
        .outputMode("append")
        .foreachBatch(write_raw_quotes)
        .option("checkpointLocation", "/tmp/spark-checkpoints/raw_quotes")
        .queryName("raw_quotes_to_db")
        .start()
    )

    # Stream 2: Persist rolling averages to PostgreSQL
    metrics_query = (
        rolling_avg.writeStream
        .outputMode("update")
        .foreachBatch(write_processed_metrics)
        .option("checkpointLocation", "/tmp/spark-checkpoints/processed_metrics")
        .queryName("metrics_to_db")
        .start()
    )

    # Await termination of streaming queries
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()