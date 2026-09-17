"""
Phase 2: Throwaway test consumer.

Prints every message it sees on "stock-quotes" so we can visually
confirm the producer is publishing correctly.
"""

import json

from confluent_kafka import Consumer

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "stock-quotes"


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "test-consumer-group",
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([KAFKA_TOPIC])

    print(f"Listening on topic '{KAFKA_TOPIC}'... Ctrl+C to stop.")
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            value = json.loads(msg.value().decode("utf-8"))
            print(f"Received: {value}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


if __name__ == "__main__":
    main()