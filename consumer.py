"""
consumer.py

Reads prediction results from the 'predictions' Kafka topic
and prints each one to the console in real time.

Usage:
    python consumer.py
"""

import json
from confluent_kafka import Consumer

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id":          "fraud-output-consumer",
    "auto.offset.reset": "latest",  # only show new predictions
}

consumer = Consumer(KAFKA_CONFIG)
consumer.subscribe(["predictions"])

print("=" * 60)
print("  Fraud Detection — Live Predictions")
print("=" * 60)
print(f"  {'Label':<12} {'Probability':>12} {'Amount':>10}  Timestamp")
print("  " + "-" * 56)

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        result = json.loads(msg.value().decode("utf-8"))

        label       = result["label"]
        probability = result["probability"]
        amount      = result["amount"]
        timestamp   = result["timestamp"]

        # Highlight fraud in output
        flag = "🚨" if label == "FRAUD" else "  "
        print(f"{flag} {label:<12} {probability:>12.4f} ${amount:>9.2f}  {timestamp}")

except KeyboardInterrupt:
    print("\nStopping consumer.")
finally:
    consumer.close()
