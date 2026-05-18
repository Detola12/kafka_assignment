"""
producer.py

Reads rows from creditcard.csv and publishes each one as a JSON message
to the Kafka 'raw-data' topic at ~1 row/second.

Usage:
    python producer.py
"""

import json
import time
import pandas as pd
from datetime import datetime, timezone
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVER = "localhost:9092"

KAFKA_CONFIG = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
}

# ── Create topics if they don't exist ─────────────────────────────────────────
def create_topic(name, partitions=1, replication=1):
    admin = AdminClient(KAFKA_CONFIG)
    result = admin.create_topics([
        NewTopic(name, num_partitions=partitions, replication_factor=replication)
    ])
    for t, future in result.items():
        try:
            future.result()
            print(f"Topic '{t}' created")
        except Exception as e:
            if "already exists" in str(e):
                print(f"Topic '{t}' already exists — skipping")
            else:
                raise

# ── Delivery callback ─────────────────────────────────────────────────────────
def on_delivery(err, msg):
    if err:
        print(f"  Delivery failed: {err}")
    else:
        print(f"  Delivered → partition {msg.partition()} | offset {msg.offset()}")

# ── Main ──────────────────────────────────────────────────────────────────────
create_topic("raw-data")
create_topic("predictions")

producer = Producer(KAFKA_CONFIG)
df = pd.read_csv("creditcard.csv")

print(f"\nStarting producer — {len(df)} rows to stream...\n")

for _, row in df.iterrows():
    message = row.to_dict()
    message["timestamp"] = datetime.now(timezone.utc).isoformat()

    producer.produce(
        topic    = "raw-data",
        key      = str(row.name),
        value    = json.dumps(message),
        callback = on_delivery
    )
    producer.poll(0)

    label = "FRAUD" if int(row["Class"]) == 1 else "LEGIT"
    print(f"[Row {row.name:>6}] {label} | Amount: ${row['Amount']:>8.2f} | {message['timestamp']}")


producer.flush()
print("\nProducer done.")
