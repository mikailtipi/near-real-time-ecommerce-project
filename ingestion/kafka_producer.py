"""
ingestion/kafka_producer.py
---------------------------
Produces simulated e-commerce order events to Kafka topic 'orders-raw'.
Run: python ingestion/kafka_producer.py
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta

from faker import Faker
from kafka import KafkaProducer

fake = Faker("pt_BR")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = "orders-raw"
INTERVAL_SEC    = float(os.getenv("PRODUCE_INTERVAL", "1.0"))

STATUSES   = ["delivered", "shipped", "processing", "cancelled", "returned"]
CATEGORIES = ["electronics", "clothing", "home_appliances", "books", "sports",
               "beauty", "toys", "furniture", "food", "automotive"]
STATES     = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO", "PE", "CE"]


def make_event():
    purchase_ts = datetime.now() - timedelta(minutes=random.randint(0, 60))
    status      = random.choice(STATUSES)
    return {
        "event_type":   "order_created",
        "order_id":     str(uuid.uuid4()),
        "customer_id":  str(uuid.uuid4()),
        "order_status": status,
        "product": {
            "product_id":    str(uuid.uuid4()),
            "category_name": random.choice(CATEGORIES),
            "price":         round(random.uniform(9.9, 999.9), 2),
        },
        "quantity":       random.randint(1, 5),
        "freight_value":  round(random.uniform(5, 50), 2),
        "customer": {
            "city":  fake.city(),
            "state": random.choice(STATES),
            "zip":   fake.postcode(),
        },
        "purchased_at":  purchase_ts.isoformat(),
        "ts":            datetime.now().isoformat(),
    }


def main():
    print(f"[producer] Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )
    print(f"[producer] Streaming to topic '{TOPIC}' every {INTERVAL_SEC}s — Ctrl+C to stop")

    sent = 0
    try:
        while True:
            event = make_event()
            producer.send(TOPIC, value=event, key=event["order_id"].encode())
            sent += 1
            print(f"[producer] #{sent} order_id={event['order_id'][:8]}... status={event['order_status']}")
            time.sleep(INTERVAL_SEC)
    except KeyboardInterrupt:
        print(f"\n[producer] Stopped. Sent {sent} events.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
