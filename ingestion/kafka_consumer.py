"""
ingestion/kafka_consumer.py
---------------------------
Consumes order events from Kafka topic 'orders-raw'
and writes them to PostgreSQL raw schema.
Run: python ingestion/kafka_consumer.py
"""

import json
import os

import psycopg2
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = "orders-raw"
GROUP_ID        = "pipeline-consumer"

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5433)),
    "dbname":   os.getenv("DB_NAME", "ecommerce"),
    "user":     os.getenv("DB_USER", "pipeline"),
    "password": os.getenv("DB_PASSWORD", "pipeline"),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def upsert_event(cur, event: dict):
    cur.execute("""
        INSERT INTO raw.customers (customer_id, customer_city, customer_state, customer_zip)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING
    """, (
        event["customer_id"],
        event["customer"]["city"],
        event["customer"]["state"],
        event["customer"]["zip"],
    ))

    cur.execute("""
        INSERT INTO raw.products (product_id, category_name, product_price)
        VALUES (%s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING
    """, (
        event["product"]["product_id"],
        event["product"]["category_name"],
        event["product"]["price"],
    ))

    cur.execute("""
        INSERT INTO raw.orders
          (order_id, customer_id, order_status, order_purchase_ts)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING
    """, (
        event["order_id"],
        event["customer_id"],
        event["order_status"],
        event["purchased_at"],
    ))

    cur.execute("""
        INSERT INTO raw.order_items
          (order_id, product_id, seller_id, quantity, price, freight_value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        event["order_id"],
        event["product"]["product_id"],
        "kafka-seller",
        event["quantity"],
        event["product"]["price"],
        event["freight_value"],
    ))


def main():
    print(f"[consumer] Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
    )

    conn = get_conn()
    cur  = conn.cursor()
    print(f"[consumer] Listening on topic '{TOPIC}' — Ctrl+C to stop")

    processed = 0
    try:
        for msg in consumer:
            event = msg.value
            upsert_event(cur, event)
            conn.commit()
            processed += 1
            print(f"[consumer] #{processed} order_id={event['order_id'][:8]}... written to DB")
    except KeyboardInterrupt:
        print(f"\n[consumer] Stopped. Processed {processed} events.")
    finally:
        cur.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
