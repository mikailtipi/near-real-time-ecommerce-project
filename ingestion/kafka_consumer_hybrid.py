"""
ingestion/kafka_consumer_hybrid.py
------------------------------------
Hybrid Kafka consumer:
  - 'orders-raw' topic'ini dinler
  - Weather + country enrichment iceren eventleri PostgreSQL'e yazar
  - raw.orders, raw.customers, raw.products, raw.order_items tablolarini gunceller

Kullanim:
  DB_HOST=localhost DB_PORT=5433 ... python ingestion/kafka_consumer_hybrid.py
"""

import json
import os

import psycopg2
from psycopg2.extras import execute_batch
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC           = "orders-raw"
GROUP_ID        = "pipeline-hybrid-consumer"

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5433)),
    "dbname":   os.getenv("DB_NAME", "ecommerce"),
    "user":     os.getenv("DB_USER", "pipeline"),
    "password": os.getenv("DB_PASSWORD", "pipeline"),
}


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def ensure_weather_columns(cur):
    """Weather kolonlarini ekle - yoksa."""
    alterations = [
        "ALTER TABLE raw.orders ADD COLUMN IF NOT EXISTS weather_condition VARCHAR(50)",
        "ALTER TABLE raw.orders ADD COLUMN IF NOT EXISTS weather_desc     VARCHAR(100)",
        "ALTER TABLE raw.orders ADD COLUMN IF NOT EXISTS temperature_c    NUMERIC",
        "ALTER TABLE raw.orders ADD COLUMN IF NOT EXISTS humidity         INT",
        "ALTER TABLE raw.orders ADD COLUMN IF NOT EXISTS wind_speed       NUMERIC",
        "ALTER TABLE raw.customers ADD COLUMN IF NOT EXISTS country_code  VARCHAR(5)",
        "ALTER TABLE raw.customers ADD COLUMN IF NOT EXISTS country_name  VARCHAR(100)",
        "ALTER TABLE raw.customers ADD COLUMN IF NOT EXISTS currency      VARCHAR(10)",
        "ALTER TABLE raw.customers ADD COLUMN IF NOT EXISTS population    BIGINT",
        "ALTER TABLE raw.customers ADD COLUMN IF NOT EXISTS region        VARCHAR(100)",
        "ALTER TABLE raw.customers ADD COLUMN IF NOT EXISTS lat           NUMERIC",
        "ALTER TABLE raw.customers ADD COLUMN IF NOT EXISTS lon           NUMERIC",
    ]
    for sql in alterations:
        cur.execute(sql)


def upsert_event(cur, event: dict):
    customer = event.get("customer", {})
    product  = event.get("product", {})
    weather  = event.get("weather", {})

    cur.execute("""
        INSERT INTO raw.customers
          (customer_id, customer_city, customer_state, customer_zip,
           country_code, country_name, currency, population, region, lat, lon)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (customer_id) DO NOTHING
    """, (
        event["customer_id"],
        customer.get("city", ""),
        customer.get("country_code", ""),
        "",
        customer.get("country_code", ""),
        customer.get("country_name", ""),
        customer.get("currency", ""),
        customer.get("population", 0),
        customer.get("region", ""),
        customer.get("lat", 0),
        customer.get("lon", 0),
    ))

    cur.execute("""
        INSERT INTO raw.products (product_id, category_name, product_price)
        VALUES (%s, %s, %s)
        ON CONFLICT (product_id) DO NOTHING
    """, (
        product.get("product_id"),
        product.get("category_name"),
        product.get("price"),
    ))

    cur.execute("""
        INSERT INTO raw.orders
          (order_id, customer_id, order_status, order_purchase_ts,
           weather_condition, weather_desc, temperature_c, humidity, wind_speed)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING
    """, (
        event["order_id"],
        event["customer_id"],
        event["order_status"],
        event["purchased_at"],
        weather.get("weather_condition", ""),
        weather.get("weather_desc", ""),
        weather.get("temperature_c", 0),
        weather.get("humidity", 0),
        weather.get("wind_speed", 0),
    ))

    cur.execute("""
        INSERT INTO raw.order_items
          (order_id, product_id, seller_id, quantity, price, freight_value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        event["order_id"],
        product.get("product_id"),
        "kafka-hybrid",
        event.get("quantity", 1),
        product.get("price", 0),
        event.get("freight_value", 0),
    ))


def main():
    print(f"[consumer] Connecting to Kafka at {KAFKA_BOOTSTRAP}...")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        enable_auto_commit=True,
    )

    conn = get_conn()
    cur  = conn.cursor()

    print("[consumer] Ensuring weather columns exist...")
    ensure_weather_columns(cur)
    conn.commit()

    print(f"[consumer] Listening on '{TOPIC}' — Ctrl+C to stop\n")

    processed = 0
    try:
        for msg in consumer:
            event = msg.value
            upsert_event(cur, event)
            conn.commit()
            processed += 1
            city    = event.get("customer", {}).get("city", "?")
            weather = event.get("weather", {}).get("weather_condition", "?")
            temp    = event.get("weather", {}).get("temperature_c", "?")
            print(
                f"[consumer] #{processed:04d} | {city:<12} | "
                f"{weather:<8} {temp}°C | order_id={event['order_id'][:8]}..."
            )
    except KeyboardInterrupt:
        print(f"\n[consumer] Stopped. Processed {processed} events.")
    finally:
        cur.close()
        conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
