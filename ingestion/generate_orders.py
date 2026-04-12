"""
ingestion/generate_orders.py
----------------------------
Simulates near-real-time e-commerce order ingestion.
Run modes:
  python generate_orders.py --mode seed      # one-time bulk load (historical data)
  python generate_orders.py --mode stream    # continuous streaming (1 order/sec)
  python generate_orders.py --mode batch     # daily batch (called by Airflow DAG)
"""

import argparse
import os
import random
import time
import uuid
from datetime import datetime, timedelta

import psycopg2
from faker import Faker
from psycopg2.extras import execute_batch

fake = Faker("pt_BR")  # Brazilian locale matches Olist dataset domain

DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "ecommerce"),
    "user":     os.getenv("DB_USER", "pipeline"),
    "password": os.getenv("DB_PASSWORD", "pipeline"),
}

STATUSES   = ["delivered", "shipped", "processing", "cancelled", "returned"]
CATEGORIES = [
    "electronics", "clothing", "home_appliances", "books",
    "sports", "beauty", "toys", "furniture", "food", "automotive",
]
STATES = ["SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO", "PE", "CE"]


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def make_customer():
    return (
        str(uuid.uuid4()),
        fake.city(),
        random.choice(STATES),
        fake.postcode(),
    )


def make_product():
    return (
        str(uuid.uuid4()),
        random.choice(CATEGORIES),
        round(random.uniform(100, 5000), 2),
        round(random.uniform(9.9, 999.9), 2),
    )


def make_order(customer_id: str, days_ago: int = 0):
    purchase_ts  = datetime.now() - timedelta(days=days_ago, hours=random.randint(0, 23))
    approved_ts  = purchase_ts + timedelta(minutes=random.randint(5, 60))
    status       = random.choice(STATUSES)
    delivered_ts = (
        approved_ts + timedelta(days=random.randint(3, 20))
        if status == "delivered"
        else None
    )
    estimated_delivery = (purchase_ts + timedelta(days=random.randint(7, 30))).date()

    return (
        str(uuid.uuid4()),
        customer_id,
        status,
        purchase_ts,
        approved_ts,
        delivered_ts,
        estimated_delivery,
    )


def make_order_item(order_id: str, product_id: str):
    qty = random.randint(1, 5)
    price = round(random.uniform(9.9, 999.9), 2)
    return (
        order_id,
        product_id,
        str(uuid.uuid4()),  # seller_id
        qty,
        price,
        round(price * 0.1, 2),  # freight ~10%
    )


def seed(n_customers=500, n_products=200, n_orders=2000):
    """Bulk-load historical data for initial pipeline run."""
    print(f"[seed] Generating {n_customers} customers, {n_products} products, {n_orders} orders...")
    conn = get_conn()
    cur  = conn.cursor()

    customers = [make_customer() for _ in range(n_customers)]
    execute_batch(cur, """
        INSERT INTO raw.customers (customer_id, customer_city, customer_state, customer_zip)
        VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
    """, customers)

    products = [make_product() for _ in range(n_products)]
    execute_batch(cur, """
        INSERT INTO raw.products (product_id, category_name, product_weight_g, product_price)
        VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING
    """, products)

    customer_ids = [c[0] for c in customers]
    product_ids  = [p[0] for p in products]

    orders     = []
    order_items = []
    for i in range(n_orders):
        cid   = random.choice(customer_ids)
        order = make_order(cid, days_ago=random.randint(0, 90))
        orders.append(order)
        n_items = random.randint(1, 4)
        for _ in range(n_items):
            order_items.append(make_order_item(order[0], random.choice(product_ids)))

    execute_batch(cur, """
        INSERT INTO raw.orders
          (order_id, customer_id, order_status, order_purchase_ts,
           order_approved_ts, order_delivered_ts, order_estimated_delivery_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
    """, orders)

    execute_batch(cur, """
        INSERT INTO raw.order_items
          (order_id, product_id, seller_id, quantity, price, freight_value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, order_items)

    conn.commit()
    cur.close()
    conn.close()
    print(f"[seed] Done. {len(orders)} orders, {len(order_items)} items loaded.")


def stream(interval_sec=1.0):
    """Emit one new order per interval — simulates a live feed."""
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("SELECT customer_id FROM raw.customers ORDER BY RANDOM() LIMIT 200")
    customer_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id FROM raw.products ORDER BY RANDOM() LIMIT 200")
    product_ids = [r[0] for r in cur.fetchall()]

    if not customer_ids or not product_ids:
        print("[stream] No customers/products found. Run --mode seed first.")
        return

    print(f"[stream] Streaming orders every {interval_sec}s — Ctrl+C to stop")
    try:
        while True:
            order = make_order(random.choice(customer_ids))
            cur.execute("""
                INSERT INTO raw.orders
                  (order_id, customer_id, order_status, order_purchase_ts,
                   order_approved_ts, order_delivered_ts, order_estimated_delivery_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
            """, order)
            n_items = random.randint(1, 3)
            for _ in range(n_items):
                item = make_order_item(order[0], random.choice(product_ids))
                cur.execute("""
                    INSERT INTO raw.order_items
                      (order_id, product_id, seller_id, quantity, price, freight_value)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, item)
            conn.commit()
            print(f"[stream] order_id={order[0]} status={order[2]} ts={order[3]:%H:%M:%S}")
            time.sleep(interval_sec)
    except KeyboardInterrupt:
        print("\n[stream] Stopped.")
    finally:
        cur.close()
        conn.close()


def batch(hours_back=24):
    """Insert orders from the last N hours — called by Airflow daily DAG."""
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("SELECT customer_id FROM raw.customers ORDER BY RANDOM() LIMIT 100")
    customer_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id FROM raw.products ORDER BY RANDOM() LIMIT 100")
    product_ids = [r[0] for r in cur.fetchall()]

    n = random.randint(80, 150)
    orders      = []
    order_items = []
    for _ in range(n):
        order = make_order(
            random.choice(customer_ids),
            days_ago=random.uniform(0, hours_back / 24),
        )
        orders.append(order)
        for _ in range(random.randint(1, 3)):
            order_items.append(make_order_item(order[0], random.choice(product_ids)))

    execute_batch(cur, """
        INSERT INTO raw.orders
          (order_id, customer_id, order_status, order_purchase_ts,
           order_approved_ts, order_delivered_ts, order_estimated_delivery_date)
        VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
    """, orders)
    execute_batch(cur, """
        INSERT INTO raw.order_items
          (order_id, product_id, seller_id, quantity, price, freight_value)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, order_items)

    conn.commit()
    cur.close()
    conn.close()
    print(f"[batch] Inserted {len(orders)} orders, {len(order_items)} items.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-commerce order data generator")
    parser.add_argument(
        "--mode",
        choices=["seed", "stream", "batch"],
        default="seed",
        help="seed=bulk load | stream=live feed | batch=daily insert",
    )
    parser.add_argument("--interval", type=float, default=1.0, help="stream interval in seconds")
    args = parser.parse_args()

    if args.mode == "seed":
        seed()
    elif args.mode == "stream":
        stream(interval_sec=args.interval)
    elif args.mode == "batch":
        batch()
