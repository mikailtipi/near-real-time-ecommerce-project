cat > scripts/init_postgres.sql << 'EOF'
CREATE USER airflow WITH PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

\connect ecommerce

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id                      VARCHAR(50) PRIMARY KEY,
    customer_id                   VARCHAR(50),
    order_status                  VARCHAR(30),
    order_purchase_ts             TIMESTAMP,
    order_approved_ts             TIMESTAMP,
    order_delivered_ts            TIMESTAMP,
    order_estimated_delivery_date DATE,
    created_at                    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id    VARCHAR(50) PRIMARY KEY,
    customer_city  VARCHAR(100),
    customer_state VARCHAR(50),
    customer_zip   VARCHAR(20),
    created_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id       VARCHAR(50) PRIMARY KEY,
    category_name    VARCHAR(100),
    product_weight_g NUMERIC,
    product_price    NUMERIC,
    created_at       TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_id      VARCHAR(50),
    product_id    VARCHAR(50),
    seller_id     VARCHAR(50),
    quantity      INT,
    price         NUMERIC,
    freight_value NUMERIC,
    created_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.dq_results (
    run_id     SERIAL PRIMARY KEY,
    table_name VARCHAR(100),
    check_name VARCHAR(100),
    status     VARCHAR(10),
    detail     TEXT,
    row_count  INT,
    fail_count INT,
    run_at     TIMESTAMP DEFAULT NOW()
);

GRANT ALL ON SCHEMA raw TO pipeline;
GRANT ALL ON SCHEMA staging TO pipeline;
GRANT ALL ON SCHEMA marts TO pipeline;
GRANT ALL ON ALL TABLES IN SCHEMA raw TO pipeline;
GRANT ALL ON ALL SEQUENCES IN SCHEMA raw TO pipeline;
EOF