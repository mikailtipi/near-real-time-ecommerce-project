-- dbt_project/models/marts/mart_order_metrics.sql
-- Daily order KPIs: revenue, AOV, delivery performance, return rate
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),
items AS (
    SELECT
        oi.order_id,
        SUM(oi.price * oi.quantity)     AS gross_revenue,
        SUM(oi.freight_value)           AS total_freight,
        COUNT(DISTINCT oi.product_id)   AS distinct_products
    FROM {{ source('raw', 'order_items') }} oi
    GROUP BY oi.order_id
),
daily AS (
    SELECT
        DATE_TRUNC('day', o.purchased_at)           AS order_date,
        COUNT(DISTINCT o.order_id)                  AS total_orders,
        COUNT(DISTINCT o.customer_id)               AS unique_customers,
        ROUND(SUM(i.gross_revenue)::NUMERIC, 2)     AS gross_revenue,
        ROUND(AVG(i.gross_revenue)::NUMERIC, 2)     AS avg_order_value,
        ROUND(AVG(o.delivery_days)::NUMERIC, 1)     AS avg_delivery_days,
        SUM(CASE WHEN o.is_late THEN 1 ELSE 0 END)  AS late_orders,
        SUM(CASE WHEN o.order_status = 'returned'
                 THEN 1 ELSE 0 END)                 AS returned_orders,
        ROUND(
            100.0 * SUM(CASE WHEN o.order_status = 'returned' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0), 2
        )                                           AS return_rate_pct
    FROM orders o
    LEFT JOIN items i ON i.order_id = o.order_id
    GROUP BY 1
)
SELECT * FROM daily
ORDER BY order_date DESC
