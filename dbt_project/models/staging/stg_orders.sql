-- dbt_project/models/staging/stg_orders.sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),
cleaned AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        order_purchase_ts                                           AS purchased_at,
        order_approved_ts                                           AS approved_at,
        order_delivered_ts                                          AS delivered_at,
        order_estimated_delivery_date                               AS estimated_delivery_date,
        CASE
            WHEN order_delivered_ts IS NOT NULL
            THEN EXTRACT(EPOCH FROM (order_delivered_ts - order_purchase_ts)) / 86400
        END                                                         AS delivery_days,
        CASE
            WHEN order_delivered_ts > order_estimated_delivery_date
            THEN TRUE ELSE FALSE
        END                                                         AS is_late,
        created_at
    FROM source
    WHERE order_id IS NOT NULL
      AND customer_id IS NOT NULL
)
SELECT * FROM cleaned
