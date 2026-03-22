/* @bruin
name: staging.stg_order_items
type: bq.sql
connection: bigquery_main
depends:
    - raw.order_items
    - raw.orders
    - raw.products
    - raw.users
materialization:
    type: table
@bruin */

SELECT
    oi.id AS order_item_id,
    oi.order_id,
    oi.inventory_item_id,
    oi.status AS order_item_status,

    CASE
        WHEN oi.status = 'Returned' THEN true
        ELSE FALSE
    END AS is_returned,
    CASE
        WHEN oi.status = 'Cancelled' THEN true
        ELSE false
    END AS is_cancelled,

    oi.product_id,
    p.name AS product_name,
    p.category,
    p.brand,
    ROUND(CAST(p.retail_price AS FLOAT64), 2) AS retail_price,
    ROUND(CAST(oi.sale_price AS FLOAT64), 2) AS sale_price,

    oi.user_id,
    u.first_name,
    u.last_name,
    u.city,
    u.country,
    u.gender,

    CAST(o.created_at AS TIMESTAMP) AS order_date,
    CAST(o.returned_at AS TIMESTAMP) AS returned_date,
    DATE_TRUNC(CAST(o.created_at AS TIMESTAMP), MONTH) AS order_month

FROM `return-analysis-490800.raw.order_items` oi
LEFT JOIN `return-analysis-490800.raw.orders` o
    ON oi.order_id = o.order_id
LEFT JOIN `return-analysis-490800.raw.products` p
    ON oi.product_id = p.id
LEFT JOIN `return-analysis-490800.raw.users` u
    ON oi.user_id = u.id
WHERE oi.id IS NOT NULL