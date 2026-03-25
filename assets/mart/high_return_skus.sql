/* @bruin
name: mart.high_return_skus
type: bq.sql
connection: bigquery_main
depends:
  - staging.stg_order_items
materialization:
  type: table
@bruin */

SELECT
    product_id,
    product_name,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS total_returns,
    ROUND(
        SUM(CASE WHEN is_returned THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2
    )  AS return_rate_pct,
    ROUND(SUM(
        CASE WHEN is_returned THEN sale_price ELSE 0 END
    ), 2)  AS revenue_lost
FROM `return-analysis-490800.staging.stg_order_items`
GROUP BY product_id, product_name
HAVING COUNT(*) >= 10
ORDER BY return_rate_pct DESC