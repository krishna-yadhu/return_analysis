/* @bruin
name: mart.returns_by_category
type: bq.sql
connection: bigquery_main
depends:
  - staging.stg_order_items
materialization:
  type: table
@bruin */

SELECT
    product_category,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS total_returns,
    ROUND(
        SUM(CASE WHEN is_returned THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2
    ) AS return_rate_pct,
    ROUND(SUM(
        CASE WHEN is_returned THEN sale_price ELSE 0 END
    ), 2) AS revenue_lost,
    ROUND(AVG(sale_price), 2) AS avg_sale_price
FROM `return-analysis-490800.staging.stg_order_items`
GROUP BY product_category
ORDER BY return_rate_pct DESC,total_returns DESC