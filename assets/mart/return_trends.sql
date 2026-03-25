/* @bruin
name: mart.return_trends
type: bq.sql
connection: bigquery_main
depends:
  - staging.stg_order_items
materialization:
  type: table
@bruin */

SELECT
    FORMAT_DATE('%Y-%m-%d', DATE(order_month)) AS month,
    COUNT(*) AS total_items,
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS returned_items,
    ROUND(
        SUM(CASE WHEN is_returned THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2
    )  AS return_percentage
FROM `return-analysis-490800.staging.stg_order_items`
GROUP BY month
ORDER BY month ASC