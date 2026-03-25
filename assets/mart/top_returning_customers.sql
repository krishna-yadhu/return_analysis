/* @bruin
name: mart.top_returning_customers
type: bq.sql
connection: bigquery_main
depends:
  - staging.stg_order_items
materialization:
  type: table
@bruin */

SELECT
    user_id,
    first_name,
    last_name,
    gender,
    COUNT(*)                                            AS total_orders,
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END)       AS total_returns,
    ROUND(
        SUM(CASE WHEN is_returned THEN 1 ELSE 0 END)
        * 100.0 / COUNT(*), 2
    )                                                   AS return_rate_pct,
    ROUND(SUM(
        CASE WHEN is_returned THEN sale_price ELSE 0 END
    ), 2)                                               AS revenue_lost
FROM `return-analysis-490800.staging.stg_order_items`
GROUP BY user_id, first_name, last_name,gender
HAVING total_returns > 0
ORDER BY return_rate_pct DESC, revenue_lost DESC