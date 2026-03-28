-- ============================================================
-- Buyer Engagement Metrics
-- Compatible: Redshift, BigQuery, Snowflake
-- Description: Segments buyers by engagement level and purchase
--              behavior. Identifies power buyers, at-risk churners,
--              and dormant users for targeted intervention.
-- ============================================================

WITH buyer_activity AS (
    SELECT
        user_id,
        MIN(created_at)                             AS first_purchase_date,
        MAX(created_at)                             AS last_purchase_date,
        COUNT(DISTINCT order_id)                    AS total_orders,
        COUNT(DISTINCT DATE_TRUNC('month', created_at)) AS active_months,
        SUM(gmv)                                    AS total_gmv,
        AVG(gmv)                                    AS avg_order_value,
        COUNT(DISTINCT seller_id)                   AS unique_sellers_purchased_from,
        COUNT(DISTINCT category)                    AS unique_categories
    FROM orders
    WHERE status = 'completed'
    GROUP BY user_id
),

buyer_recency AS (
    SELECT
        user_id,
        DATEDIFF('day', last_purchase_date, CURRENT_DATE) AS days_since_last_purchase,
        DATEDIFF('day', first_purchase_date, last_purchase_date) AS buyer_lifespan_days
    FROM buyer_activity
)

SELECT
    ba.user_id,
    ba.first_purchase_date,
    ba.last_purchase_date,
    br.days_since_last_purchase,
    ba.total_orders,
    ba.active_months,
    ROUND(ba.total_gmv, 2)                          AS total_gmv,
    ROUND(ba.avg_order_value, 2)                    AS avg_order_value,
    ba.unique_sellers_purchased_from,
    ba.unique_categories,
    CASE
        WHEN br.days_since_last_purchase <= 30
             AND ba.total_orders >= 5               THEN 'Power Buyer'
        WHEN br.days_since_last_purchase <= 30      THEN 'Active Buyer'
        WHEN br.days_since_last_purchase BETWEEN 31 AND 60 THEN 'At Risk'
        WHEN br.days_since_last_purchase BETWEEN 61 AND 90 THEN 'Lapsing'
        ELSE 'Churned'
    END                                             AS engagement_segment,
    CASE
        WHEN ba.total_orders >= 10                  THEN 'Frequent'
        WHEN ba.total_orders BETWEEN 3 AND 9        THEN 'Occasional'
        ELSE 'One-Time'
    END                                             AS purchase_frequency_segment
FROM buyer_activity ba
JOIN buyer_recency br USING (user_id)
ORDER BY ba.total_gmv DESC
;
