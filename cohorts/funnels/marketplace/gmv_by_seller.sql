-- ============================================================
-- GMV and Seller Performance Metrics
-- Compatible: Redshift, BigQuery, Snowflake
-- Description: Ranks sellers by GMV, order volume, and buyer reach.
--              Includes MoM growth and identifies top/at-risk sellers.
-- ============================================================

WITH monthly_seller_metrics AS (
    SELECT
        o.seller_id,
        DATE_TRUNC('month', o.created_at)       AS month,
        COUNT(DISTINCT o.order_id)              AS total_orders,
        COUNT(DISTINCT o.buyer_id)              AS unique_buyers,
        SUM(o.gmv)                              AS total_gmv,
        AVG(o.gmv)                              AS avg_order_value,
        SUM(o.platform_fee)                     AS platform_revenue,
        AVG(o.seller_rating)                    AS avg_rating
    FROM orders o
    WHERE o.status = 'completed'
    AND o.created_at >= DATEADD('month', -6, DATE_TRUNC('month', CURRENT_DATE))
    GROUP BY o.seller_id, DATE_TRUNC('month', o.created_at)
),

seller_with_growth AS (
    SELECT
        seller_id,
        month,
        total_orders,
        unique_buyers,
        total_gmv,
        avg_order_value,
        platform_revenue,
        avg_rating,
        LAG(total_gmv) OVER (
            PARTITION BY seller_id ORDER BY month
        )                                       AS prev_month_gmv,
        ROUND(
            (total_gmv - LAG(total_gmv) OVER (PARTITION BY seller_id ORDER BY month))
            * 100.0
            / NULLIF(LAG(total_gmv) OVER (PARTITION BY seller_id ORDER BY month), 0),
        2)                                      AS gmv_mom_growth_pct
    FROM monthly_seller_metrics
)

SELECT
    seller_id,
    month,
    total_orders,
    unique_buyers,
    ROUND(total_gmv, 2)             AS total_gmv,
    ROUND(avg_order_value, 2)       AS avg_order_value,
    ROUND(platform_revenue, 2)      AS platform_revenue,
    ROUND(avg_rating, 2)            AS avg_rating,
    gmv_mom_growth_pct,
    RANK() OVER (
        PARTITION BY month ORDER BY total_gmv DESC
    )                               AS gmv_rank_this_month,
    CASE
        WHEN gmv_mom_growth_pct >= 20  THEN 'High Growth'
        WHEN gmv_mom_growth_pct >= 0   THEN 'Stable'
        WHEN gmv_mom_growth_pct < 0    THEN 'Declining'
        ELSE 'New Seller'
    END                             AS seller_health_segment
FROM seller_with_growth
ORDER BY month DESC, total_gmv DESC
;
