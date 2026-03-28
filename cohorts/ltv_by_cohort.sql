-- ============================================================
-- Lifetime Value (LTV) by Cohort
-- Compatible: Redshift, BigQuery, Snowflake
-- Description: Calculates cumulative revenue per cohort over time.
--              Useful for comparing monetization across signup cohorts
--              and projecting LTV curves.
-- ============================================================

WITH user_cohorts AS (
    SELECT
        user_id,
        DATE_TRUNC('month', MIN(created_at)) AS cohort_month
    FROM orders
    WHERE status = 'completed'
    GROUP BY user_id
),

monthly_revenue AS (
    SELECT
        o.user_id,
        DATE_TRUNC('month', o.created_at)    AS revenue_month,
        SUM(o.gmv)                           AS monthly_gmv
    FROM orders o
    WHERE o.status = 'completed'
    GROUP BY o.user_id, DATE_TRUNC('month', o.created_at)
),

cohort_revenue AS (
    SELECT
        uc.cohort_month,
        DATEDIFF('month', uc.cohort_month, mr.revenue_month) AS months_since_acquisition,
        COUNT(DISTINCT mr.user_id)           AS paying_users,
        SUM(mr.monthly_gmv)                  AS total_gmv
    FROM user_cohorts uc
    JOIN monthly_revenue mr USING (user_id)
    GROUP BY uc.cohort_month, months_since_acquisition
),

cohort_sizes AS (
    SELECT cohort_month, COUNT(DISTINCT user_id) AS cohort_size
    FROM user_cohorts
    GROUP BY cohort_month
)

SELECT
    cr.cohort_month,
    cr.months_since_acquisition,
    cs.cohort_size,
    cr.paying_users,
    cr.total_gmv,
    ROUND(cr.total_gmv / cs.cohort_size, 2)      AS avg_gmv_per_user,
    ROUND(
        SUM(cr.total_gmv) OVER (
            PARTITION BY cr.cohort_month
            ORDER BY cr.months_since_acquisition
        ) / cs.cohort_size, 2
    )                                             AS cumulative_ltv_per_user
FROM cohort_revenue cr
JOIN cohort_sizes cs USING (cohort_month)
ORDER BY cr.cohort_month, cr.months_since_acquisition
;
