-- ============================================================
-- Acquisition Funnel Analysis
-- Compatible: Redshift, BigQuery, Snowflake
-- Description: Tracks users through the acquisition funnel from
--              first visit through account creation and first purchase.
--              Calculates step-level conversion rates and drop-off.
-- ============================================================

WITH funnel_steps AS (
    SELECT
        user_id,
        MAX(CASE WHEN event_type = 'page_view'          THEN 1 ELSE 0 END) AS reached_visit,
        MAX(CASE WHEN event_type = 'signup_start'       THEN 1 ELSE 0 END) AS reached_signup_start,
        MAX(CASE WHEN event_type = 'signup_complete'    THEN 1 ELSE 0 END) AS reached_signup_complete,
        MAX(CASE WHEN event_type = 'listing_view'       THEN 1 ELSE 0 END) AS reached_listing_view,
        MAX(CASE WHEN event_type = 'add_to_cart'        THEN 1 ELSE 0 END) AS reached_add_to_cart,
        MAX(CASE WHEN event_type = 'purchase_complete'  THEN 1 ELSE 0 END) AS reached_purchase
    FROM events
    WHERE event_timestamp >= DATEADD('day', -30, CURRENT_DATE)
    GROUP BY user_id
),

funnel_counts AS (
    SELECT
        SUM(reached_visit)           AS step1_visit,
        SUM(reached_signup_start)    AS step2_signup_start,
        SUM(reached_signup_complete) AS step3_signup_complete,
        SUM(reached_listing_view)    AS step4_listing_view,
        SUM(reached_add_to_cart)     AS step5_add_to_cart,
        SUM(reached_purchase)        AS step6_purchase
    FROM funnel_steps
)

SELECT
    '1. Visit'            AS funnel_step,
    step1_visit           AS users,
    100.0                 AS pct_of_top,
    NULL                  AS pct_of_prev_step
FROM funnel_counts

UNION ALL SELECT '2. Signup Start',    step2_signup_start,
    ROUND(step2_signup_start * 100.0 / NULLIF(step1_visit, 0), 2),
    ROUND(step2_signup_start * 100.0 / NULLIF(step1_visit, 0), 2)
FROM funnel_counts

UNION ALL SELECT '3. Signup Complete', step3_signup_complete,
    ROUND(step3_signup_complete * 100.0 / NULLIF(step1_visit, 0), 2),
    ROUND(step3_signup_complete * 100.0 / NULLIF(step2_signup_start, 0), 2)
FROM funnel_counts

UNION ALL SELECT '4. Listing View',    step4_listing_view,
    ROUND(step4_listing_view * 100.0 / NULLIF(step1_visit, 0), 2),
    ROUND(step4_listing_view * 100.0 / NULLIF(step3_signup_complete, 0), 2)
FROM funnel_counts

UNION ALL SELECT '5. Add to Cart',     step5_add_to_cart,
    ROUND(step5_add_to_cart * 100.0 / NULLIF(step1_visit, 0), 2),
    ROUND(step5_add_to_cart * 100.0 / NULLIF(step4_listing_view, 0), 2)
FROM funnel_counts

UNION ALL SELECT '6. Purchase',        step6_purchase,
    ROUND(step6_purchase * 100.0 / NULLIF(step1_visit, 0), 2),
    ROUND(step6_purchase * 100.0 / NULLIF(step5_add_to_cart, 0), 2)
FROM funnel_counts

ORDER BY funnel_step
;
