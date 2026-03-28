-- ============================================================
-- Checkout Conversion Analysis
-- Compatible: Redshift, BigQuery, Snowflake
-- Description: Measures conversion and drop-off through the checkout
--              flow. Breaks down by device type and traffic source
--              to identify where and why users abandon.
-- ============================================================

WITH checkout_events AS (
    SELECT
        session_id,
        user_id,
        device_type,
        traffic_source,
        MAX(CASE WHEN event_type = 'checkout_start'         THEN 1 ELSE 0 END) AS started_checkout,
        MAX(CASE WHEN event_type = 'shipping_info_entered'  THEN 1 ELSE 0 END) AS entered_shipping,
        MAX(CASE WHEN event_type = 'payment_info_entered'   THEN 1 ELSE 0 END) AS entered_payment,
        MAX(CASE WHEN event_type = 'order_review_viewed'    THEN 1 ELSE 0 END) AS viewed_review,
        MAX(CASE WHEN event_type = 'purchase_complete'      THEN 1 ELSE 0 END) AS completed_purchase
    FROM events
    WHERE event_type IN (
        'checkout_start', 'shipping_info_entered',
        'payment_info_entered', 'order_review_viewed', 'purchase_complete'
    )
    AND event_timestamp >= DATEADD('day', -30, CURRENT_DATE)
    GROUP BY session_id, user_id, device_type, traffic_source
),

conversion_by_segment AS (
    SELECT
        device_type,
        traffic_source,
        COUNT(*)                                                    AS checkout_sessions,
        SUM(entered_shipping)                                       AS entered_shipping,
        SUM(entered_payment)                                        AS entered_payment,
        SUM(viewed_review)                                          AS viewed_review,
        SUM(completed_purchase)                                     AS completed_purchase,
        ROUND(SUM(completed_purchase) * 100.0 / COUNT(*), 2)       AS overall_conversion_pct,
        ROUND(
            (1 - SUM(completed_purchase) * 1.0 / NULLIF(COUNT(*), 0)) * 100, 2
        )                                                           AS abandonment_rate_pct
    FROM checkout_events
    WHERE started_checkout = 1
    GROUP BY device_type, traffic_source
)

SELECT *
FROM conversion_by_segment
ORDER BY overall_conversion_pct DESC
;
