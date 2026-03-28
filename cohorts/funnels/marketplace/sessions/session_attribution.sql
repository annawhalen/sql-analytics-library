-- ============================================================
-- Session Attribution Analysis
-- Compatible: Redshift, BigQuery, Snowflake
-- Description: Attributes purchases to traffic sources using both
--              first-touch and last-touch attribution models.
--              Compares channel performance by volume and revenue.
-- ============================================================

WITH sessions AS (
    SELECT
        session_id,
        user_id,
        traffic_source,
        utm_campaign,
        utm_medium,
        MIN(event_timestamp)    AS session_start,
        MAX(event_timestamp)    AS session_end,
        COUNT(*)                AS events_in_session
    FROM events
    WHERE event_timestamp >= DATEADD('day', -30, CURRENT_DATE)
    GROUP BY session_id, user_id, traffic_source, utm_campaign, utm_medium
),

purchases AS (
    SELECT
        o.order_id,
        o.user_id,
        o.created_at            AS purchase_time,
        o.gmv
    FROM orders o
    WHERE o.status = 'completed'
    AND o.created_at >= DATEADD('day', -30, CURRENT_DATE)
),

last_touch AS (
    SELECT
        p.order_id,
        p.user_id,
        p.gmv,
        s.traffic_source        AS last_touch_source,
        s.utm_campaign          AS last_touch_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY p.order_id
            ORDER BY s.session_start DESC
        )                       AS rn
    FROM purchases p
    JOIN sessions s
        ON p.user_id = s.user_id
        AND s.session_start <= p.purchase_time
),

first_touch AS (
    SELECT
        p.order_id,
        p.user_id,
        p.gmv,
        s.traffic_source        AS first_touch_source,
        s.utm_campaign          AS first_touch_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY p.order_id
            ORDER BY s.session_start ASC
        )                       AS rn
    FROM purchases p
    JOIN sessions s ON p.user_id = s.user_id
)

SELECT
    lt.last_touch_source,
    lt.last_touch_campaign,
    ft.first_touch_source,
    COUNT(DISTINCT lt.order_id)             AS orders_last_touch,
    ROUND(SUM(lt.gmv), 2)                   AS gmv_last_touch,
    COUNT(DISTINCT ft.order_id)             AS orders_first_touch,
    ROUND(SUM(ft.gmv), 2)                   AS gmv_first_touch
FROM last_touch lt
JOIN first_touch ft ON lt.order_id = ft.order_id
WHERE lt.rn = 1 AND ft.rn = 1
GROUP BY lt.last_touch_source, lt.last_touch_campaign, ft.first_touch_source
ORDER BY gmv_last_touch DESC
;
