-- ============================================================
-- DBT Model: fct_user_events
-- Layer: Mart (fact table)
-- Description: Grain = one row per user event. Enriches raw events
--              with user and session dimensions for downstream analysis.
-- ============================================================

{{
  config(
    materialized = 'incremental',
    unique_key = 'event_id',
    sort = 'event_timestamp',
    dist = 'user_id'
  )
}}

WITH raw_events AS (
    SELECT *
    FROM {{ source('raw', 'events') }}
    {% if is_incremental() %}
        WHERE event_timestamp > (SELECT MAX(event_timestamp) FROM {{ this }})
    {% endif %}
),

users AS (
    SELECT * FROM {{ ref('dim_users') }}
),

session_info AS (
    SELECT
        session_id,
        traffic_source,
        utm_campaign,
        utm_medium,
        device_type,
        MIN(event_timestamp) AS session_start_time
    FROM raw_events
    GROUP BY session_id, traffic_source, utm_campaign, utm_medium, device_type
)

SELECT
    e.event_id,
    e.event_timestamp,
    e.event_type,
    e.session_id,
    e.user_id,
    u.signup_date,
    u.user_segment,
    u.country,
    s.traffic_source,
    s.utm_campaign,
    s.utm_medium,
    s.device_type,
    s.session_start_time,
    DATEDIFF('day', u.signup_date, e.event_timestamp) AS days_since_signup,
    e.properties

FROM raw_events e
LEFT JOIN users u        USING (user_id)
LEFT JOIN session_info s USING (session_id)
