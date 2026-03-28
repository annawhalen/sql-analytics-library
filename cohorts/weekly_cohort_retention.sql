-- ============================================================
-- Weekly Cohort Retention Analysis
-- Compatible: Redshift, BigQuery, Snowflake
-- Description: Calculates week-over-week retention by signup cohort.
--              Each user is assigned to a cohort based on their first
--              activity week. Retention is measured as the % of that
--              cohort still active in subsequent weeks.
-- ============================================================

WITH user_first_week AS (
    SELECT
        user_id,
        DATE_TRUNC('week', MIN(event_timestamp)) AS cohort_week
    FROM events
    WHERE event_type = 'session_start'
    GROUP BY user_id
),

user_activity_weeks AS (
    SELECT DISTINCT
        user_id,
        DATE_TRUNC('week', event_timestamp) AS activity_week
    FROM events
    WHERE event_type = 'session_start'
),

cohort_activity AS (
    SELECT
        f.user_id,
        f.cohort_week,
        a.activity_week,
        DATEDIFF('week', f.cohort_week, a.activity_week) AS week_number
    FROM user_first_week f
    JOIN user_activity_weeks a USING (user_id)
),

cohort_sizes AS (
    SELECT
        cohort_week,
        COUNT(DISTINCT user_id) AS cohort_size
    FROM user_first_week
    GROUP BY cohort_week
)

SELECT
    ca.cohort_week,
    ca.week_number,
    cs.cohort_size,
    COUNT(DISTINCT ca.user_id)                              AS retained_users,
    ROUND(
        COUNT(DISTINCT ca.user_id) * 100.0 / cs.cohort_size, 2
    )                                                       AS retention_rate_pct
FROM cohort_activity ca
JOIN cohort_sizes cs USING (cohort_week)
WHERE ca.week_number BETWEEN 0 AND 12
GROUP BY ca.cohort_week, ca.week_number, cs.cohort_size
ORDER BY ca.cohort_week, ca.week_number
;
