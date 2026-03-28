-- ============================================================
-- DBT Model: dim_users
-- Layer: Mart (dimension table)
-- Description: One row per user. Combines profile, behavioral,
--              and segmentation attributes for use across fact tables.
-- ============================================================

{{
  config(
    materialized = 'table',
    dist = 'user_id'
  )
}}

WITH base_users AS (
    SELECT * FROM {{ source('raw', 'users') }}
),

order_stats AS (
    SELECT
        buyer_id                            AS user_id,
        COUNT(DISTINCT order_id)            AS total_orders,
        SUM(gmv)                            AS total_gmv,
        MIN(created_at)                     AS first_purchase_date,
        MAX(created_at)                     AS last_purchase_date,
        DATEDIFF(
            'day', MAX(created_at), CURRENT_DATE
        )                                   AS days_since_last_purchase
    FROM {{ source('raw', 'orders') }}
    WHERE status = 'completed'
    GROUP BY buyer_id
),

seller_stats AS (
    SELECT
        seller_id                           AS user_id,
        COUNT(DISTINCT order_id)            AS total_sales,
        SUM(gmv)                            AS total_gmv_sold,
        MIN(created_at)                     AS first_sale_date
    FROM {{ source('raw', 'orders') }}
    WHERE status = 'completed'
    GROUP BY seller_id
)

SELECT
    u.user_id,
    u.email,
    u.created_at                            AS signup_date,
    u.country,
    u.device_type,
    u.acquisition_source,

    -- Purchase behavior
    COALESCE(os.total_orders, 0)            AS total_orders,
    COALESCE(os.total_gmv, 0)              AS total_buyer_gmv,
    os.first_purchase_date,
    os.last_purchase_date,
    os.days_since_last_purchase,

    -- Seller behavior
    COALESCE(ss.total_sales, 0)            AS total_sales,
    COALESCE(ss.total_gmv_sold, 0)        AS total_seller_gmv,
    ss.first_sale_date,

    -- Derived flags
    CASE WHEN ss.user_id IS NOT NULL THEN TRUE ELSE FALSE END   AS is_seller,
    CASE WHEN os.user_id IS NOT NULL THEN TRUE ELSE FALSE END   AS is_buyer,
    CASE
        WHEN os.user_id IS NOT NULL
         AND ss.user_id IS NOT NULL THEN TRUE ELSE FALSE
    END                                                          AS is_buyer_and_seller,

    -- Engagement segment
    CASE
        WHEN os.days_since_last_purchase <= 30
             AND os.total_orders >= 5       THEN 'Power Buyer'
        WHEN os.days_since_last_purchase <= 30 THEN 'Active'
        WHEN os.days_since_last_purchase <= 90 THEN 'At Risk'
        WHEN os.days_since_last_purchase > 90  THEN 'Churned'
        ELSE 'Never Purchased'
    END                                     AS user_segment

FROM base_users u
LEFT JOIN order_stats os  USING (user_id)
LEFT JOIN seller_stats ss USING (user_id)
```

4. Scroll down and click **"Commit changes"**

---

That completes `sql-analytics-library`. Your repo will have this structure:
```
sql-analytics-library/
├── cohorts/
│   ├── weekly_cohort_retention.sql
│   └── ltv_by_cohort.sql
├── funnels/
│   ├── acquisition_funnel.sql
│   └── checkout_conversion.sql
├── marketplace/
│   ├── gmv_by_seller.sql
│   └── buyer_engagement.sql
├── sessions/
│   └── session_attribution.sql
└── dbt_models/
    ├── fct_user_events.sql
    └── dim_users.sql
