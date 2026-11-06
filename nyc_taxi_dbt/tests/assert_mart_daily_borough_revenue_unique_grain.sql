SELECT
    pickup_date,
    pickup_borough,
    COUNT(*) AS record_count

FROM {{ ref('mart_daily_borough_revenue') }}

GROUP BY
    pickup_date,
    pickup_borough

HAVING COUNT(*) > 1
