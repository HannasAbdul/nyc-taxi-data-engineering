WITH intermediate_totals AS (

    SELECT
        COUNT(*) AS total_trips
    FROM {{ ref('int_trips_with_zones') }}
    WHERE pickup_date IS NOT NULL

),

mart_totals AS (

    SELECT
        COALESCE(SUM(total_trips), 0) AS total_trips
    FROM {{ ref('mart_daily_borough_revenue') }}

)

SELECT
    intermediate_totals.total_trips AS intermediate_trips,
    mart_totals.total_trips AS mart_trips

FROM intermediate_totals
CROSS JOIN mart_totals

WHERE intermediate_totals.total_trips <> mart_totals.total_trips
