{{ config(materialized='table') }}

SELECT
    pickup_date,
    pickup_borough,

    -- Overall trip metrics
    COUNT(*) AS total_trips,

    SUM(COALESCE(passenger_count, 0)) AS total_passengers,

    ROUND(SUM(trip_distance), 2) AS total_distance,

    ROUND(SUM(fare_amount), 2) AS total_fare,

    ROUND(SUM(tip_amount), 2) AS total_tips,

    ROUND(SUM(total_amount), 2) AS total_revenue,

    -- Valid trip metrics
    COUNT_IF(
        duration_quality_status = 'VALID'
    ) AS valid_duration_trips,

    COUNT_IF(
        duration_quality_status <> 'VALID'
    ) AS invalid_duration_trips,

    ROUND(
        AVG(
            CASE
                WHEN duration_quality_status = 'VALID'
                THEN trip_distance
            END
        ),
        2
    ) AS average_valid_trip_distance,

    ROUND(
        AVG(
            CASE
                WHEN duration_quality_status = 'VALID'
                THEN trip_duration_seconds
            END
        ),
        2
    ) AS average_valid_trip_duration_seconds,

    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM {{ ref('int_trips_with_zones') }}

WHERE pickup_date IS NOT NULL

GROUP BY
    pickup_date,
    pickup_borough
