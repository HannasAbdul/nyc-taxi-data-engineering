SELECT
    pickup_date,
    pickup_borough,
    total_trips,
    valid_duration_trips,
    invalid_duration_trips
FROM {{ ref('mart_daily_borough_revenue') }}
WHERE total_trips <>
      valid_duration_trips + invalid_duration_trips
