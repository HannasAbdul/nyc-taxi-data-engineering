SELECT
    *
FROM {{ ref('int_trips_with_zones') }}
WHERE duration_quality_status = 'VALID'
  AND (
      trip_duration_seconds <= 0
      OR trip_duration_seconds > 86400
  )
