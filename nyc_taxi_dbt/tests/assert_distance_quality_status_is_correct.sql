
SELECT *
FROM {{ ref('int_trips_with_zones') }}

WHERE
    (
        trip_distance IS NULL
        AND distance_quality_status <> 'MISSING_DISTANCE'
    )

    OR (
        trip_distance < 0
        AND distance_quality_status <> 'INVALID_NEGATIVE_DISTANCE'
    )

    OR (
        trip_distance = 0
        AND distance_quality_status <> 'ZERO_DISTANCE'
    )

    OR (
        trip_distance > 100
        AND distance_quality_status <> 'EXTREME_DISTANCE_REVIEW'
    )

    OR (
        trip_distance > 0
        AND trip_distance <= 100
        AND distance_quality_status <> 'VALID_RECORDED_DISTANCE'
    )
