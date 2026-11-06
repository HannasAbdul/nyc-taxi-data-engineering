
{{ config(materialized='view') }}

SELECT
    trips.vendor_id,

    trips.pickup_datetime,
    trips.dropoff_datetime,

    trips.passenger_count,
    trips.trip_distance,

    trips.rate_code_id,
    trips.store_and_forward_flag,

    trips.pickup_location_id,
    pickup_zone.borough AS pickup_borough,
    pickup_zone.zone_name AS pickup_zone,

    trips.dropoff_location_id,
    dropoff_zone.borough AS dropoff_borough,
    dropoff_zone.zone_name AS dropoff_zone,

    trips.payment_type,

    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.congestion_surcharge,
    trips.airport_fee,
    trips.cbd_congestion_fee,

    -- Calculate trip duration in seconds
    DATEDIFF(
        'second',
        trips.pickup_datetime,
        trips.dropoff_datetime
    ) AS trip_duration_seconds,

    -- Classify trip duration quality
    CASE
        WHEN trips.pickup_datetime IS NULL
          OR trips.dropoff_datetime IS NULL
            THEN 'MISSING_TIMESTAMP'

        WHEN trips.dropoff_datetime < trips.pickup_datetime
            THEN 'INVALID_NEGATIVE_DURATION'

        WHEN trips.dropoff_datetime = trips.pickup_datetime
            THEN 'INVALID_ZERO_DURATION'

        WHEN DATEDIFF(
            'second',
            trips.pickup_datetime,
            trips.dropoff_datetime
        ) > 86400
            THEN 'UNUSUALLY_LONG_DURATION'

        ELSE 'VALID'
    END AS duration_quality_status,

    -- Classify trip distance quality
    CASE
        WHEN trips.trip_distance IS NULL
            THEN 'MISSING_DISTANCE'

        WHEN trips.trip_distance < 0
            THEN 'INVALID_NEGATIVE_DISTANCE'

        WHEN trips.trip_distance = 0
            THEN 'ZERO_DISTANCE'

        WHEN trips.trip_distance > 100
            THEN 'EXTREME_DISTANCE_REVIEW'

        ELSE 'VALID_RECORDED_DISTANCE'
    END AS distance_quality_status,

    -- Extract pickup date for dimensional modelling
    TO_DATE(trips.pickup_datetime) AS pickup_date,

    -- Data lineage
    trips.source_file,
    trips.batch_id,
    trips.ingested_at

FROM {{ ref('stg_yellow_trips') }} AS trips

LEFT JOIN {{ ref('stg_taxi_zones') }} AS pickup_zone
    ON trips.pickup_location_id = pickup_zone.location_id

LEFT JOIN {{ ref('stg_taxi_zones') }} AS dropoff_zone
    ON trips.dropoff_location_id = dropoff_zone.location_id
