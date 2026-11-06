{{ config(materialized='table') }}

SELECT
    -- Dimension foreign keys
    pickup_date,
    pickup_location_id,
    dropoff_location_id,
    vendor_id,
    payment_type,

    -- Trip identifiers and attributes
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_forward_flag,

    -- Financial measures
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,

    -- Duration and distance quality
    trip_duration_seconds,
    duration_quality_status,
    distance_quality_status,

    -- Passenger quality
    CASE
        WHEN passenger_count IS NULL
            THEN 'MISSING_PASSENGER_COUNT'

        WHEN passenger_count = 0
            THEN 'ZERO_PASSENGER_COUNT'

        WHEN passenger_count < 0
            THEN 'INVALID_NEGATIVE_PASSENGER_COUNT'

        ELSE 'VALID'
    END AS passenger_count_quality_status,

    -- Data lineage
    source_file,
    batch_id,
    ingested_at

FROM {{ ref('int_trips_with_zones') }}

WHERE duration_quality_status = 'VALID'
