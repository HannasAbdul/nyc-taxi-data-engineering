{{ config(materialized='view') }}

SELECT
    VENDORID AS vendor_id,

    TPEP_PICKUP_DATETIME AS pickup_datetime,
    TPEP_DROPOFF_DATETIME AS dropoff_datetime,

    PASSENGER_COUNT AS passenger_count,
    TRIP_DISTANCE AS trip_distance,

    RATECODEID AS rate_code_id,
    STORE_AND_FWD_FLAG AS store_and_forward_flag,

    PULOCATIONID AS pickup_location_id,
    DOLOCATIONID AS dropoff_location_id,

    PAYMENT_TYPE AS payment_type,

    FARE_AMOUNT AS fare_amount,
    EXTRA AS extra,
    MTA_TAX AS mta_tax,
    TIP_AMOUNT AS tip_amount,
    TOLLS_AMOUNT AS tolls_amount,
    IMPROVEMENT_SURCHARGE AS improvement_surcharge,
    TOTAL_AMOUNT AS total_amount,
    CONGESTION_SURCHARGE AS congestion_surcharge,
    AIRPORT_FEE AS airport_fee,
    CBD_CONGESTION_FEE AS cbd_congestion_fee,

    SOURCE_FILE AS source_file,
    BATCH_ID AS batch_id,
    INGESTED_AT AS ingested_at

FROM {{ source('raw', 'yellow_trips') }}
