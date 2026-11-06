{{ config(materialized='view') }}

SELECT
    LOCATIONID AS location_id,
    BOROUGH AS borough,
    ZONE AS zone_name,
    SERVICE_ZONE AS service_zone,
    SOURCE_FILE AS source_file,
    BATCH_ID AS batch_id,
    INGESTED_AT AS ingested_at

FROM {{ source('raw', 'taxi_zones') }}
