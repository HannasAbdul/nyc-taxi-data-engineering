{{ config(materialized='table') }}

SELECT
    location_id,
    borough,
    zone_name,
    service_zone,

    source_file,
    batch_id,
    ingested_at,

    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM {{ ref('stg_taxi_zones') }}
