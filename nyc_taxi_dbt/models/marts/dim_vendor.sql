{{ config(materialized='table') }}

WITH vendors AS (

    SELECT DISTINCT
        vendor_id
    FROM {{ ref('fct_trips') }}
    WHERE vendor_id IS NOT NULL

)

SELECT
    vendor_id,

    CASE
        WHEN vendor_id = 1
            THEN 'Creative Mobile Technologies, LLC'

        WHEN vendor_id = 2
            THEN 'Curb Mobility, LLC'

        WHEN vendor_id = 6
            THEN 'Myle Technologies Inc'

        ELSE 'Unknown Vendor'
    END AS vendor_name,

    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM vendors
