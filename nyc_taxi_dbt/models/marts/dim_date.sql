{{ config(materialized='table') }}

WITH date_bounds AS (

    SELECT
        MIN(pickup_date) AS min_date,
        MAX(pickup_date) AS max_date
    FROM {{ ref('int_trips_with_zones') }}

),

date_spine AS (

    SELECT
        DATEADD(DAY, SEQ4(), min_date)::DATE AS date_day

    FROM date_bounds,
         TABLE(GENERATOR(ROWCOUNT => 10000))

    WHERE DATEADD(DAY, SEQ4(), min_date)::DATE <= max_date

)

SELECT
    date_day,

    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    MONTHNAME(date_day) AS month_name,

    EXTRACT(DAY FROM date_day) AS day_of_month,
    DAYNAME(date_day) AS day_name,
    DAYOFWEEKISO(date_day) AS day_of_week,

    CASE
        WHEN DAYOFWEEKISO(date_day) IN (6, 7)
        THEN TRUE
        ELSE FALSE
    END AS is_weekend

FROM date_spine
