{{ config(materialized='table') }}

WITH payment_types AS (

    SELECT
        column1::NUMBER AS payment_type,
        column2::VARCHAR AS payment_type_description

    FROM VALUES
        (0, 'Flex Fare'),
        (1, 'Credit card'),
        (2, 'Cash'),
        (3, 'No charge'),
        (4, 'Dispute'),
        (5, 'Unknown'),
        (6, 'Voided trip')

)

SELECT
    payment_type,
    payment_type_description,
    CURRENT_TIMESTAMP() AS dbt_updated_at

FROM payment_types
