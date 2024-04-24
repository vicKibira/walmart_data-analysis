{{ config(materialized='view') }}

WITH walmart_sales_filtered AS (
    SELECT 
        Store AS store,
        Date AS date_of_sell,
        Weekly_sales AS weekly_sales,
        Holiday_Flag AS holiday_flag,
        Temperatures AS temperature,
        Fuel_Price AS fuel_price,
        CPI AS cpi,
        Unemployment AS unemployment,
        ROW_NUMBER() OVER () AS rn
    FROM {{ source('staging', 'walmart_sales_data') }}
)
SELECT *
FROM walmart_sales_filtered
WHERE rn = 1

{% if var('is_test_run', default=true) %}

LIMIT 100

{% endif %}
