CREATE OR REPLACE TABLE GOLD.DIM_DATE AS
WITH date_range AS (
    SELECT 
        MIN(DATE(ORDER_DATE)) AS min_date,
        MAX(DATE(ORDER_DATE)) AS max_date
    FROM SILVER.ORDERS
),
date_spine AS (
    SELECT 
        DATEADD(DAY, SEQ4(), min_date) AS date_key
    FROM date_range, TABLE(GENERATOR(ROWCOUNT => 10000))
    WHERE date_key <= max_date
)
SELECT 
    date_key,
    YEAR(date_key) AS year,
    QUARTER(date_key) AS quarter,
    MONTH(date_key) AS month,
    MONTHNAME(date_key) AS month_name,
    WEEK(date_key) AS week,
    DAYOFWEEK(date_key) AS day_of_week,
    DAYNAME(date_key) AS day_name,
    DAY(date_key) AS day,
    CASE WHEN DAYOFWEEK(date_key) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend,
    DATE_TRUNC('MONTH', date_key) AS month_start_date,
    LAST_DAY(date_key) AS month_end_date
FROM date_spine;
