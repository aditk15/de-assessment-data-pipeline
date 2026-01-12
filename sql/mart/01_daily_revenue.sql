CREATE OR REPLACE VIEW MART.DAILY_REVENUE AS
SELECT 
    D.date_key,
    D.year,
    D.month,
    D.month_name,
    D.day,
    D.day_name,
    COUNT(DISTINCT F.ORDER_ID) AS total_orders,
    SUM(F.ORDER_REVENUE_USD) AS total_revenue_usd,
    AVG(F.ORDER_REVENUE_USD) AS avg_order_value_usd,
    SUM(F.TOTAL_QUANTITY) AS total_items_sold,
    SUM(F.TOTAL_DISCOUNT_USD) AS total_discounts_usd,
    SUM(F.IS_PAID) AS paid_orders,
    SUM(F.IS_DELIVERED) AS delivered_orders,
    SUM(F.IS_FULFILLED) AS fulfilled_orders
FROM GOLD.FACT_ORDERS F
INNER JOIN GOLD.DIM_DATE D ON F.ORDER_DATE_KEY = D.DATE_KEY
GROUP BY D.date_key, D.year, D.month, D.month_name, D.day, D.day_name
ORDER BY D.date_key DESC;
