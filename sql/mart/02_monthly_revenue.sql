CREATE OR REPLACE VIEW MART.MONTHLY_REVENUE AS
SELECT 
    D.year,
    D.month,
    D.month_name,
    COUNT(DISTINCT F.ORDER_ID) AS total_orders,
    SUM(F.ORDER_REVENUE_USD) AS total_revenue_usd,
    AVG(F.ORDER_REVENUE_USD) AS avg_order_value_usd,
    SUM(F.TOTAL_QUANTITY) AS total_items_sold,
    SUM(F.TOTAL_DISCOUNT_USD) AS total_discounts_usd,
    ROUND((SUM(F.TOTAL_DISCOUNT_USD) / NULLIF(SUM(F.LIST_PRICE_TOTAL_USD), 0)) * 100, 2) AS avg_discount_percentage,
    COUNT(DISTINCT F.CUSTOMER_ID) AS unique_customers,
    ROUND(SUM(F.IS_PAID) * 100.0 / COUNT(*), 2) AS payment_success_rate,
    ROUND(SUM(F.IS_DELIVERED) * 100.0 / COUNT(*), 2) AS delivery_rate,
    AVG(F.DELIVERY_DAYS) AS avg_delivery_days
FROM GOLD.FACT_ORDERS F
INNER JOIN GOLD.DIM_DATE D ON F.ORDER_DATE_KEY = D.DATE_KEY
GROUP BY D.year, D.month, D.month_name
ORDER BY D.year DESC, D.month DESC;
