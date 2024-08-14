{{ config(
    materialized='table'  
) }}

SELECT
    DATE_TRUNC('month', TO_DATE(Date, 'MM-DD-YY')) AS month,
    Category,
    ASIN,
    SUM(QTY) AS total_qty_sold,
    SUM(CASE WHEN STATUS = 'Shipped' THEN AMOUNT ELSE 0 END) AS total_revenue,
    COUNT(CASE WHEN STATUS = 'Returned' THEN 1 END) AS total_returns,
    COUNT(CASE WHEN STATUS = 'Cancelled' THEN 1 END) AS total_cancellations
FROM
    {{ ref('stg_amazon_sales') }}
WHERE
    STATUS IN ('Shipped', 'Returned', 'Cancelled')
GROUP BY
    month, Category, ASIN