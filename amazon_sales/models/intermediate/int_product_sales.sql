{{ config(
    materialized='table'  
) }}

SELECT
    ASIN,
    Category,
    SHIP_STATE AS ship_state,
    SUM(QTY) AS total_qty_sold,  -- Includes all statuses
    SUM(CASE WHEN STATUS = 'Shipped' THEN AMOUNT ELSE 0 END) AS total_revenue,  -- Revenue only from shipped orders
    SUM(CASE WHEN STATUS = 'Pending' THEN AMOUNT ELSE 0 END) AS potential_revenue,  -- Potential revenue from pending orders
    AVG(AMOUNT) AS avg_order_value,  -- Average of orders
    COUNT(CASE WHEN STATUS = 'Returned' THEN 1 ELSE NULL END) AS total_returns,
    COUNT(CASE WHEN STATUS = 'Cancelled' THEN 1 ELSE NULL END) AS total_cancellations,
    
FROM
    {{ ref('stg_amazon_sales') }}
WHERE
    STATUS IN ('Shipped', 'Returned', 'Cancelled', 'Pending')
GROUP BY
    ASIN, Category, SHIP_STATE