{{ config(
    materialized='table'  
) }}

SELECT
    SHIP_STATE,
    Category,
    SUM(QTY) AS total_qty_sold,  -- Total quantity sold in each state and category
    SUM(CASE WHEN STATUS = 'Shipped' THEN AMOUNT ELSE 0 END) AS total_revenue,  -- Total revenue from shipped orders
    AVG(CASE WHEN STATUS = 'Shipped' THEN AMOUNT ELSE NULL END) AS avg_order_value,  -- Average order value from shipped orders
    COUNT(CASE WHEN STATUS = 'Returned' THEN 1 ELSE NULL END) AS total_returns,  -- Total returns in each state and category
    COUNT(CASE WHEN STATUS = 'Cancelled' THEN 1 ELSE NULL END) AS total_cancellations,  -- Total cancellations in each state and category
    SUM(CASE WHEN STATUS = 'Returned' THEN AMOUNT ELSE 0 END) / NULLIF(SUM(CASE WHEN STATUS = 'Shipped' THEN AMOUNT ELSE 0 END), 0) * 100 AS return_rate,  -- Return rate as a percentage
    SUM(CASE WHEN STATUS = 'Cancelled' THEN AMOUNT ELSE 0 END) / NULLIF(SUM(CASE WHEN STATUS = 'Shipped' THEN AMOUNT ELSE 0 END), 0) * 100 AS cancellation_rate  -- Cancellation rate as a percentage
FROM
    {{ ref('stg_amazon_sales') }}
GROUP BY
    SHIP_STATE, Category