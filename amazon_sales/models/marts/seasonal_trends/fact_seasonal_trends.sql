{{ config(
    materialized='table'  
) }}

SELECT
    month,
    ASIN,
    Category,
    SUM(total_qty_sold) AS total_qty_sold,  -- Total quantity sold in each month
    SUM(total_revenue) AS total_revenue,  -- Total revenue in each month
    SUM(total_returns) AS total_returns,  -- Total returns in each month
    SUM(total_cancellations) AS total_cancellations,  -- Total cancellations in each month
    SUM(total_returns) / NULLIF(SUM(total_qty_sold), 0) * 100 AS return_rate,  -- Return rate as a percentage in each month
    SUM(total_cancellations) / NULLIF(SUM(total_qty_sold), 0) * 100 AS cancellation_rate  -- Cancellation rate as a percentage in each month
FROM
    {{ ref('int_seasonal_trends') }}
GROUP BY
    month, Category,ASIN