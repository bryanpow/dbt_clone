{{ config(
    materialized='table'  
) }}

WITH 
cancellation_revenue AS (
    SELECT
        ASIN,
        SUM(AMOUNT) AS revenue_loss_due_to_cancellations  -- Calculate revenue loss for cancellations
    FROM
        {{ ref('stg_amazon_sales') }}
    WHERE
        STATUS = 'Cancelled'
    GROUP BY
        ASIN
),

return_revenue AS (
    SELECT
        ASIN,
        SUM(AMOUNT) AS revenue_loss_due_to_returns  -- Calculate revenue loss for returns
    FROM
        {{ ref('stg_amazon_sales') }}
    WHERE
        STATUS = 'Returned'
    GROUP BY
        ASIN    
)

SELECT
    
    ps.ASIN,
    ps.Category,
    SUM(ps.total_qty_sold) AS total_qty_sold,  -- Total quantity sold across all statuses
    SUM(ps.total_revenue) AS total_revenue,  -- Total revenue from shipped orders
    AVG(ps.avg_order_value) AS avg_order_value,  -- Average order value from shipped orders
    SUM(ps.total_returns) AS total_returns,  -- Total returns
    SUM(ps.total_cancellations) AS total_cancellations,  -- Total cancellations
    SUM(ps.total_returns) / NULLIF(SUM(ps.total_qty_sold), 0) * 100 AS return_rate,  -- Return rate as a percentage
    SUM(ps.total_cancellations) / NULLIF(SUM(ps.total_qty_sold), 0) * 100 AS cancellation_rate,  -- Return rate as a percentage
    COALESCE(cr.revenue_loss_due_to_cancellations, 0) AS revenue_loss_due_to_cancellations, -- Revenue loss due to cancellations
    COALESCE(rr.revenue_loss_due_to_returns, 0) AS revenue_loss_due_to_returns  -- Revenue loss due to returns
FROM
    {{ ref('int_product_sales') }} ps
LEFT JOIN
    cancellation_revenue cr ON ps.ASIN = cr.ASIN
LEFT JOIN
    return_revenue rr ON ps.ASIN = rr.ASIN
GROUP BY
    ps.ASIN, ps.Category, cr.revenue_loss_due_to_cancellations, rr.revenue_loss_due_to_returns

