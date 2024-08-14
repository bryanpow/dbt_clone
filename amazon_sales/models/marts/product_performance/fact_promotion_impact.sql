{{ config(
    materialized='table'  
) }}

SELECT
    ASIN,
    Category,
    promotion_type,
    SUM(total_qty_sold) AS total_qty_sold,
    SUM(total_revenue) AS total_revenue,
    AVG(avg_order_value) AS avg_order_value
FROM (
    SELECT
        ASIN,
        Category,
        CASE WHEN PROMOTION_IDS IS NOT NULL THEN 'Promotion Applied' ELSE 'No Promotion' END AS promotion_type,
        QTY AS total_qty_sold,
        AMOUNT AS total_revenue,
        AMOUNT AS avg_order_value
    FROM
        {{ ref('stg_amazon_sales') }}
    WHERE
        STATUS = 'Shipped'
)
GROUP BY
    ASIN, Category, promotion_type
ORDER BY 
    ASIN, promotion_type