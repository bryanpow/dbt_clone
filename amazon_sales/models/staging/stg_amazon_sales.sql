{{ config(
    materialized='table'  
) }}

-- IF COURIER_STATUS = ‘Cancelled’ change STATUS to ‘Cancelled’
-- Change ‘Shipped-Delivered to buyer’ in status column to ‘Shipped’
-- Change ‘Shipped - Returned to Seller’ to ‘Returned’
-- Change ‘Shipped - Returning to Seller’ to ‘Returned’
-- Change ‘Shipped - Out for Delivery’  to ‘Shipped’
-- Drop all that say ‘Shipped - Damaged’ or ‘Shipped - Lost in Transit’ or ‘Shipped - Rejected by Buyer’
-- Change ‘Shipping’ to Shipped
-- Fix nulls in amount by using subqueries (And If we cant find an amount to use from other orders of the same product then remove that product)
-- DROP all columns not being used by the bi questions (keep COURIER_STATUS for staging)


WITH amount_cleaned AS (
    SELECT 
        ASIN,
        Category,
        PROMOTION_IDS,
        Qty,
        -- Convert AMOUNT from INR to USD before applying the FIRST_VALUE function
        FIRST_VALUE(AMOUNT * 0.012) OVER (PARTITION BY ASIN ORDER BY CASE WHEN AMOUNT IS NOT NULL THEN 1 ELSE 0 END DESC) AS AMOUNT_CLEANED,
        STATUS,
        Fulfilment,
        Date,
        SHIP_STATE,
        COURIER_STATUS
    FROM 
        {{ source('google_sheets', 'AMAZON_SALES') }} a
),

status_cleaned AS (
    SELECT
        ASIN,
        PROMOTION_IDS,
        Category,
        CASE WHEN Qty = 0 THEN 1 ELSE Qty END AS Qty,
        AMOUNT_CLEANED,
        CASE
            WHEN COURIER_STATUS = 'Cancelled' THEN 'Cancelled'
            WHEN STATUS = 'Shipped - Delivered to Buyer' THEN 'Shipped'
            WHEN STATUS IN ('Shipped - Returned to Seller', 'Shipped - Returning to Seller') THEN 'Returned'
            WHEN STATUS = 'Shipped - Out for Delivery' THEN 'Shipped'
            WHEN STATUS = 'Pending - Waiting for Pick Up' THEN 'Shipped'
            WHEN STATUS = 'Shipped - Picked Up' THEN 'Shipped'
            WHEN STATUS = 'Shipping' THEN 'Shipped'
            ELSE STATUS
        END AS STATUS_CLEANED,
        Fulfilment,
        Date,
        SHIP_STATE,
        COURIER_STATUS
    FROM
        amount_cleaned
    WHERE
        STATUS NOT IN ('Shipped - Damaged', 'Shipped - Lost in Transit', 'Shipped - Rejected by Buyer')
)

SELECT 
    ASIN,
    Category,
    PROMOTION_IDS,
    Qty,
    AMOUNT_CLEANED AS AMOUNT,
    STATUS_CLEANED AS STATUS,
    Fulfilment,
    Date,
    SHIP_STATE
FROM 
    status_cleaned
WHERE 
    AMOUNT_CLEANED IS NOT NULL
    AND AMOUNT_CLEANED > 0