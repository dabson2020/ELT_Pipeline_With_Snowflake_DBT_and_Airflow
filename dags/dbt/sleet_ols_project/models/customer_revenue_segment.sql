{{config(
    materialized='table',
    schema='consumption'
)}}

WITH customer_revenue AS (
    SELECT 
        c.customer_id,
        c.customer_name,
        o.revenue revenue,
    FROM {{ref ('customer_stg')}} c
    JOIN {{ref ('order_facts')}} o 
    ON c.customer_id = o.customer_id
    
),
revenue_percentiles AS (
    SELECT 
        customer_id,
        customer_name,
        revenue,
        NTILE(5) OVER (ORDER BY revenue DESC) AS revenue_bucket
    FROM customer_revenue
)
SELECT 
    customer_id,
    customer_name,
    revenue,
    CASE 
        WHEN revenue_bucket = 1 THEN 'High Value'   -- Top 20%
        WHEN revenue_bucket IN (2,3,4) THEN 'Medium Value' -- Middle 60%
        WHEN revenue_bucket = 5 THEN 'Low Value'    -- Bottom 20%
    END AS segment
FROM revenue_percentiles
ORDER BY revenue DESC