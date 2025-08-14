{{config(materialized='table',
         schema='consumption') }}
       
       with cte_customer_revenue AS (
    SELECT c.customer_id,
           c.customer_name,
           o.revenue revenue,
           o.Order_count order_count
           
    FROM {{ref ('customer_stg') }} c
    JOIN {{ref('order_facts') }} o 
    ON o.customer_id = c.customer_id)
    
    

    select customer_id, customer_name, revenue, order_count
    from cte_customer_revenue