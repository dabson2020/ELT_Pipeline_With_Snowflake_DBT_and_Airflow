with cte_customerorder AS (
select c.customer_id AS customer_id,
    customer_name, 
    COUNT(o.order_id) no_of_orders
    from 
    {{ ref('customer_stg')}} c
    JOIN {{ ref('orders_stg')}} o
    ON o.customer_id = c.customer_id
    GROUP BY 1,2
    ORDER BY 3 DESC)

select customer_id, customer_name, no_of_orders
FROM cte_customerorder