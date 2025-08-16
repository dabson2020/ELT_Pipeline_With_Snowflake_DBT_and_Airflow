-- employess with total_orders and revenue
{{config(materialized = 'table', schema = 'consumption')}}

-- employess with total_orders and revenue
select e.employee_id,
       employee_name,
       COUNT(DISTINCT(o.order_id)) order_count, 
       SUM(oi.quantity*oi.unit_price) revenue
FROM {{ ref('employees_stg')}} e
JOIN {{ ref('orders_stg')}} o
ON e.employee_id = o.employee_id
JOIN {{ ref('orderitems_stg')}} oi
ON o.order_id = oi.order_id
GROUP BY 1,2
ORDER BY 3 DESC

