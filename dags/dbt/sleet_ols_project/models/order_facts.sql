select o.order_id,
       o.order_date,
       o.customer_id,
       o.employee_id,
       o.store_id,
       o.statuscd,
       o.StatusDesc,
       o.updated_at,
       COUNT(DISTINCT(o.order_id)) AS Order_count,
       SUM(oi.total_price) revenue
FROM
   {{ ref('orders_stg') }} o
JOIN {{ ref('orderitems_stg') }} oi
ON o.order_id = oi.order_id

GROUP BY o.order_id,
       o.order_date,
       o.customer_id,
       o.employee_id,
       o.store_id,
       o.statuscd,
       o.StatusDesc,
       o.updated_at

ORDER BY revenue DESC