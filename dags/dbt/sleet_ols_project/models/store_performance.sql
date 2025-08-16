SELECT 
    s.store_id,
    s.store_name,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(oi.quantity * oi.unit_price) AS total_revenue,
    ROUND(SUM(oi.quantity * oi.unit_price) / COUNT(DISTINCT o.order_id),2) AS avg_order_value
FROM {{ ref('stores_stg')}} s
JOIN {{ ref("orders_stg")}} o
ON s.store_id = o.store_id
JOIN {{ ref('orderitems_stg')}} oi
ON o.order_id = oi.order_id
GROUP BY s.store_id, s.store_name
ORDER BY total_revenue DESC