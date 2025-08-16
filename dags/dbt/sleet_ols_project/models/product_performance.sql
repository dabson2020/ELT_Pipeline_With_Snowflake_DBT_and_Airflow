{{config(materilized='table', schema='consumption')}}

select p.product_id, 
       p.product_name, 
       p.retail_price,
       SUM(oi.total_price) revenue,
       SUM(oi.quantity) AS total_quantity_sold
FROM {{ref('products_stg')}} p
JOIN {{ ref("orderitems_stg")}} oi
ON oi.product_id = p.product_id
GROUP BY 1,2,3
ORDER BY 4 DESC