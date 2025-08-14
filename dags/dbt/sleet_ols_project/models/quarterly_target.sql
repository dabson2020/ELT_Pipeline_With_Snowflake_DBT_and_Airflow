{{config(
    materialized='table',
    schema='Consumption'
)}}

select o.store_id,
        s.salestarget salestarget,
        SUM(oi.total_price) revenue,
        ROUND(SUM(oi.total_price)/s.salestarget * 100,2) percent_achieved
        
        
from {{ ref ('orders_stg')}} o
JOIN {{ ref ('orderitems_stg')}} oi
ON o.order_id = oi.order_id
JOIN {{ ref ('salestargets')}} s
ON o.store_id = s.store_id

GROUP BY 1,2

