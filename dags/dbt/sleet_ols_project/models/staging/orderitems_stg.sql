
{{config(
    materialized='view',
    schema='Processing'
)}}
select orderitemid orderitem_id,
       orderid order_id,
       productid product_id,
       quantity,
       unitprice unit_price,
       quantity * unit_price As total_price,
       updated_at
from 
        {{ source('landing', 'orderitems') }}