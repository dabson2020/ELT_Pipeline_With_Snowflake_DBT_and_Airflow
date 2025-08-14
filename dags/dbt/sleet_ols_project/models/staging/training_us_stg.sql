{{config(
    materialized='view',
    schema='training'
)}}
select sales_date,
        quantity_sold,
        unit_sell_price,
        unit_purchase_cost
from 
        {{ source('training','sales_us') }}