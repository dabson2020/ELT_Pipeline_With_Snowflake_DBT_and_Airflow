{{
    config(
        materialized='view',
        schema='landing'
    )
}}

select productid product_id,
       name product_name,
       category product_category,
       retailprice retail_price
from 
    {{ source('landing','products')}}