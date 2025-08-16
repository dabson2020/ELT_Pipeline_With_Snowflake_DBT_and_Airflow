{{config(
    materialized='view',
    schema='landing'
)}}
    
    
select storeid store_id,
       storename store_name,
       address,
       city,
       state,
       zipcode zip_code,
       email,
       phone,
       updated_at
from 
        {{ source('landing', 'stores') }}