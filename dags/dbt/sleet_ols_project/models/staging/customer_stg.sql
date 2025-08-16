
{{config(
    materialized='view',
    schema='landing'
)}}
select customerid customer_id,
       CONCAT(firstname,' ',lastname) customer_name,
       email,
       phone,
       address,
       city,
       state,
       zipcode zip_code,
       updated_at
from 
        {{ source('landing', 'customers') }}