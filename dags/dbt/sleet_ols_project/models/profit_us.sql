{{ config( 
    materialized='view',
    schema='training'
)}}

{{ compute_profit ('sales_us') }}