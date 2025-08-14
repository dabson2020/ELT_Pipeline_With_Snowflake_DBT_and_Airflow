
{{config(
    materialized='incremental',
    unique_key = 'order_id',
    schema='Processing'
)}}
SELECT orderid order_id,
      orderdate order_date,
      customerid customer_id,
      employeeid employee_id,
      storeid store_id,
      status as statuscd,
      CASE 
            WHEN status = '01' THEN 'In Progress'
            WHEN status = '02' THEN 'Completed'
            WHEN status = '03' THEN 'Canceled'
      ELSE NULL
      END AS StatusDesc,
      updated_at
 FROM 
      {{ source('landing', 'orders') }}

{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}