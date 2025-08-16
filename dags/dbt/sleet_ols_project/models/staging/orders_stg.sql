
{{config(
    materialized='view',
    schema='landing'
)}}
SELECT orderid order_id,
      orderdate order_date,
      customerid customer_id,
      employeeid employee_id,
      storeid store_id,
      CASE 
            WHEN status = '01' THEN 'In Progress'
            WHEN status = '02' THEN 'Completed'
            WHEN status = '03' THEN 'Canceled'
      ELSE NULL
      END AS StatusDesc,
      status statuscd,
      updated_at
 FROM 
      {{ source('landing', 'orders') }}

