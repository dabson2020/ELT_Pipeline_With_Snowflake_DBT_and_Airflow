{{config(
    materialized='view',
    schema='landing'
)}}

select employeeid employee_id,
       CONCAT(firstname,' ',lastname) employee_name,
       email,
       jobtitle job_title,
       hiredate hire_date,
       managerid manager_id,
       address,
       city,
       state,
       zipcode zip_code,
       updated_at
from {{ source('landing', 'employees') }}