{% macro compute_profit (table_name) %}


SELECT 
    sales_date,
    SUM(quantity_sold * unit_sell_price) AS total_revenue,
    SUM(quantity_sold * unit_purchase_cost) AS total_cost,
    SUM(quantity_sold * (unit_sell_price - unit_purchase_cost)) AS total_profit
FROM 
    {{ source ('training', table_name) }}
GROUP BY 
    sales_date
{% endmacro %}