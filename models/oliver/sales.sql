
  {{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}


SELECT
c.First_Name as customer_first_name,
c.Last_Name as customer_last_name,
d.Date_Day,
e.First_Name as employee_first_name,
e.Last_Name as employee_last_name,
p.Product_Name,
s.Store_Name,
f.Order_ID,
f.Quantity,
f.Unit_Price
FROM {{ ref('fact_sales') }} f

LEFT JOIN {{ ref('oliver_dim_customer') }} c
    ON f.Customer_Key = c.Customer_ID

LEFT JOIN {{ ref('oliver_dim_date') }} d
    ON f.date_key = d.date_key

LEFT JOIN {{ ref('oliver_dim_employee') }} e
    ON f.Employee_Key = e.Employee_Key

LEFT JOIN {{ ref('oliver_dim_product') }} p 
    ON f.Product_Key = p.Product_Key

LEFT JOIN {{ ref('oliver_dim_store') }} s
    ON f.Store_Key = s.Store_Key
