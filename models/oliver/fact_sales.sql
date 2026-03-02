{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

SELECT
    c.Customer_Key,
    s.Store_Key,
    d.Date_Key,
    e.Employee_Key,
    p.Product_Key,
    o.Order_ID,
    ol.Quantity,
    ol.Unit_Price
FROM {{ source('oliver_landing', 'orders') }} o
INNER JOIN {{ source('oliver_landing', 'orderline') }} ol ON o.Order_ID = ol.Order_ID
INNER JOIN {{ ref('oliver_dim_customer') }} c on c.Customer_ID = o.Customer_ID
INNER JOIN {{ ref('oliver_dim_store') }} s on s.Store_ID = o.Store_ID
INNER JOIN {{ ref('oliver_dim_date') }} d on d.Date_Day = o.Order_Date
INNER JOIN {{ ref('oliver_dim_employee') }} e on e.Employee_ID = o.Employee_ID
INNER JOIN {{ ref('oliver_dim_product') }} p on p.Product_ID = ol.Product_ID

