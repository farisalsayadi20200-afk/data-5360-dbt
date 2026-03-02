{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
    )
}}

SELECT Customer_ID as Customer_Key, Customer_ID, First_Name, Last_Name, Email, Phone_Number, State
FROM {{ source('oliver_landing', 'customer') }}