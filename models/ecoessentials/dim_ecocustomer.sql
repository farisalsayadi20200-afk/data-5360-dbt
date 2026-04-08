{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
) }}

with customer_base as (

    -- All customers from the main customer table
    select
        customer_id,
        customer_first_name,
        customer_last_name,
        customer_email,
        customer_phone,
        customer_address,
        customer_city,
        customer_state,
        customer_zip,
        customer_country
    from {{ source('ecoessentials_landing', 'customer') }}

),

email_customers as (

    -- Customers that only exist in marketing_emails
    select
        TRY_TO_NUMBER(NULLIF(customerid, 'NULL')) as customer_id,
        subscriberfirstname as customer_first_name,
        subscriberlastname  as customer_last_name,
        subscriberemail      as customer_email,
        null as customer_phone,
        null as customer_address,
        null as customer_city,
        null as customer_state,
        null as customer_zip,
        null as customer_country
    from {{ source('ecoessentials_landing', 'marketing_emails') }}

),

all_customers as (

    -- Union and de-duplicate by customer_id
    select * from customer_base
    union
    select * from email_customers

),

deduped as (

    -- In case of duplicates, keep the first (you could add more rules here)
    select *
    from (
        select
            *,
            row_number() over (partition by customer_id order by customer_id) as rn
        from all_customers
    ) t
    where rn = 1

)

select
    {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customer_key,
    customer_id,
    customer_first_name,
    customer_last_name,
    customer_email,
    customer_phone,
    customer_address,
    customer_city,
    customer_state,
    customer_zip,
    customer_country
from deduped