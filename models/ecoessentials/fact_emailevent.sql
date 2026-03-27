{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['EMAILID']) }} as email_key,
    {{ dbt_utils.generate_surrogate_key(['CAMPAIGNID']) }} as campaign_key,
    {{ dbt_utils.generate_surrogate_key(['CUSTOMERID']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['EVENTTYPE']) }} as eventtype_key,
    {{ dbt_utils.generate_surrogate_key(['cast(EVENTTIMESTAMP as date)']) }}as date_key,
    1 as event_count
from {{ source('ecoessentials_landing', 'marketing_emails') }}