{{ config(
    materialized = 'view'
) }}

with source as (

    select * 
    from {{ source('ecoessentials_landing', 'marketing_emails') }}

),

cleaned as (

    select
        -- Clean IDs
        TRY_TO_NUMBER(NULLIF(emailid, 'NULL'))      as email_id,
        TRY_TO_NUMBER(NULLIF(campaignid, 'NULL'))   as campaign_id,
        TRY_TO_NUMBER(NULLIF(customerid, 'NULL'))   as customer_id,

        -- Clean text
        NULLIF(eventtype, 'NULL') as event_type,

        -- Timestamp (already a timestamp, no need for TRY_CAST)
        eventtimestamp as event_timestamp,

        -- Convert timestamp → date safely
        TO_DATE(eventtimestamp) as event_date

    from source

)

select * from cleaned