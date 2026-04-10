{{ config(
    materialized='table',
    schema='dw_ecoessentials'
) }}


select
    e.email_key,
    ca.campaign_key,
    c.customer_key,
    ev.eventtype_key,
    d.date_key,
    1 as event_count
from {{ source('ecoessentials_landing', 'marketing_emails')}} m 

inner join {{ ref('dim_ecocustomer') }} c
    on c.customer_id = TRY_CAST(m.customerid AS NUMBER)

inner join {{ ref('dim_campaign') }} ca
    on ca.campaign_id = TRY_CAST(m.campaignid AS NUMBER)

inner join {{ ref('dim_email') }} e
    on e.emailid = TRY_CAST(m.emailid AS NUMBER)

inner join {{ ref('dim_eventtype') }} ev
    on ev.eventtype = m.eventtype

inner join {{ ref('dim_ecodate') }} d
    on d.date_day = DATE(m.eventtimestamp)

where m.eventtimestamp IS NOT NULL