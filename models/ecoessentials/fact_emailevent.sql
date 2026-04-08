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
from {{ ref('stg_marketing_emails') }} m

-- Clean dimension IDs during join
inner join {{ ref('dim_email') }} e
    on TRY_TO_NUMBER(NULLIF(e.emailid, 'NULL')) = m.email_id

inner join {{ ref('dim_campaign') }} ca
    on TRY_TO_NUMBER(NULLIF(ca.campaign_id, 'NULL')) = m.campaign_id

inner join {{ ref('dim_ecocustomer') }} c
    on c.customer_id = m.customer_id

inner join {{ ref('dim_eventtype') }} ev
    on NULLIF(ev.eventtype, 'NULL') = m.event_type

inner join {{ ref('dim_ecodate') }} d
    on d.date_day = m.event_date

where m.email_id is not null
  and m.customer_id is not null
  and m.campaign_id is not null
  and m.event_type is not null