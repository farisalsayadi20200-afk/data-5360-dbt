{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['emailid']) }} as email_key,
    emailid,
    emailname
FROM {{ source('ecoessentials_landing', 'marketing_emails') }}
