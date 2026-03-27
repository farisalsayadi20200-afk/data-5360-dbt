{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['eventtype']) }} as eventtype_key,
    eventtype
FROM {{ source('ecoessentials_landing', 'marketing_emails') }}