{{ config(
    materialized = 'table',
    schema = 'dw_ecoessentials'
) }}

select
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} as order_key,
    {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['ca.campaign_id']) }} as campaign_key,
    {{ dbt_utils.generate_surrogate_key(['d.date_day']) }} as date_key,
    o.order_id,
    ol.quantity,
    pr.price,
    ol.discount,
    ol.price_after_discount
from {{ source('ecoessentials_landing', 'order')}} o 
inner join {{ source('ecoessentials_landing', 'order_line')}} ol on o.order_id = ol.order_id
inner join {{ ref('dim_ecocustomer') }} c on c.customer_id = o.customer_id
inner join {{ ref('dim_ecodate') }} d on d.date_day = cast(o.order_timestamp as date)
inner join {{ ref('dim_product')}} p on p.product_id = ol.product_id
inner join {{ ref('dim_campaign') }} ca on ca.campaign_id = ol.campaign_id
inner join {{ source('ecoessentials_landing', 'product') }} pr on pr.product_id = ol.product_id