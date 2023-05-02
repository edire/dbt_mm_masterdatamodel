
{{
  config(
    materialized = 'view',
    )
}}

select a.id_order
    , h.timestamp as status_change_date
    , upper(nullif(trim(h.value), '')) as order_status
from {{ ref('int_orders_agg') }} a
    join {{ source('hubspot', 'deal_property_history') }} h
        on cast(a.hubspot_deal_id as int) = h.deal_id
        and h.name = 'product_status'
        and h._fivetran_active = true
        and h.value in ('Cancelled', 'Paused')