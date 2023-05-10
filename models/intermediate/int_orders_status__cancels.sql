
{{
  config(
    materialized = 'view',
    )
}}

select a.id_order
    , a.cancelled_date as status_change_date
    , 'CANCELLED' as order_status
from {{ ref('int_orders__agg') }} a
  join analytics.dim_products p
    on a.product = p.product
    and p.is_subscription = true
where a.cancelled_date is not null