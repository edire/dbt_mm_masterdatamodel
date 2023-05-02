
{{
  config(
    materialized = 'view',
    )
}}

select a.id_order
    , a.cancelled_date as status_change_date
    , 'CANCELLED' as order_status
from {{ ref('int_orders_agg') }} a
where a.cancelled_date is not null