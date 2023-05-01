
select b.id_order
    , b.email
    , b.product
    , b.order_date
    , b.order_amount
    , b.tracking_orders_id
    , b.hubspot_deal_id
    , b.hubspot_rep_id
    , b.id_transactions
from {{ ref('int_orders_base') }} b

union all

select b.id_order
    , b.email
    , b.product
    , b.order_date
    , null as order_amount
    , null as tracking_orders_id
    , null as hubspot_deal_id
    , null as hubspot_rep_id
    , b.id_transactions
from {{ ref('int_orders_missing') }} b