

select b.id_order
    , b.id_transactions
    , b.email
    , b.product
    , b.transaction_date as order_date
    , b.order_amount
    , b.tracking_orders_id
    , b.hubspot_deal_id
    , b.hubspot_rep_id
from {{ ref('int_transactions_agg') }} b
where b.id_order is not null
qualify row_number() over (partition by b.id_order order by b.transaction_date asc) = 1