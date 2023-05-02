

with trans as (
    select *
    from {{ ref('int_transactions_agg') }}
)

, un_classified as (
    select t.id_transactions
        , o.id_order
    from trans t
        join {{ ref('fct_orders') }} o
            on t.email = o.email
            and t.product = o.product
            and t.transaction_date >= o.order_date
    where t.id_order is null
        and t.email is not null
        and t.product is not null
    qualify row_number() over (partition by t.id_transactions order by o.order_date desc) = 1
)

select t.id_transactions
    , t.gross_amount
    , t.fee_amount
    , t.net_amount
    , t.status
    , t.description
    , t.category
    , t.sub_category
    , t.transaction_date
    , t.posted_date
    , t.email
    , t.product
    , t.order_amount
    , coalesce(t.id_order, c.id_order) as id_order
    , t.hubspot_rep_id
    , t.hubspot_deal_id
    , t.tracking_orders_id
    , t.stripe_orig_email
    , t.tracking_orders_orig_email
    , t.hubspot_orig_email
    , t.stripe_product_id
    , t.tracking_orders_product_id
    , t.hubspot_product_id
    , t.source_id
    , t.source
from trans t
    left join un_classified c
        on t.id_transactions = c.id_transactions