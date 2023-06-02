

with trans as (
    select *
    from {{ ref('int_stripe_transactions__agg') }}
)

, un_classified as (
    select t.pk
        , o.id_order
    from trans t
        join {{ ref('int_orders__agg') }} o
            on t.email = o.email
            and t.product = o.product
            and t.transaction_date >= o.order_date
    where t.id_order is null
        and t.email is not null
        and t.product is not null
    qualify row_number() over (partition by t.pk order by o.order_date desc) = 1
)

select t.pk as id_transactions
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
    , t.charge_id
    , t.payment_intent_id
    , t.id_balance_transaction
    , t.source
    , `bbg-platform.analytics.fnEmail_IsTest`(t.email) as is_test
from trans t
    left join un_classified c
        on t.pk = c.pk