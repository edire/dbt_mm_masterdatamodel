

with base as (
    select *
    from {{ ref('stg_stripe_mastermind__transactions') }}
)

, subs as (
    select b.id_balance_transaction
        , max(k.email) as tracking_orders_email
        , max(k.orig_email) as tracking_orders_orig_email
        , STRING_AGG(k.product, ', ' order by k.id_tracking_orders) AS product
        , STRING_AGG(cast(k.productid as string), ', ' order by k.id_tracking_orders) AS tracking_orders_product_id
        , STRING_AGG(cast(k.amount as string), ', ' order by k.id_tracking_orders) AS order_amount 
        , STRING_AGG(k.id_tracking_orders, ', ' order by k.id_tracking_orders) AS tracking_orders_id
    from base b
        join {{ ref('stg_kbb_evergreen__orders') }} k
            on b.subscription_id = k.transaction_id
    group by b.id_balance_transaction
)

, pay_intent as (
    select b.id_balance_transaction
        , max(k.email) as tracking_orders_email
        , max(k.orig_email) as tracking_orders_orig_email
        , STRING_AGG(k.product, ', ' order by k.id_tracking_orders) AS product
        , STRING_AGG(cast(k.productid as string), ', ' order by k.id_tracking_orders) AS tracking_orders_product_id
        , STRING_AGG(cast(k.amount as string), ', ' order by k.id_tracking_orders) AS order_amount 
        , STRING_AGG(k.id_tracking_orders, ', ' order by k.id_tracking_orders) AS tracking_orders_id
    from base b
        join {{ ref('stg_kbb_evergreen__orders') }} k
            on b.payment_intent_id = k.transaction_id
    group by b.id_balance_transaction
)

, description_one as (
    select b.id_balance_transaction
        , TRIM(ARRAY_REVERSE(SPLIT(b.charge_description, 'Product: '))[OFFSET(0)]) as product
    from base b
    where b.charge_description LIKE '%Product: %'
)

, description_two as (
    select b.id_balance_transaction
        , ARRAY_REVERSE(SPLIT(b.charge_description, 'Products: '))[OFFSET(0)] as product
    from base b
    where b.charge_description LIKE '%Products: %'
)

select b.id_balance_transaction
    , b.gross_amount
    , b.fee_amount
    , b.net_amount
    , b.status
    , b.description
    , b.category
    , b.sub_category
    , b.transaction_date
    , b.posted_date
    , coalesce(b.email, s.tracking_orders_email, p.tracking_orders_email) as email
    , b.orig_email as stripe_orig_email
    , coalesce(s.tracking_orders_orig_email, p.tracking_orders_orig_email) as tracking_orders_orig_email
    , b.product_id as stripe_product_id
    , coalesce(s.tracking_orders_product_id, p.tracking_orders_product_id) as tracking_orders_product_id
    , trim(coalesce(s.product, p.product, d1.product, d2.product, b.product)) as product
    , coalesce(s.order_amount, p.order_amount) as order_amount
    , coalesce(s.tracking_orders_id, p.tracking_orders_id) as tracking_orders_id
    , b.cancelled_date
from base b
    left join subs s
        on b.id_balance_transaction = s.id_balance_transaction
    left join pay_intent p
        on b.id_balance_transaction = p.id_balance_transaction
    left join description_one d1
        on b.id_balance_transaction = d1.id_balance_transaction
    left join description_two d2
        on b.id_balance_transaction = d2.id_balance_transaction