

with base as (
    {{ dbt_utils.union_relations(
        relations=[ref('stg_stripe_mastermind__transactions'), ref('stg_stripe_mindmint__transactions')]
    ) }}
)

, subs as (
    select b.pk
        , max(k.email) as tracking_orders_email
        , max(k.orig_email) as tracking_orders_orig_email
        , STRING_AGG(k.product, ', ' order by k.id_tracking_orders) AS product
        , STRING_AGG(cast(k.productid as string), ', ' order by k.id_tracking_orders) AS tracking_orders_product_id
        , STRING_AGG(cast(k.amount as string), ', ' order by k.id_tracking_orders) AS order_amount 
        , STRING_AGG(k.id_tracking_orders, ', ' order by k.id_tracking_orders) AS tracking_orders_id
    from base b
        join {{ ref('stg_kbb_evergreen__orders') }} k
            on b.subscription_id = k.transaction_id
    group by b.pk
)

, pay_intent as (
    select b.pk
        , max(k.email) as tracking_orders_email
        , max(k.orig_email) as tracking_orders_orig_email
        , STRING_AGG(k.product, ', ' order by k.id_tracking_orders) AS product
        , STRING_AGG(cast(k.productid as string), ', ' order by k.id_tracking_orders) AS tracking_orders_product_id
        , STRING_AGG(cast(k.amount as string), ', ' order by k.id_tracking_orders) AS order_amount 
        , STRING_AGG(k.id_tracking_orders, ', ' order by k.id_tracking_orders) AS tracking_orders_id
    from base b
        join {{ ref('stg_kbb_evergreen__orders') }} k
            on b.payment_intent_id = k.transaction_id
    group by b.pk
)

, hubspot as (
    select b.pk
        , analytics.fnEmail(k.property_email_address_of_contact) as hubspot_email
        , k.property_email_address_of_contact as hubspot_orig_email
        , p.property_name as product
        , ifnull(b.hubspot_product_id, k.property_product_id) as product_id
        , ifnull(cast(b.hubspot_rep_id as int), k.property_rep_id) as rep_id
    from base b
        join {{ source('hubspot', 'deal') }} k
            on cast(b.hubspot_deal_id as int) = k.deal_id
        left join {{ source('hubspot', 'product') }} p
            on ifnull(cast(b.hubspot_product_id as int), cast(k.property_product_id as int)) = p.id
)

, description_one as (
    select b.pk
        , TRIM(ARRAY_REVERSE(SPLIT(b.charge_description, 'Product: '))[OFFSET(0)]) as product
    from base b
    where b.charge_description LIKE '%Product: %'
)

, description_two as (
    select b.pk
        , ARRAY_REVERSE(SPLIT(b.charge_description, 'Products: '))[OFFSET(0)] as product
    from base b
    where b.charge_description LIKE '%Products: %'
)

select b.pk
    , b.id_balance_transaction
    , b.source
    , b.gross_amount
    , b.fee_amount
    , b.net_amount
    , b.status
    , b.description
    , b.category
    , b.sub_category
    , b.transaction_date
    , b.posted_date
    , coalesce(b.email, h.hubspot_email, s.tracking_orders_email, p.tracking_orders_email) as email
    , b.orig_email as stripe_orig_email
    , coalesce(s.tracking_orders_orig_email, p.tracking_orders_orig_email) as tracking_orders_orig_email
    , h.hubspot_orig_email
    , b.product_id as stripe_product_id
    , coalesce(s.tracking_orders_product_id, p.tracking_orders_product_id) as tracking_orders_product_id
    , h.product_id as hubspot_product_id
    , trim(coalesce(s.product, p.product, h.product, d1.product, d2.product, b.product)) as product
    , coalesce(s.order_amount, p.order_amount) as order_amount
    , coalesce(s.tracking_orders_id, p.tracking_orders_id) as tracking_orders_id
    , b.hubspot_deal_id
    , h.rep_id as hubspot_rep_id
    , b.cancelled_date
    , b.payment_intent_id
    , b.subscription_id
    , b.charge_id
    , coalesce(s.tracking_orders_id, p.tracking_orders_id, b.hubspot_deal_id, b.subscription_id) as id_order
from base b
    left join subs s
        on b.pk = s.pk
    left join pay_intent p
        on b.pk = p.pk
    left join hubspot h
        on b.pk = h.pk
    left join description_one d1
        on b.pk = d1.pk
    left join description_two d2
        on b.pk = d2.pk