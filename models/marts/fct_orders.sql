
select b.id_order
    , b.email
    , b.product
    , b.order_date
    , b.order_amount
    , coalesce(r.order_status, h.order_status, c.order_status, p1.order_status, p2.order_status, a.order_status, 'ACTIVE') as order_status
    , coalesce(
        cast(r.status_change_date as datetime)
        , cast(h.status_change_date as datetime)
        , cast(c.status_change_date as datetime)
        , cast(p1.status_change_date as datetime)
        , cast(p2.status_change_date as datetime)
        , cast(a.status_change_date as datetime)
        ) as status_change_date
    , b.tracking_orders_id
    , b.hubspot_deal_id
    , b.hubspot_rep_id
    , b.id_transactions
    , `bbg-platform.analytics.fnEmail_IsTest`(b.email) as is_test
from {{ ref('int_orders__agg') }} b
    left join {{ ref('int_orders_status__refunds') }} r
        on b.id_order = r.id_order
    left join {{ ref('int_orders_status__hubspot') }} h
        on b.id_order = h.id_order
    left join {{ ref('int_orders_status__cancels') }} c
        on b.id_order = c.id_order
    left join {{ ref('int_orders_status__paymentplans_paid') }} p1
        on b.id_order = p1.id_order
    left join {{ ref('int_orders_status__pif_paid') }} p2
        on b.id_order = p2.id_order
    left join {{ ref('int_orders_status__aged') }} a
        on b.id_order = a.id_order