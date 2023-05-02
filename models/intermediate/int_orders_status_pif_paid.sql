
{{
  config(
    materialized = 'view',
    )
}}

select d.id_order
    , max(t.transaction_date) as status_change_date
    , 'PAID' as order_status
from {{ ref('int_orders_agg') }} d
    join {{ ref('fct_transactions') }} t
        on d.id_order = t.id_order
    join analytics.dim_products p
        on d.product = p.product
        and p.is_paymentplan = false
        and p.is_subscription = false
group by d.id_order
having sum(t.gross_amount) >= 1