
{{
  config(
    materialized = 'view',
    )
}}

select d.id_order
    , max(t.transaction_date) as status_change_date
    , 'PAID' as order_status
from {{ ref('int_orders__agg') }} d
    join {{ ref('fct_transactions') }} t
        on d.id_order = t.id_order
    join analytics.dim_products p
        on d.product = p.product
        and p.is_paymentplan = true
        and p.contractual_amount > 0
group by d.id_order
    , p.contractual_amount
having sum(t.gross_amount) >= 0.95 * p.contractual_amount