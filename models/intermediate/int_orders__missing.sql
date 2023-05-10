
{{
  config(
    materialized = 'view',
    )
}}

select {{ dbt_utils.generate_surrogate_key(['t.email', 't.product', 't.transaction_date']) }} as id_order
    , t.pk as id_transactions
    , t.email
    , t.product
    , t.transaction_date as order_date
    , t.cancelled_date
from {{ ref('int_stripe_transactions__agg') }} t
    left join {{ ref('int_orders__base') }} o
        on t.email = o.email
        and t.product = o.product
        and t.transaction_date >= o.order_date
where t.id_order is null
    and t.email is not null
    and t.product is not null
    and o.id_order is null
qualify row_number() over (partition by t.email, t.product order by t.transaction_date) = 1