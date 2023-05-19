
{{
  config(
    materialized = 'view',
    )
}}

select d.id_order
    , date_add(max(t.transaction_date), interval ifnull(p.aging_allowance, 45) day) as status_change_date
    , 'AGED' as order_status
from {{ ref('int_orders__agg') }} d
    join {{ ref('fct_transactions') }} t
        on d.id_order = t.id_order
    left join analytics.dim_products p
        on d.product = p.product
group by d.id_order
  , p.aging_allowance
having max(t.transaction_date) < date_add(current_date, interval -ifnull(p.aging_allowance, 45) day)