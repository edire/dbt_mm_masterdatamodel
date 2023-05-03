
{{
  config(
    materialized = 'view',
    )
}}

select d.id_order
    , date_add(max(t.transaction_date), interval 13 month) as status_change_date
    , 'AGED' as order_status
from {{ ref('int_orders_agg') }} d
    join {{ ref('fct_transactions') }} t
        on d.id_order = t.id_order
group by d.id_order
having max(t.transaction_date) < date_add(current_date, interval -13 month)