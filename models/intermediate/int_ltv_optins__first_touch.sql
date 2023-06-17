
with optins as (
  select p.pk
    , p.email
    , c.dt_captured
  from {{ ref('fct_optins') }} p
    join {{ ref('dim_contacts') }} c
        on c.email = p.email
        and c.funnel_id_captured = p.funnel_id
where p.is_test = false
)

select s.pk
  , floor(date_diff(t.transaction_date, s.dt_captured, day) / 7) as wob
  , floor(date_diff(t.transaction_date, s.dt_captured, day) / 28) as fwob
  , sum(t.gross_amount) as gross_amount
from optins s
  join {{ ref('fct_orders') }} d
    on s.email = d.email
    and cast(d.order_date as date) >= cast(s.dt_captured as date)
  join {{ ref('fct_transactions') }} t
    on d.id_order = t.id_order
group by 1, 2, 3