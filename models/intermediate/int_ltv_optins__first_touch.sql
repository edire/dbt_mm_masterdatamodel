
with optins as (
  select p.id_optin
    , p.email
    , c.dt_captured
  from {{ ref('fct_optins') }} p
    join {{ ref('dim_contacts') }} c
        on c.email = p.email
        and c.funnel_id_captured = p.funnel_id
where p.is_test = false
)

select s.id_optin
  , floor(date_diff(t.transaction_date, s.dt_captured, day) / 7) as wob
  , sum(t.gross_amount) as amt_first_touch
from optins s
  join {{ ref('fct_orders') }} d
    on s.email = d.email
    and cast(d.order_date as date) >= cast(s.dt_captured as date)
  join {{ ref('fct_transactions') }} t
    on d.id_order = t.id_order
group by 1, 2