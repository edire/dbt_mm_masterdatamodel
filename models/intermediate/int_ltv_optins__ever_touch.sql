
with optins as (
  select p.id_optin
    , p.email
    , p.dt as optin_date
  from {{ ref('fct_optins') }} p
where p.is_test = false
)

select s.id_optin
  , floor(date_diff(t.transaction_date, s.optin_date, day) / 7) as wob
  , sum(t.gross_amount) as amt_ever_touch
  , sum(exp(-date_diff(t.transaction_date, s.optin_date, day) / 42) * t.gross_amount) as amt_time_decay
  , sum(case when date_diff(t.transaction_date, s.optin_date, day) <= 10 then t.gross_amount
    else exp(-(date_diff(t.transaction_date, s.optin_date, day) - 10) / 42) * t.gross_amount
    end) as amt_lag_time_decay
from optins s
  join {{ ref('fct_orders') }} d
    on s.email = d.email
    and cast(d.order_date as date) >= cast(s.optin_date as date)
  join {{ ref('fct_transactions') }} t
    on d.id_order = t.id_order
group by 1, 2