
with optins as (
  select p.pk
    , p.email
    , p.funnel_id
    , {{ datediff('p.dt', 'current_datetime()', 'day') }} as wob_alive
  from {{ ref('fct_optins') }} p
where p.is_test = false
  and p.dt >= '2022-06-01'
)

, dim_wob as (
  select *
  from unnest(generate_array(0, 1000)) as wob
)

, optin_wob as (
  select s.pk
    , s.email
    , s.funnel_id
    , w.wob
    , floor(w.wob / 4) as fwob
  from optins s
    cross join dim_wob w
  where w.wob <= s.wob_alive
)

select s.pk
  , s.email
  , s.funnel_id
  , s.wob
  , s.fwob
  , ft.gross_amount as amt_first_touch
  , lt.gross_amount as amt_last_touch
  , et.gross_amount as amt_ever_touch
  , td.gross_amount as amt_time_decay
  , ifnull(ft.gross_amount, 0) * 0.4 + ifnull(lt.gross_amount, 0) * 0.4 + ifnull(td.gross_amount, 0) * 0.2 as amt_first_last
from optin_wob s
  left join {{ ref('int_ltv_optins__first_touch') }} ft
    on s.pk = ft.pk
    and s.wob = ft.wob
  left join {{ ref('int_ltv_optins__last_touch') }} lt
    on s.pk = lt.pk
    and s.wob = lt.wob
  left join {{ ref('int_ltv_optins__ever_touch') }} et
    on s.pk = et.pk
    and s.wob = et.wob
  left join {{ ref('int_ltv_optins__time_decay') }} td
    on s.pk = td.pk
    and s.wob = td.wob
where ft.gross_amount <> 0
  or lt.gross_amount <> 0
  or et.gross_amount <> 0
  or td.gross_amount <> 0