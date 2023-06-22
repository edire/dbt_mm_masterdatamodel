
with optins as (
  select p.id_optin
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
  select s.id_optin
    , s.email
    , s.funnel_id
    , w.wob
  from optins s
    cross join dim_wob w
  where w.wob <= s.wob_alive
)

select {{ dbt_utils.generate_surrogate_key(['s.id_optin', 's.wob']) }} as id_ltv_optin
  , s.id_optin
  , s.email
  , s.funnel_id
  , s.wob
  , ft.amt_first_touch
  , lt.amt_last_touch
  , et.amt_ever_touch
  , et.amt_time_decay
  , et.amt_lag_time_decay
from optin_wob s
  left join {{ ref('int_ltv_optins__first_touch') }} ft
    on s.id_optin = ft.id_optin
    and s.wob = ft.wob
  left join {{ ref('int_ltv_optins__last_touch') }} lt
    on s.id_optin = lt.id_optin
    and s.wob = lt.wob
  left join {{ ref('int_ltv_optins__ever_touch') }} et
    on s.id_optin = et.id_optin
    and s.wob = et.wob
where ft.amt_first_touch <> 0
  or lt.amt_last_touch <> 0
  or et.amt_ever_touch <> 0
