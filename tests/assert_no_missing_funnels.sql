
{{
  config(
    error_if = '>2',
    )
}}

select distinct a.funnel_id
from {{ ref('fct_optins_activity') }} a
    left join `bbg-platform.analytics.dim_funnels` f
        on cast(a.funnel_id as int) = f.funnel_id
where f.funnel_id is null
    and a.funnel_id is not null

union distinct

select distinct d.funnel_id
from {{ ref('stg_kbb_evergreen__orders') }} d
    left join `bbg-platform.analytics.dim_funnels` f
        on cast(d.funnel_id as int) = f.funnel_id
where f.funnel_id is null
    and d.funnel_id is not null