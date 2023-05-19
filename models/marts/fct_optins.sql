
select {{ dbt_utils.generate_surrogate_key(['p.email', 'p.dt']) }} as pk
    , p.dt
    , p.email
    , p.funnel_id
    , p.funnel_step_id
    , p.ip
    , p.optin_ip
    , p.domain_userid
    , p.utm_source
    , p.utm_medium
    , p.utm_content
    , p.utm_campaign
    , p.utm_term
    , p.source_id
    , p.source_desc
    , `bbg-platform.analytics.fnEmail_IsTest`(p.email) as is_test
from {{ ref('int_optins__agg') }} p
qualify row_number() over (partition by p.email, p.funnel_id order by p.dt) = 1