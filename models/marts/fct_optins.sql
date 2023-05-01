
select dt
    , email
    , funnel_id
    , funnel_step_id
    , ip
    , optin_ip
    , domain_userid
    , utm_source
    , utm_medium
    , utm_content
    , utm_campaign
    , utm_term
    , source_id
    , source_desc
from {{ ref('int_optins_agg') }}
qualify row_number() over (partition by email, funnel_id order by dt) = 1