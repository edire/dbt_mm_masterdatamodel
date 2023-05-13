
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
    , `bbg-platform.analytics.fnEmail_IsTest`(email) as is_test
from {{ ref('int_optins__agg') }}
qualify row_number() over (partition by email, funnel_id, cast(dt as date) order by dt) = 1