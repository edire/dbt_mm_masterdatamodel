

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
from {{ source('analytics_stage', 'fct_historical_optins') }}

union all

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
    , id_tracking_optins
    , 'kbb_evergreen_tracking_optins' as source_desc
from {{ ref('stg_kbb_evergreen__optins') }}