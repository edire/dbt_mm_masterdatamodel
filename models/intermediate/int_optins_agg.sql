
{# {{
  config(
    materialized = 'incremental'
    )
}} #}

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
    {# , cast(null as timestamp) as _fivetran_synced #}
from {{ source('analytics_stage', 'fct_historical_optins') }}

union all

select p.dt
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
    , p.id_tracking_optins as source_id
    , 'kbb_evergreen_tracking_optins' as source_desc
    {# , p._fivetran_synced #}
from {{ ref('stg_kbb_evergreen__optins') }} p
{# where true
{% if is_incremental() %}
  and p._fivetran_synced > coalesce((select max(_fivetran_synced) from {{ this }}), '1900-01-01')
{% endif %} #}