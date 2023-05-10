

{% set partitions_to_replace = [
    'current_date',
    'date_add(current_date, interval -1 day)'
] %}


{{config(
    materialized = 'incremental',
    partition_by = {
      'field': '_fivetran_synced',
      'data_type': 'date'
      },
    incremental_strategy = 'insert_overwrite',
    partitions = partitions_to_replace
)}}


{% if not is_incremental() %}

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
        , cast('1900-01-01' as date) as _fivetran_synced
    from {{ source('analytics_stage', 'fct_historical_optins') }}

    union all

{% endif %}

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
    , date(p._fivetran_synced) as _fivetran_synced
from {{ ref('stg_kbb_evergreen__optins') }} p

{% if is_incremental() %}

    where date(p._fivetran_synced) in ({{ partitions_to_replace | join(',') }})
		
{% endif %}