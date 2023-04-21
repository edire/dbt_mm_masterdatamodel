
{# {{
  config(
    materialized = 'incremental',
    unique_key = ['email', 'source_desc', 'dt', 'source_id']
    )
}} #}

with combined as (

    select k.email
        , k.orig_email
        , k.first_name
        , k.last_name
        , k.phone
        , k.orig_phone
        , k.address_1
        , k.address_2
        , k.city
        , k.state
        , k.zip
        , k.country
        , k.funnel_id
        , cast(k.dt as datetime) as dt
        , k.source_desc
        , cast(k.source_id as string) as source_id
    from {{ source('analytics_stage', 'fct_historical_optins') }} k

    union all

    select k.email
        , k.orig_email
        , k.first_name
        , k.last_name
        , k.phone
        , k.orig_phone
        , k.address_1
        , cast(null as string) as address_2
        , k.city
        , k.state
        , k.zip
        , k.country
        , k.funnel_id
        , cast(k.dt as datetime) as dt
        , k.source_desc
        , cast(k.source_id as string) as source_id
    from {{ source('analytics_stage', 'fct_historical_orders') }} k

    union all

    select k.email
        , k.orig_email
        , k.first_name
        , k.last_name
        , k.phone
        , k.orig_phone
        , k.address_1
        , k.address_2
        , k.city
        , k.state
        , k.zip
        , k.country
        , cast(null as string) as funnel_id
        , cast(k.dt as datetime) as dt
        , k.source_desc
        , cast(k.id as string) as source_id
    from {{ source('analytics_stage', 'stg_cms__customers') }} k

    union all

    select k.email
        , k.orig_email
        , k.first_name
        , k.last_name
        , k.phone
        , k.orig_phone
        , k.address_1
        , cast(null as string) as address_2
        , k.city
        , k.state
        , k.zip
        , k.country
        , k.funnel_id
        , cast(k.dt as datetime) as dt
        , k.source_desc
        , cast(k.source_id as string) as source_id
    from {{ ref('stg_kbb_evergreen__optins') }} k

    union all

    select k.email
        , k.orig_email
        , k.first_name
        , k.last_name
        , k.phone
        , k.orig_phone
        , k.address_1
        , cast(null as string) as address_2
        , k.city
        , k.state
        , k.zip
        , k.country
        , k.funnel_id
        , cast(k.dt as datetime) as dt
        , k.source_desc
        , cast(k.source_id as string) as source_id
    from {{ ref('stg_kbb_evergreen__orders') }} k

    union all

    select k.email
        , k.orig_email
        , k.first_name
        , k.last_name
        , k.phone
        , k.orig_phone
        , k.address_1
        , cast(null as string) as address_2
        , k.city
        , k.state
        , k.zip
        , k.country
        , cast(null as string) as funnel_id
        , cast(k.dt as datetime) as dt
        , k.source_desc
        , cast(k.source_id as string) as source_id
    from {{ ref('stg_hubspot__contacts') }} k

    union all

    select k.email
        , k.orig_email
        , k.first_name
        , k.last_name
        , k.phone
        , k.orig_phone
        , k.address_1
        , k.address_2
        , k.city
        , k.state
        , k.zip
        , k.country
        , cast(null as string) as funnel_id
        , cast(k.dt as datetime) as dt
        , k.source_desc
        , cast(k.source_id as string) as source_id
    from {{ ref('stg_stripe_mastermind__customers') }} k

    union all

    select k.email
        , k.orig_email
        , k.first_name
        , k.last_name
        , k.phone
        , k.orig_phone
        , k.address_1
        , k.address_2
        , k.city
        , k.state
        , k.zip
        , k.country
        , cast(null as string) as funnel_id
        , cast(k.dt as datetime) as dt
        , k.source_desc
        , cast(k.source_id as string) as source_id
    from {{ ref('stg_stripe_mindmint__customers') }} k
    
)

select c.*
from combined c
{# {% if is_incremental() %}
  where c.dt >= date_add(current_date, interval -3 day)
{% endif %} #}