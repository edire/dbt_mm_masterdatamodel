
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
        , 'kbb_evergreen.tracking_optins' as source_desc
        , cast(k.id_tracking_optins as string) as source_id
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
        , 'kbb_evergreen.tracking_orders' as source_desc
        , cast(k.id_tracking_orders as string) as source_id
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
        , 'hubspot.contact' as source_desc
        , cast(k.id_contact as string) as source_id
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
        , 'stripe_mastermind.customer' as source_desc
        , cast(k.id_customer as string) as source_id
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
        , 'stripe_mindmint.customer' as source_desc
        , cast(k.id_customer as string) as source_id
    from {{ ref('stg_stripe_mindmint__customers') }} k
    
)

select c.*
from combined c