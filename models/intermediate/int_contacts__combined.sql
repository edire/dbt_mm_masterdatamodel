
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
        , k.ip
        , k.optin_ip as activity_ip
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
        , k.ip
        , k.order_ip as activity_ip
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
        , cast(null as string) as ip
        , cast(null as string) as activity_ip
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
        , k.ip
        , k.optin_ip as activity_ip
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
        , k.ip
        , k.order_ip as activity_ip
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
        , cast(null as string) as ip
        , cast(null as string) as activity_ip
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
        , cast(null as string) as ip
        , cast(null as string) as activity_ip
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
        , cast(null as string) as ip
        , cast(null as string) as activity_ip
        , cast(null as string) as funnel_id
        , cast(k.dt as datetime) as dt
        , 'stripe_mindmint.customer' as source_desc
        , cast(k.id_customer as string) as source_id
    from {{ ref('stg_stripe_mindmint__customers') }} k

    union all

    select k.email
        , k.orig_email
        , k.first_name
        , k.last_name
        , k.phone
        , k.orig_phone
        , cast(null as string) as address_1
        , cast(null as string) as address_2
        , k.city
        , k.state
        , cast(null as string) as zip
        , k.country
        , k.ip
        , null as activity_ip
        , k.webinar_name as funnel_id
        , cast(k.dt as datetime) as dt
        , 'kbb_evergreen.webinarfuel_leads' as source_desc
        , cast(k.id_webinarfuel_leads as string) as source_id
    from {{ ref('stg_kbb_evergreen__webinarfuel_leads') }} k

    union all

    select k.email
        , k.orig_email
        , k.first_name
        , k.last_name
        , k.phone
        , k.orig_phone
        , cast(null as string) as address_1
        , cast(null as string) as address_2
        , k.city
        , k.state
        , cast(null as string) as zip
        , k.country
        , k.ip
        , null as activity_ip
        , k.webinar_name as funnel_id
        , cast(k.dt as datetime) as dt
        , 'kbb_evergreen.webinarfuel_registrations' as source_desc
        , cast(k.id_webinarfuel_registrations as string) as source_id
    from {{ ref('stg_kbb_evergreen__webinarfuel_registrations') }} k
    
)

select c.*
    , `bbg-platform.analytics.fnEmail_IsTest`(c.email) as is_test
    , {{ dbt_utils.generate_surrogate_key(['c.email', 'c.dt', 'c.source_id', 'c.source_desc']) }} as pk
from combined c