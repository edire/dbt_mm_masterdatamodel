
with base as (
    select k.email
        , k.orig_email
        , k.dt as dt_captured
        , k.funnel_id as funnel_id_captured
        , k.source_desc as source_captured
        , k.source_id as source_id_captured
        , row_number() over (partition by ifnull(k.email, k.orig_email) order by k.dt asc) as rownum
    from {{ ref('int_contacts_combined') }} k
    where ifnull(k.email, k.orig_email) is not null
)
, first_instance as (
    select k.email
        , k.orig_email
        , k.dt_captured
        , k.funnel_id_captured
        , k.source_captured
        , k.source_id_captured
    from base k
    where k.rownum = 1
)
, last_address as (
    select a.email
        , a.orig_email
        , a.address_1
        , a.address_2
        , a.city
        , a.state
        , a.zip
        , a.country
    from {{ ref('dim_contacts_addresses') }} a
    where a.recency = 1
)
, last_phone as (
    select a.email
        , a.orig_email
        , a.phone
        , a.orig_phone
    from {{ ref('dim_contacts_phones') }} a
    where a.recency = 1
)
, last_name as (
    select a.email
        , a.orig_email
        , a.first_name
        , a.last_name
    from {{ ref('dim_contacts_names') }} a
    where a.recency = 1
)

select b.email
    , b.orig_email
    , n.first_name
    , n.last_name
    , b.dt_captured
    , b.funnel_id_captured
    , b.source_captured
    , b.source_id_captured
    , p.phone
    , p.orig_phone
    , a.address_1
    , a.address_2
    , a.city
    , a.state
    , a.zip
    , a.country
from first_instance b
    left join last_address a
        on ifnull(b.email, b.orig_email) = ifnull(a.email, a.orig_email)
    left join last_phone p
        on ifnull(b.email, b.orig_email) = ifnull(p.email, p.orig_email)
    left join last_name n
        on ifnull(b.email, b.orig_email) = ifnull(n.email, n.orig_email)