
with all_address as (
    select k.email
        , k.orig_email
        , k.address_1
        , k.address_2
        , k.city
        , k.state
        , k.zip
        , k.country
        , k.dt
        , k.source_id
        , row_number() over (partition by ifnull(k.email, k.orig_email) order by k.dt asc, k.source_id desc) as rownum
        , k.is_test
    from {{ ref('int_contacts__combined') }} k
    where ifnull(k.email, k.orig_email) is not null
        and (k.zip is not null or k.state is not null)
)

select a.email
    , a.orig_email
    , a.address_1
    , a.address_2
    , a.city
    , a.state
    , a.zip
    , a.country
    , a.dt
    , row_number() over (partition by ifnull(a.email, a.orig_email) order by a.dt desc, a.source_id) as recency
    , a.is_test
from all_address a
    left join all_address aa
        on ifnull(a.email, a.orig_email) = ifnull(aa.email, aa.orig_email)
        and a.rownum - 1 = aa.rownum
        and IFNULL(a.address_1, '') = IFNULL(aa.address_1, '')
        and IFNULL(a.address_2, '') = IFNULL(aa.address_2, '')
        and IFNULL(a.city, '') = IFNULL(aa.city, '')
        and IFNULL(a.state, '') = IFNULL(aa.state, '')
        and IFNULL(a.zip, '') = IFNULL(aa.zip, '')
        and IFNULL(a.country, '') = IFNULL(aa.country, '')
where aa.orig_email is null