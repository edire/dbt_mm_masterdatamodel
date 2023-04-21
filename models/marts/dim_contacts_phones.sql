with all_phones as (
    select k.email
        , k.orig_email
        , k.phone
        , k.orig_phone
        , k.dt
        , row_number() over (partition by ifnull(k.email, k.orig_email) order by k.dt asc) as rownum
    from {{ ref('int_contacts_combined') }} k
    where ifnull(k.email, k.orig_email) is not null
        and k.orig_phone is not null
)

select a.email
    , a.orig_email
    , a.phone
    , a.orig_phone
    , a.dt
    , row_number() over (partition by ifnull(a.email, a.orig_email) order by a.dt desc) as recency
from all_phones a
    left join all_phones aa
        on ifnull(a.email, a.orig_email) = ifnull(aa.email, aa.orig_email)
        and a.rownum - 1 = aa.rownum
        and coalesce(a.phone, a.orig_phone, '') = coalesce(aa.phone, aa.orig_phone, '')
where aa.orig_email is null