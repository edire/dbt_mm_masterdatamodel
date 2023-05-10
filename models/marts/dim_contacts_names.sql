with all_names as (
    select k.email
        , k.orig_email
        , replace(k.first_name, '"', '') as first_name
        , replace(k.last_name, '"', '') as last_name
        , k.dt
        , k.source_id
        , row_number() over (partition by ifnull(k.email, k.orig_email) order by k.dt asc, k.source_id desc) as rownum
    from {{ ref('int_contacts__combined') }} k
    where ifnull(k.email, k.orig_email) is not null
        and (k.first_name is not null or k.last_name is not null)
)

select a.email
    , a.orig_email
    , a.first_name
    , a.last_name
    , a.dt
    , row_number() over (partition by ifnull(a.email, a.orig_email) order by a.dt desc, a.source_id) as recency
from all_names a
    left join all_names aa
        on ifnull(a.email, a.orig_email) = ifnull(aa.email, aa.orig_email)
        and a.rownum - 1 = aa.rownum
        and ifnull(a.first_name, '') = ifnull(aa.first_name, '')
        and ifnull(a.last_name, '') = ifnull(aa.last_name, '')
where aa.orig_email is null