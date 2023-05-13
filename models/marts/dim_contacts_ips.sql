with all_ips as (
    select k.email
        , k.orig_email
        , k.ip
        , k.activity_ip
        , k.dt
        , k.source_id
        , row_number() over (partition by ifnull(k.email, k.orig_email) order by k.dt asc, k.source_id desc) as rownum
        , k.is_test
    from {{ ref('int_contacts__combined') }} k
    where ifnull(k.email, k.orig_email) is not null
        and (k.ip is not null
            or k.activity_ip is not null)
)

select a.email
    , a.orig_email
    , a.ip
    , a.activity_ip
    , a.dt
    , row_number() over (partition by ifnull(a.email, a.orig_email) order by a.dt desc, a.source_id) as recency
    , a.is_test
from all_ips a
    left join all_ips aa
        on ifnull(a.email, a.orig_email) = ifnull(aa.email, aa.orig_email)
        and a.rownum - 1 = aa.rownum
        and coalesce(a.ip, '') = coalesce(aa.ip, '')
        and coalesce(a.activity_ip, '') = coalesce(aa.activity_ip, '')
where aa.orig_email is null