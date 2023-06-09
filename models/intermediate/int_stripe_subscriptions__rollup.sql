
with base as (
    select *
    from {{ ref('int_stripe_subscriptions__agg') }} s
)

, first_instance as (
    select *
    from base s
    qualify row_number() over (partition by s.pk order by s.created asc) = 1
)

, trial_dates as (
    select s.pk
        , s.trial_start
        , s.trial_end
    from base s
    where trial_start is not null
        and trial_end is not null
    qualify row_number() over (partition by s.pk order by s.created desc) = 1
)

, trial_period as (
    select s.pk
        , s.created as trial_start
        , {{ dateadd('day', 's.trial_period_days', 's.created') }} as trial_end
    from base s
    where s.trial_period_days is not null
    qualify row_number() over (partition by s.pk order by s.created desc) = 1
)

, trial_product as (
    select s.pk
        , s.created as trial_start
        , {{ dateadd('day', 's.trial_period_days', 's.created') }} as trial_end
    from base s
    where s.product = 'Mastermind 14-Day Trial'
    qualify row_number() over (partition by s.pk order by s.created desc) = 1
)

, coupons as (
    select s.pk
        , s.coupon_start
        , s.coupon_end
    from base s
    where s.coupon_start is not null
    qualify row_number() over (partition by s.pk order by s.created desc) = 1
)

, payment as (
    select s.pk
        , s.first_payment_due_date
        , s.is_paid
        , s.first_charge_status
        , case when s.product in (
                'Mastermind.com Yearly Membership  - Launch+ Plan',
                'Mastermind Annual Purchase',
                'Roadtrip - Annual Mastermind Upgrade'
            ) then true else false end as is_annual
    from base s
    where s.product not like '%Trial%'
        and (s.is_paid is not null
            or s.first_payment_due_date is not null)
    qualify row_number() over (partition by s.pk order by s.created desc) = 1
)

, cancels as (
    select s.pk
        , max(s.cancelled_at) as cancelled_date
        , max(s.cancellation_reason) as cancellation_reason
    from base s
    group by 1
)

, non_cancels as (
    select distinct s.pk
    from base s
    where s.cancelled_at is null
)

, trials_combined as (
    select s.pk
        , s.email
        , s.created
        , coalesce(td.trial_start, tp.trial_start) as trial_start
        , coalesce(td.trial_end, tp.trial_end) as trial_end
    from first_instance s
        left join trial_dates td
            on s.pk = td.pk
        left join trial_period tp
            on s.pk = tp.pk
        left join trial_product tt
            on s.pk = tt.pk
    where (td.trial_start is not null
        or tp.trial_start is not null
        or tt.trial_start is not null)
)

, coupons_combined as (
    select s.pk
        , s.email
        , s.created
        , cp.coupon_start
        , cp.coupon_end
    from first_instance s
        left join coupons cp
            on s.pk = cp.pk
    where cp.coupon_start is not null
)

, all_aggs as (
    select s.pk
        , string_agg(s.status, ', ' order by s.created asc) as status
        , string_agg(s.product, ', ' order by s.created asc) as product
    from base s
    group by 1
)

select s.pk
    , s.email
    , s.created
    , case when td.pk is not null then true else false end as has_trial
    , td.trial_start
    , td.trial_end
    , case when cp.pk is not null then true else false end as has_coupon
    , cp.coupon_start
    , cp.coupon_end
    , pm.first_payment_due_date
    , ifnull(pm.is_paid, false) as made_first_payment
    , pm.first_charge_status
    , pm.is_annual
    , c.cancelled_date
    , s.funnel_id
    , s.funnel_name
    , c.cancellation_reason
    , aa.status
    , aa.product
    , s.is_test
from first_instance s
    left join trials_combined td
        on s.pk = td.pk
    left join coupons_combined cp
        on s.pk = cp.pk
    left join payment pm
        on s.pk = pm.pk
    left join non_cancels nc
        on s.pk = nc.pk
    left join cancels c
        on s.pk = c.pk
        and nc.pk is null
    left join all_aggs aa
        on s.pk = aa.pk