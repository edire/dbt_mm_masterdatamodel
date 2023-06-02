
with dte_start as (
    select s.pk
        , case when s.trial_start < s.created
                and s.trial_start < s.coupon_start then s.trial_start
            when s.coupon_start < s.created then s.coupon_start
            else s.created end as created_date
    from {{ ref('int_stripe_subscriptions__rollup') }} s
)

, dte_pmt_due as (
    select s.pk
        , case when s.payment_date > ifnull(s.trial_end, '1900-01-01') and s.payment_date > ifnull(s.coupon_end, '1900-01-01') then s.payment_date
            when s.is_converted = false
                and s.cancelled_date > ifnull(s.trial_end, '1900-01-01') and s.cancelled_date > ifnull(s.coupon_end, '1900-01-01')
                then s.cancelled_date
            when s.trial_end > ifnull(s.coupon_end, '1900-01-01') then s.trial_end
            when s.coupon_end > ifnull(s.trial_end, '1900-01-01') then s.coupon_end
            else coalesce(s.payment_date, s.cancelled_date, s.created) end as payment_due_date
    from {{ ref('int_stripe_subscriptions__rollup') }} s
)

select s.pk
    , s.email
    , ds.created_date
    , case when dp.payment_due_date = s.trial_end then 'trial'
        when dp.payment_due_date = s.coupon_end then 'coupon'
        when {{ datediff('ds.created_date', 'dp.payment_due_date', 'hour') }} > 24 then 'unknown'
        else null end as perp_type
    , dp.payment_due_date
    , s.cancelled_date
    , s.payment_date
    , s.is_converted
    , s.gross_amount as first_payment_amount
    , s.is_annual
    , s.funnel_id
    , s.funnel_name
    , s.cancellation_reason
    , s.is_test
    , s.status
    , s.product
from {{ ref('int_stripe_subscriptions__rollup') }} s
    left join dte_start ds
        on s.pk = ds.pk
    left join dte_pmt_due dp
        on s.pk = dp.pk