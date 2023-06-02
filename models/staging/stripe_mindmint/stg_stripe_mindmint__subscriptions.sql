

select sh.id as subscription_id
    , "mindmint" as source
    , analytics.fnEmail(cs.email) as email
	, pd.name AS product
	, datetime(sh.created, 'America/Phoenix') as created
	, datetime(sh.canceled_at, 'America/Phoenix') as cancelled_at
	, datetime(sh.trial_start, 'America/Phoenix') as trial_start
	, datetime(sh.trial_end, 'America/Phoenix') as trial_end
	, datetime(sh._fivetran_synced, 'America/Phoenix') as _fivetran_synced
	, pl.amount
	, pl.`interval` as pay_interval
	, pl.interval_count as pay_interval_count
	, pl.trial_period_days
    , datetime(sd.start, 'America/Phoenix') as coupon_start
    , datetime(sd.end, 'America/Phoenix') as coupon_end
    , cp.amount_off as coupon_amount_off
    , cp.percent_off as coupon_percent_off
    , cp.duration_in_months as coupon_months
    , analytics.fnEmail_IsTest(cs.email) as is_test
	, cs.email as orig_email
    , json_value(sh.metadata, '$.netsuite_CF_funnel_id') as funnel_id
    , json_value(sh.metadata, '$.netsuite_CF_funnel_name') as funnel_name
    , json_value(sh.metadata, '$.cancellation_reason') as cancellation_reason
    , sh.status
from {{ source('stripe_mindmint', 'subscription_history') }} sh
    left join {{ source('stripe_mindmint', 'subscription_item') }} si
        on sh.id = si.subscription_id
        and si.subscription_id NOT IN ('sub_1LSqKlLYbD2uWeLiNBgjCD9F', 'sub_1LS5VrLYbD2uWeLiysxmrc1q', 'sub_1LS5fpLYbD2uWeLic7zeSL2Q', 'sub_1LRHNpLYbD2uWeLilbYSlAR3', 'sub_1LMyGLLYbD2uWeLiolKAmxe0')
    left join {{ source('stripe_mindmint', 'plan') }} pl
        on si.plan_id = pl.id
    left join {{ source('stripe_mindmint', 'product') }} pd
        on pl.product_id = pd.id
    LEFT JOIN {{ source('stripe_mindmint', 'customer') }} cs
        ON sh.customer_id = cs.id
    left join {{ source('stripe_mindmint', 'subscription_discount') }} sd
        on sh.id = sd.subscription_id
    left join {{ source('stripe_mindmint', 'coupon') }} cp
        on sd.coupon_id = cp.id
where sh._fivetran_active = true
