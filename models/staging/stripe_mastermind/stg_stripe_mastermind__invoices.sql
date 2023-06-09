

select i.id as invoice_id
    , i.subscription_id
    , i.status as invoice_status
    , datetime(i.created, 'America/Phoenix') as invoice_dt
    , datetime(ifnull(i.due_date, i.period_end), 'America/Phoenix')  as due_date
    , i.subtotal
    , i.tax
    , i.total
    , i.amount_due
    , i.amount_remaining
    , row_number() over (partition by i.subscription_id order by i.created) as invoice_number
from {{ source('stripe_mastermind', 'invoice') }} i
where i.is_deleted = false
    and i.amount_due > 0