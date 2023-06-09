

select c.id as charge_id
    , datetime(c.created, 'America/Phoenix') as charge_dt
    , c.status as charge_status
    , c.amount as charge_amount
    , c.invoice_id
    , row_number() over (partition by c.invoice_id order by c.created asc) as charge_number
from {{ source('stripe_mastermind', 'charge') }} c