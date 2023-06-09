
with base as (
    {{ dbt_utils.union_relations(
        relations=[ref('stg_stripe_mastermind__subscriptions'), ref('stg_stripe_mindmint__subscriptions')]
    ) }}
)

, invoices as (
    {{ dbt_utils.union_relations(
        relations=[ref('stg_stripe_mastermind__invoices'), ref('stg_stripe_mindmint__invoices')]
    ) }}
)

, charges as (
    {{ dbt_utils.union_relations(
        relations=[ref('stg_stripe_mastermind__charges'), ref('stg_stripe_mindmint__charges')]
    ) }}
)

, base_filtered as (
    select *
        , cast(created as date) as created_date
    from base b
    where b.product in (
            'Mastermind 14-Day Trial',
            'Mastermind.com Monthly Membership - Startup Plan',
            'Mastermind.com Monthly Membership - Elite Plan',
            'Mastermind.com Yearly Membership  - Launch+ Plan',
            'Mastermind Monthly Purchase',
            'Mastermind 4 Month Purchase',
            'Mastermind Annual Purchase',
            'Roadtrip - Annual Mastermind Upgrade'
        )
)

, first_transaction as (
    select b.subscription_id
        , i.due_date as first_payment_due_date
        , case when i.invoice_status = 'paid' then true else false end as is_paid
        , c.charge_status as first_charge_status
    from base_filtered b
        join invoices i
            on b.subscription_id = i.subscription_id
            and invoice_number = 1
        left join charges c
            on i.invoice_id = c.invoice_id
            and c.charge_number = 1
)

select b.*
    , ft.first_payment_due_date
    , ft.is_paid
    , ft.first_charge_status
    , {{ dbt_utils.generate_surrogate_key(['b.email', 'b.created_date']) }} as pk
from base_filtered b
    left join first_transaction ft
        on b.subscription_id = ft.subscription_id
where b.subscription_id != 'sub_HwJ3jA7y9fXjaq'