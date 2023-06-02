
with base as (
    {{ dbt_utils.union_relations(
        relations=[ref('stg_stripe_mastermind__subscriptions'), ref('stg_stripe_mindmint__subscriptions')]
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
        , t.transaction_date
        , t.gross_amount
    from base_filtered b
        join {{ ref('int_stripe_transactions__agg') }} t
            on b.subscription_id = t.subscription_id
    where t.gross_amount > 5
    qualify row_number() over (partition by t.subscription_id order by t.transaction_date) = 1
)

select b.*
    , ft.transaction_date
    , ft.gross_amount
    , {{ dbt_utils.generate_surrogate_key(['b.email', 'b.created_date']) }} as pk
from base_filtered b
    left join first_transaction ft
        on b.subscription_id = ft.subscription_id