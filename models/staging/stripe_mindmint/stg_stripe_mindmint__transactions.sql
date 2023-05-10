
with charge as (
    select c.id
        , c.payment_intent_id
        , replace(json_extract(metadata, '$.deal_id'), '"', '') as deal_id
        , replace(json_extract(metadata, '$.product_id'), '"', '') as product_id
        , replace(json_extract(metadata, '$.rep_id'), '"', '') as rep_id
        , c.description
        , c.customer_id
        , c.invoice_id
    from {{ source('stripe_mindmint', 'charge') }} c
)

, refund as (
    select r.id
        , r.charge_id
    from {{ source('stripe_mindmint', 'refund') }} r
)

, dispute as (
    select d.id
        , d.charge_id
    from {{ source('stripe_mindmint', 'dispute') }} d
)

, invoice as (
    select i.id
        , i.payment_intent_id
        , subscription_id
    from {{ source('stripe_mindmint', 'invoice') }} i
)

, transactions as (
    select b.id as id_balance_transaction
        , b.amount / 100 as gross_amount
        , -b.fee / 100 as fee_amount
        , b.net / 100 as net_amount
        , datetime(b.created, 'America/Phoenix') as transaction_date
        , datetime(b.available_on, 'America/Phoenix') as posted_date
        , b.description
        , b.reporting_category as category
        , b.type as sub_category
        , b.status
        , b.source
    from {{ source('stripe_mindmint', 'balance_transaction') }} b
    where ifnull(left(b.source, 2), 'ch') IN ('ch', 're', 'du', 'py')
)

select {{ dbt_utils.generate_surrogate_key(['t.id_balance_transaction', '"mindmint"']) }} as pk
    , t.id_balance_transaction
    , "mindmint" as source
    , t.gross_amount
    , t.fee_amount
    , t.net_amount
    , t.transaction_date
    , t.posted_date
    , t.description
    , t.category
    , t.sub_category
    , t.status
    , cs.id as stripe_customer_id
    , analytics.fnEmail(cs.email) as email
    , cs.email as orig_email
    , i.subscription_id
    , i.payment_intent_id
    , IFNULL(c.id, c2.id) as charge_id
    , IFNULL(NULLIF(TRIM(c.deal_id), ''), NULLIF(TRIM(c2.deal_id), '')) AS hubspot_deal_id
    , IFNULL(NULLIF(TRIM(c.product_id), ''), NULLIF(TRIM(c2.product_id), '')) AS hubspot_product_id
    , IFNULL(NULLIF(TRIM(c.rep_id), ''), NULLIF(TRIM(c2.rep_id), '')) AS hubspot_rep_id
    , IFNULL(c.description, c2.description) AS charge_description
    , pl.product_id
    , pd.name as product
    , sh.canceled_at as cancelled_date
from transactions t
    left join charge c
        on t.source = c.id
    left join refund r
        on t.source = r.id
    left join dispute d
        on t.source = d.id
    LEFT JOIN charge c2
        ON IFNULL(r.charge_id, d.charge_id) = c2.id
    LEFT JOIN {{ source('stripe_mindmint', 'customer') }} cs
        ON IFNULL(c.customer_id, c2.customer_id) = cs.id
    LEFT JOIN invoice i
        ON IFNULL(c.invoice_id, c2.invoice_id) = i.id
    left join {{ source('stripe_mindmint', 'subscription_item') }} si
        on i.subscription_id = si.subscription_id
        and si.subscription_id NOT IN ('sub_1LSqKlLYbD2uWeLiNBgjCD9F', 'sub_1LS5VrLYbD2uWeLiysxmrc1q', 'sub_1LS5fpLYbD2uWeLic7zeSL2Q', 'sub_1LRHNpLYbD2uWeLilbYSlAR3', 'sub_1LMyGLLYbD2uWeLiolKAmxe0')
    left join {{ source('stripe_mindmint', 'plan') }} pl
        on si.plan_id = pl.id
    left join {{ source('stripe_mindmint', 'product') }} pd
        on pl.product_id = pd.id
    left join {{ source('stripe_mindmint', 'subscription_history') }} sh
        on i.subscription_id = sh.id
        and sh._fivetran_active = true