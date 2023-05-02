
select FARM_FINGERPRINT(concat(t.id_balance_transaction, 'stripe_mindmint_balance_transaction')) as id_transactions
    , t.id_balance_transaction as source_id
    , 'stripe_mindmint_balance_transaction' as source
    , t.gross_amount
    , t.fee_amount
    , t.net_amount
    , t.status
    , t.description
    , t.category
    , t.sub_category
    , t.transaction_date
    , t.posted_date
    , t.email
    , t.stripe_orig_email
    , t.tracking_orders_orig_email
    , t.hubspot_orig_email
    , t.stripe_product_id
    , t.tracking_orders_product_id
    , t.hubspot_product_id
    , t.product
    , t.order_amount
    , t.tracking_orders_id
    , t.hubspot_deal_id
    , coalesce(t.tracking_orders_id, t.hubspot_deal_id) as id_order
    , t.hubspot_rep_id
    , t.cancelled_date
from {{ ref('int_transactions_mindmint') }} t

union all

select FARM_FINGERPRINT(concat(t.id_balance_transaction, 'stripe_mastermind_balance_transaction')) as id_transactions
    , t.id_balance_transaction as source_id
    , 'stripe_mastermind_balance_transaction' as source
    , t.gross_amount
    , t.fee_amount
    , t.net_amount
    , t.status
    , t.description
    , t.category
    , t.sub_category
    , t.transaction_date
    , t.posted_date
    , t.email
    , t.stripe_orig_email
    , t.tracking_orders_orig_email
    , null as hubspot_orig_email
    , t.stripe_product_id
    , t.tracking_orders_product_id
    , null as hubspot_product_id
    , t.product
    , t.order_amount
    , t.tracking_orders_id
    , null as hubspot_deal_id
    , t.tracking_orders_id as order_id
    , null as hubspot_rep_id
    , t.cancelled_date
from {{ ref('int_transactions_mastermind') }} t