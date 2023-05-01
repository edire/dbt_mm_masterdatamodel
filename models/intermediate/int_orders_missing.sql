
with trans as (
    select *
    from {{ ref('int_transactions_agg') }}
)

, un_classified as (
    select t.id_transactions
        , t.email
        , t.product
        , t.transaction_date
        , row_number() over (partition by t.email, t.product order by t.transaction_date) as rownum
    from trans t
        left join {{ ref('int_orders_base') }} o
            on t.email = o.email
            and t.product = o.product
            and t.transaction_date >= o.order_date
    where t.id_order is null
        and t.email is not null
        and t.product is not null
        and o.id_order is null
)

select generate_uuid() as id_order
    , c.id_transactions
    , c.email
    , c.product
    , c.transaction_date as order_date
from un_classified c
where c.rownum = 1