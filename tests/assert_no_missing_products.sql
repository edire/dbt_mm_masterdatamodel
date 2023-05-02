
{{
  config(
    error_if = '>5',
    )
}}

select t.source_id
from {{ ref('fct_transactions') }} t
    left join analytics.dim_products p
        on t.product = p.product
where t.product is not null
    and p.product is null