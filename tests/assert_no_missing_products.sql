
{{
  config(
    error_if = '>2',
    )
}}

select distinct t.product
from {{ ref('fct_transactions') }} t
    left join `bbg-platform.analytics.dim_products` p
        on t.product = p.product
where t.product is not null
    and p.product is null