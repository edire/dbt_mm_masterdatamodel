
select p.product
from `bbg-platform.analytics.dim_products` p
group by 1
having count(*) > 1