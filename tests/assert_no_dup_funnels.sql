
select p.funnel_id
from `bbg-platform.analytics.dim_funnels` p
group by 1
having count(*) > 1