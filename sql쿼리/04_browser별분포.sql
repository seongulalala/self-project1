with base as (
  select   
    device.category as category,
    device.operating_system as operating_system,
    device.web_info.browser as browser
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
)

, category_counts as(
select
  category,
  -- operating_system,
  -- browser
  count(*) as counts
from base
group by 1
)
-- select * from category_counts order by counts desc

, operating_system_counts as (
select
  -- category,
  operating_system, 
  -- browser,
  count(*) as counts
from base
group by 1
)
-- select * from operating_system_counts order by counts desc

, browser_counts as(
select
  -- category,
  -- operating_system,
  browser,
  count(*) as counts
from base
group by 1
)
select * from browser_counts order by counts desc