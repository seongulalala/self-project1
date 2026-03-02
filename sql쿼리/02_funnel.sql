# session_start / view_item / begin_checkout / purchase
with base as(
  select 
    user_pseudo_id,
    event_name,
    event_timestamp,
    datetime(timestamp_micros(event_timestamp),'America/Los_Angeles') as event_datetime,
    event_date,
    max(if(event.key = 'ga_session_id', event.value.int_value,null)) as ga_session_id
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`, unnest(event_params) as event
  WHERE REGEXP_CONTAINS(_TABLE_SUFFIX, r'^\d{8}$')
  group by 
    user_pseudo_id,
    event_name,
    event_timestamp,
    event_date

)
-- select * from base limit 100
, first_session as (
  select *
  from base
  where 
    concat(user_pseudo_id, '-', cast(ga_session_id as string)) in 
    (
      select distinct concat(user_pseudo_id, '-', cast(ga_session_id as string))
      from base
      where event_name = 'first_visit'
    )
)

, funnel_session as (
  select
    user_pseudo_id,
    ga_session_id,
    max(if(event_name = 'session_start', 1, 0)) as has_session_start,
    max(if(event_name = 'view_item', 1, 0)) as has_view_item,
    max(if(event_name = 'begin_checkout', 1, 0)) as has_begin_checkout,
    max(if(event_name = 'purchase', 1, 0)) as has_purchase

  from first_session
  where event_name in ('session_start','view_item','begin_checkout','purchase')
  group by user_pseudo_id, ga_session_id
)

-- select * from funnel_session limit 100

, funnel_result as (
  select 1 as step, 'session_start' as step_name, countif(has_session_start = 1) as cnt from funnel_session
  union all
  select 2, 'view_item', countif(has_session_start=1 and has_view_item=1) from funnel_session
  union all
  select 3, 'begin_checkout', countif(has_session_start=1 and has_view_item=1 and has_begin_checkout=1) from funnel_session
  union all
  select 4, 'purchase', countif(has_session_start=1 and has_view_item=1 and has_begin_checkout=1 and has_purchase=1) from funnel_session
)

select 
  step_name, 
  cnt,
  round(safe_divide(cnt, max(cnt) over()), 3) as total_con_rate,
  round(safe_divide(cnt, lag(cnt) over(order by step)), 3) as lag_con_rate
from funnel_result 
order by step;
