with base as (
  select
    user_pseudo_id,
    event_name,
    event_timestamp,

    max(if(event.key = 'ga_session_id', event.value.int_value, null)) as ga_session_id,

    traffic_source.source as source,
    traffic_source.medium as medium

  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
       unnest(event_params) as event
  where regexp_contains(_table_suffix, r'^\d{8}$')
    and event_name in ('session_start', 'view_item')
  group by
    user_pseudo_id, event_name, event_timestamp, source, medium
),

session_channel as (
  select
    concat(user_pseudo_id, '-', cast(ga_session_id as string)) as session_key,
    coalesce(source, '(direct)') as source,
    coalesce(medium, '(none)') as medium
  from base
  where event_name = 'session_start'
    and ga_session_id is not null
),

session_events as (
  select distinct
    concat(user_pseudo_id, '-', cast(ga_session_id as string)) as session_key,
    event_name
  from base
  where ga_session_id is not null
),

channel_funnel as (
  select
    sc.source,
    sc.medium,
    count(distinct sc.session_key) as session_cnt,
    count(distinct if(se.event_name = 'view_item', sc.session_key, null)) as view_cnt
  from session_channel sc
  left join session_events se
    on sc.session_key = se.session_key
  group by 1, 2
),

# 출력 결과 (data deleted)존재 식별 > 해당 값 필터링
# 시각화를 위해 "shop.googlemerchandisestore.com" > Self-Referral로 변경
filtered as (
  select *,
    case when source = 'shop.googlemerchandisestore.com' then "Self-Referral"
    else source
    end as trim_source
  from channel_funnel
  where source != '(data deleted)'
    and medium != '(data deleted)'
)

select
  trim_source,
  source,
  medium,
  session_cnt,
  view_cnt,
  round(safe_divide(view_cnt, session_cnt), 3) as conversion_rate
from filtered
where session_cnt >= 100
order by conversion_rate desc;
