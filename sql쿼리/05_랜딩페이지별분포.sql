with base as ( 
  select
    user_pseudo_id,
    event_timestamp,
    event_name,
    max(if(ep.key = 'ga_session_id', ep.value.int_value, null)) as ga_session_id,
    max(if(ep.key = 'page_location', ep.value.string_value, null)) as page_location
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
       unnest(event_params) as ep
  where regexp_contains(_table_suffix, r'^\d{8}$')
    and event_name in ('session_start','page_view','view_item')
  group by user_pseudo_id, event_timestamp, event_name
),

# 1 랜딩 페이지 작업
session_key_tb as (
  select
    *,
    concat(user_pseudo_id, '-', cast(ga_session_id as string)) as session_key
  from base
  where ga_session_id is not null
),

landing_page_tb as (
  select
    session_key,
    page_location as landing_page
  from (
    select
      session_key,
      page_location,
      event_timestamp,
      row_number() over (partition by session_key order by event_timestamp) as rownum
    from session_key_tb
    where page_location is not null
  )
  where rownum = 1
),
# 정규화 작업 필요
landing_path_normalized as (
  select
    lp.session_key,
    case
      # '/' 작업
      when lp.landing_page is null or lp.landing_page = '' then '/'

      else (
        case
          when cleaned_path = '' then '/'
          when cleaned_path = '/' then '/'
          else regexp_replace(cleaned_path, r'/+$', '')
        end
      )
    end as normalized_landing_page

  from (
    SELECT
      session_key,
      landing_page,
      regexp_replace(
        landing_page,
        r'^https?://(www\.|shop\.)?googlemerchandisestore\.com',
        ''
      ) AS cleaned_path
    from landing_page_tb
  ) lp
)

-- select * from landing_path_normalized limit 100;
# mapping
, landing_type_mapped as (
  select
    session_key,
    normalized_landing_page as landing_path,
    case
      # HOME
      when normalized_landing_page = '/' then 'HOME'

      # CART 
      when normalized_landing_page = '/basket.html' then 'CART'

      # BRAND
      when starts_with(normalized_landing_page, '/Google+Redesign/Shop+by+Brand/') then 'BRAND'

      # CATEGORY 
      when normalized_landing_page in (
        '/Google+Redesign/Apparel',
        '/Google+Redesign/Accessories',
        '/Google+Redesign/Lifestyle',
        '/Google+Redesign/New'
      ) then 'CATEGORY'

      # PRODUCT (카테고리 아래)
      when regexp_contains(
        normalized_landing_page,
        r'^/Google\+Redesign/(Apparel|Accessories|Lifestyle|New)/[^/]+$'
      ) then 'PRODUCT'

      else 'OTHER'
    end as landing_type

  from landing_path_normalized
)
-- select landing_type, count(*) as sessions
-- from landing_type_mapped
-- group by 1
-- order by sessions desc;

# 2 view_item 전환율 / view_item 이전 탐색 정도 / view_item까지 시간 연결한 테이블 만들기
, session_ts as (
  select
    session_key,
    min(if(event_name = 'session_start', event_timestamp, null)) as session_start_ts,
    min(if(event_name = 'view_item', event_timestamp, null)) as first_view_item_ts,
    if(countif(event_name = 'view_item') > 0, 1, 0) as view_item_flag
  from session_key_tb
  group by session_key
),

pageviews_before as (
  select
    sk.session_key,
    countif(
      sk.event_name = 'page_view'
      and st.first_view_item_ts is not null
      and sk.event_timestamp < st.first_view_item_ts
    ) as pageviews_before_view_item
  from session_key_tb sk
  join session_ts st
    on sk.session_key = st.session_key
  group by sk.session_key
),

session_summary as (
  select
    st.session_key,
    lt.landing_type,
    st.view_item_flag,
    pb.pageviews_before_view_item,
    safe_divide(st.first_view_item_ts - st.session_start_ts, 1000000) as time_to_view_item_sec
  from session_ts st
  left join pageviews_before pb
    on st.session_key = pb.session_key
  left join landing_type_mapped lt
    on st.session_key = lt.session_key
)

# view_item 전환율
-- select
--   landing_type,
--   count(*) as sessions,
--   avg(view_item_flag) as view_item_rate
-- from session_summary
-- group by landing_type
-- order by sessions desc;

# view_item 이전 탐색 정도
-- select
--   landing_type,
--   avg(pageviews_before_view_item) as avg_pageviews
-- from session_summary
-- where view_item_flag = 1
-- group by landing_type;

# view_item까지 시간
-- select
--   landing_type,
--   approx_quantiles(time_to_view_item_sec, 100)[offset(50)] as median_time_sec
-- from session_summary
-- where view_item_flag = 1
-- group by landing_type;
select
  landing_type,

  count(*) as sessions,

  avg(view_item_flag) as view_item_rate,

  avg(
    case when view_item_flag = 1
         then pageviews_before_view_item
    end
  ) as avg_pageviews_before_view_item,

  approx_quantiles(
    case when view_item_flag = 1
         then time_to_view_item_sec
    end,
    100
  )[offset(50)] as median_time_to_view_item_sec

from session_summary
group by landing_type
order by sessions desc;
