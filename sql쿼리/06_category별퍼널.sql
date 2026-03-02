with landing_sessions as (
  select session_key, landing_type
  from `inflearn-bigquery-484209.advanced.v_funnel_base_landing`
  where landing_type in ('HOME','CATEGORY')
),

base as (
  select
    concat(user_pseudo_id, '-', cast(max(if(event.key='ga_session_id', event.value.int_value, null)) as string)) as session_key,
    event_timestamp,
    event_name,
    max(if(event.key='page_location', event.value.string_value, null)) as page_location
  from `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
       unnest(event_params) event
  where regexp_contains(_table_suffix, r'^\d{8}$')
    and event_name in ('page_view','view_item')
  group by user_pseudo_id, event_timestamp, event_name
),

pageviews as (
  select
    b.session_key,
    b.event_timestamp,
    b.page_location
  from base b
  join landing_sessions ls
    on b.session_key = ls.session_key
  where b.event_name = 'page_view'
),

ordered as (
  select
    session_key,
    page_location,
    row_number() over(partition by session_key order by event_timestamp) as step
  from pageviews
),

-- p1/p2/p3 원본 URL
p_steps as (
  select
    session_key,
    max(case when step=1 then page_location end) as p1_url,
    max(case when step=2 then page_location end) as p2_url,
    max(case when step=3 then page_location end) as p3_url
  from ordered
  where step <= 3
  group by session_key
),

-- URL 정규화 domain 제거 , 끝 ,"/"" 제거
norm as (
  select
    session_key,

    -- p1
    case
      when p1_url is null or p1_url = '' then '/'
      else regexp_replace(
             regexp_replace(
               regexp_replace(p1_url, r'^https?://(www\.|shop\.)?googlemerchandisestore\.com', ''),
               r'\?.*$', ''
             ),
             r'/+$', ''
           )
    end as p1_path,

    -- p2
    case
      when p2_url is null or p2_url = '' then '/'
      else regexp_replace(
             regexp_replace(
               regexp_replace(p2_url, r'^https?://(www\.|shop\.)?googlemerchandisestore\.com', ''),
               r'\?.*$', ''
             ),
             r'/+$', ''
           )
    end as p2_path,

    -- p3
    case
      when p3_url is null or p3_url = '' then '/'
      else regexp_replace(
             regexp_replace(
               regexp_replace(p3_url, r'^https?://(www\.|shop\.)?googlemerchandisestore\.com', ''),
               r'\?.*$', ''
             ),
             r'/+$', ''
           )
    end as p3_path
  from p_steps
),

-- 매핑
typed as (
  select
    n.session_key,

    -- p1
    case
      when n.p1_path in ('','/') then 'HOME'
      when n.p1_path = '/basket.html' then 'CART'
      when starts_with(n.p1_path, '/Google+Redesign/Shop+by+Brand/') then 'BRAND'
      when n.p1_path in ('/Google+Redesign/Apparel','/Google+Redesign/Accessories','/Google+Redesign/Lifestyle','/Google+Redesign/New') then 'CATEGORY'
      when regexp_contains(n.p1_path, r'^/Google\+Redesign/(Apparel|Accessories|Lifestyle|New)/[^/]+$') then 'PRODUCT'
      when regexp_contains(n.p1_path, r'(asearch\.html|search)') then 'SEARCH'
      when regexp_contains(n.p1_path, r'signin\.html') then 'SIGNIN'
      else 'OTHER'
    end as p1_type,

    -- p2
    case
      when n.p2_path is null then 'EXIT'
      when n.p2_path in ('','/') then 'HOME'
      when n.p2_path = '/basket.html' then 'CART'
      when starts_with(n.p2_path, '/Google+Redesign/Shop+by+Brand/') then 'BRAND'
      when n.p2_path in ('/Google+Redesign/Apparel','/Google+Redesign/Accessories','/Google+Redesign/Lifestyle','/Google+Redesign/New') then 'CATEGORY'
      when regexp_contains(n.p2_path, r'^/Google\+Redesign/(Apparel|Accessories|Lifestyle|New)/[^/]+$') then 'PRODUCT'
      when regexp_contains(n.p2_path, r'(asearch\.html|search)') then 'SEARCH'
      when regexp_contains(n.p2_path, r'signin\.html') then 'SIGNIN'
      else 'OTHER'
    end as p2_type,

    -- p3
    case
      when n.p3_path is null then 'EXIT'
      when n.p3_path in ('','/') then 'HOME'
      when n.p3_path = '/basket.html' then 'CART'
      when starts_with(n.p3_path, '/Google+Redesign/Shop+by+Brand/') then 'BRAND'
      when n.p3_path in ('/Google+Redesign/Apparel','/Google+Redesign/Accessories','/Google+Redesign/Lifestyle','/Google+Redesign/New') then 'CATEGORY'
      when regexp_contains(n.p3_path, r'^/Google\+Redesign/(Apparel|Accessories|Lifestyle|New)/[^/]+$') then 'PRODUCT'
      when regexp_contains(n.p3_path, r'(asearch\.html|search)') then 'SEARCH'
      when regexp_contains(n.p3_path, r'signin\.html') then 'SIGNIN'
      else 'OTHER'
    end as p3_type
  from norm n
),

view_flag as (
  select
    session_key,
    max(case when event_name='view_item' then 1 else 0 end) as view_item_flag
  from base
  group by session_key
),

final as (
  select
    ls.landing_type,
    concat(t.p1_type,' > ',t.p2_type,' > ',t.p3_type) as path,
    v.view_item_flag
  from landing_sessions ls
  join typed t
    on ls.session_key = t.session_key
  left join view_flag v
    on ls.session_key = v.session_key
)

select
  landing_type,
  path,
  count(*) as sessions,
  round(count(*) / sum(count(*)) over(partition by landing_type), 3) as sessions_rate,
  round(avg(view_item_flag), 3) as view_item_rate
from final
group by landing_type, path
order by landing_type , sessions desc
