with base as(
select 
  distinct
    user_pseudo_id, 
    date(datetime(timestamp_micros(event_timestamp), 'America/New_York')) as event_date
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE REGEXP_CONTAINS(_TABLE_SUFFIX, r'^\d{8}$') and event_name = 'session_start' 
)

, first_week_tb as(
select 
  distinct
    user_pseudo_id,
    date_trunc(MIN(event_date) over (partition by user_pseudo_id),week(monday)) as first_week,
    date_trunc(event_date,week(monday)) as event_week
from base 
)

, first_week_diff_tb as(
  select
    user_pseudo_id,
    first_week,
    date_diff(event_week,first_week,week) as first_week_diff
  from first_week_tb
)

, user_counts as(
  select
    first_week,
    first_week_diff,
    count(distinct user_pseudo_id) as active_users
  from first_week_diff_tb
  group by all
)

, cohort as (
  SELECT
    first_week,
    active_users AS cohort_users
  FROM user_counts
  WHERE first_week_diff = 0
)

SELECT
  uc.first_week,
  uc.first_week_diff,
  uc.active_users,
  c.cohort_users,
  ROUND(SAFE_DIVIDE(uc.active_users, c.cohort_users), 2) AS retention_rate
FROM user_counts as uc
JOIN cohort as c USING (first_week)
ORDER BY first_week, first_week_diff;
