WITH home_sessions AS (
  SELECT DISTINCT
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210102`
  WHERE event_name = 'page_view'
    AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') = 'Home'
    AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') IS NOT NULL
    AND event_date = '20210102'
),
checkout_sessions AS (
  SELECT DISTINCT
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210102`
  WHERE event_name = 'page_view'
    AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') = 'Checkout Confirmation'
    AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') IS NOT NULL
    AND event_date = '20210102'
)
SELECT 
  (SELECT COUNT(*) FROM (
    SELECT user_pseudo_id, session_id FROM home_sessions
    INTERSECT DISTINCT
    SELECT user_pseudo_id, session_id FROM checkout_sessions
  )) * 100.0 / (SELECT COUNT(*) FROM home_sessions) AS conversion_rate_percentage