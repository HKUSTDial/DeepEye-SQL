WITH purchase_events AS (
  SELECT 
    `user_pseudo_id`,
    param.value.int_value AS ga_session_id,
    `ecommerce`.purchase_revenue_in_usd AS revenue_usd
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  LEFT JOIN UNNEST(event_params) AS param
  ON param.key = 'ga_session_id'
  WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20201130'
    AND event_name = 'purchase'
    AND `ecommerce`.purchase_revenue_in_usd IS NOT NULL
    AND param.value.int_value IS NOT NULL
),
session_totals AS (
  SELECT 
    user_pseudo_id,
    ga_session_id,
    SUM(revenue_usd) AS session_revenue
  FROM purchase_events
  GROUP BY user_pseudo_id, ga_session_id
)
SELECT 
  user_pseudo_id,
  AVG(session_revenue) AS avg_purchase_revenue_per_session
FROM session_totals
GROUP BY user_pseudo_id
HAVING COUNT(ga_session_id) > 1