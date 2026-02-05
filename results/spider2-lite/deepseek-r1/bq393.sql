WITH user_activity AS (
  SELECT DISTINCT
    `by` AS user,
    DATE_TRUNC(DATE(timestamp), MONTH) AS activity_month
  FROM `bigquery-public-data.hacker_news.full`
  WHERE timestamp <= TIMESTAMP('2024-09-10')
    AND `by` IS NOT NULL
),
user_stats AS (
  SELECT
    user,
    MIN(activity_month) AS first_month,
    MAX(activity_month) AS last_month,
    COUNT(DISTINCT activity_month) AS active_months_count
  FROM user_activity
  GROUP BY user
),
user_gap_check AS (
  SELECT
    user,
    first_month,
    last_month,
    active_months_count,
    DATE_DIFF(last_month, first_month, MONTH) + 1 AS total_months_in_span
  FROM user_stats
  WHERE last_month < DATE('2024-09-01')
)
SELECT
  user AS user_id,
  DATE_DIFF(DATE_ADD(last_month, INTERVAL 1 MONTH), first_month, MONTH) AS month_number
FROM user_gap_check
WHERE total_months_in_span > active_months_count
ORDER BY month_number DESC, user_id ASC
LIMIT 1