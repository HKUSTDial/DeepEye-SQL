WITH all_sessions AS (
  SELECT
    fullVisitorId,
    `date`,
    visitStartTime,
    device.deviceCategory,
    IFNULL(totals.transactions, 0) > 0 AS has_transaction
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
),
first_visit AS (
  SELECT
    fullVisitorId,
    MIN(`date`) AS first_visit_date
  FROM all_sessions
  GROUP BY fullVisitorId
),
first_transaction AS (
  SELECT
    fullVisitorId,
    `date` AS first_transaction_date,
    deviceCategory AS first_transaction_device,
    ROW_NUMBER() OVER (PARTITION BY fullVisitorId ORDER BY `date`, visitStartTime) AS rn
  FROM all_sessions
  WHERE has_transaction = TRUE
)
SELECT DISTINCT
  fv.fullVisitorId
FROM first_visit fv
INNER JOIN first_transaction ft ON fv.fullVisitorId = ft.fullVisitorId
WHERE ft.rn = 1
  AND ft.first_transaction_device = 'mobile'
  AND ft.first_transaction_date > fv.first_visit_date