WITH filtered_complaint_types AS (
  SELECT complaint_type
  FROM `bigquery-public-data.new_york.311_service_requests`
  WHERE created_date >= '2011-01-01' AND created_date < '2021-01-01'
  GROUP BY complaint_type
  HAVING COUNT(*) > 3000
),
daily_totals AS (
  SELECT DATE(created_date) as complaint_date, COUNT(*) as daily_total
  FROM `bigquery-public-data.new_york.311_service_requests`
  WHERE created_date >= '2011-01-01' AND created_date < '2021-01-01'
  GROUP BY complaint_date
),
daily_type_counts AS (
  SELECT DATE(created_date) as complaint_date, complaint_type, COUNT(*) as type_count
  FROM `bigquery-public-data.new_york.311_service_requests`
  WHERE created_date >= '2011-01-01' AND created_date < '2021-01-01'
    AND complaint_type IN (SELECT complaint_type FROM filtered_complaint_types)
  GROUP BY complaint_date, complaint_type
),
daily_proportions AS (
  SELECT d.complaint_date, d.complaint_type, d.type_count / t.daily_total as proportion
  FROM daily_type_counts d
  JOIN daily_totals t ON d.complaint_date = t.complaint_date
),
wind_data AS (
  SELECT DATE(CONCAT(year, '-', mo, '-', da)) as wind_date, SAFE_CAST(wdsp AS FLOAT64) as avg_wind_speed
  FROM `bigquery-public-data.noaa_gsod.gsod*`
  WHERE _TABLE_SUFFIX BETWEEN '2011' AND '2020'
    AND stn = '744860'
    AND wdsp != '999.9'
),
combined_data AS (
  SELECT p.complaint_date, p.complaint_type, p.proportion, w.avg_wind_speed
  FROM daily_proportions p
  JOIN wind_data w ON p.complaint_date = w.wind_date
  WHERE p.proportion IS NOT NULL AND w.avg_wind_speed IS NOT NULL
),
correlations AS (
  SELECT complaint_type, ROUND(CORR(proportion, avg_wind_speed), 4) as correlation
  FROM combined_data
  GROUP BY complaint_type
  HAVING CORR(proportion, avg_wind_speed) IS NOT NULL
),
max_min AS (
  SELECT MAX(correlation) as max_corr, MIN(correlation) as min_corr
  FROM correlations
)
SELECT complaint_type, correlation
FROM correlations
WHERE correlation = (SELECT max_corr FROM max_min)
   OR correlation = (SELECT min_corr FROM max_min)
ORDER BY correlation DESC