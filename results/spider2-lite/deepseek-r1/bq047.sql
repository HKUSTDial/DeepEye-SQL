WITH daily_temps AS (
  SELECT DATE(CONCAT(year, '-', mo, '-', da)) AS date, AVG(temp) AS avg_temp
  FROM `bigquery-public-data.noaa_gsod.gsod20*`
  WHERE _TABLE_SUFFIX BETWEEN '08' AND '17' AND stn IN ('725030', '744860') AND temp != 9999.9 AND year BETWEEN '2008' AND '2017'
  GROUP BY date
), daily_complaints AS (
  SELECT DATE(created_date, 'America/New_York') AS date, complaint_type, COUNT(*) AS complaint_count
  FROM `bigquery-public-data.new_york.311_service_requests`
  WHERE DATE(created_date, 'America/New_York') BETWEEN '2008-01-01' AND '2017-12-31'
  GROUP BY date, complaint_type
), daily_totals AS (
  SELECT date, SUM(complaint_count) AS total_complaints
  FROM daily_complaints
  GROUP BY date
), filtered_types AS (
  SELECT complaint_type, SUM(complaint_count) AS total_complaints
  FROM daily_complaints
  GROUP BY complaint_type
  HAVING SUM(complaint_count) > 5000
), series AS (
  SELECT ft.complaint_type, dt.date, dt.avg_temp, COALESCE(dc.complaint_count, 0) AS daily_count, dt2.total_complaints AS daily_total
  FROM filtered_types ft
  CROSS JOIN daily_temps dt
  LEFT JOIN daily_complaints dc ON dt.date = dc.date AND ft.complaint_type = dc.complaint_type
  LEFT JOIN daily_totals dt2 ON dt.date = dt2.date
), aggregates AS (
  SELECT complaint_type, SUM(daily_count) AS total_complaints, COUNT(*) AS total_days_with_valid_temp, ROUND(CORR(avg_temp, daily_count), 4) AS corr_temp_count, ROUND(CORR(avg_temp, CASE WHEN daily_total > 0 THEN daily_count / daily_total ELSE NULL END), 4) AS corr_temp_percentage
  FROM series
  GROUP BY complaint_type
  HAVING ABS(CORR(avg_temp, daily_count)) > 0.5
)
SELECT complaint_type, total_complaints, total_days_with_valid_temp, corr_temp_count, corr_temp_percentage
FROM aggregates
ORDER BY complaint_type