SELECT month FROM (
  SELECT EXTRACT(MONTH FROM end_date) AS month, ABS(SUM(CASE WHEN subscriber_type = 'Customer' THEN duration_sec/60 ELSE 0 END) - SUM(CASE WHEN subscriber_type = 'Subscriber' THEN duration_sec/60 ELSE 0 END)) AS diff
  FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
  WHERE EXTRACT(YEAR FROM end_date) = 2017
  GROUP BY month
  ORDER BY diff DESC
  LIMIT 1
)