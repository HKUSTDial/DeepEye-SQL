SELECT MAX(daily_count) AS highest_number
FROM (
  SELECT DATE(`start_time`) AS day, COUNT(*) AS daily_count
  FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips`
  WHERE `bike_type` = 'electric'
    AND `duration_minutes` > 10
    AND `subscriber_type` = 'Student Membership'
    AND `start_station_name` NOT IN ('Mobile Station', 'Repair Shop')
    AND `end_station_name` NOT IN ('Mobile Station', 'Repair Shop')
  GROUP BY day
) AS daily_counts