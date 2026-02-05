SELECT 
  latitude,
  longitude,
  DATE(timestamp) AS date,
  AVG(wind_speed) AS daily_avg_wind_speed
FROM `bigquery-public-data.noaa_icoads.icoads_core_*`
WHERE wind_speed IS NOT NULL
  AND EXTRACT(YEAR FROM timestamp) BETWEEN 2005 AND 2015
  AND REGEXP_CONTAINS(_TABLE_SUFFIX, r'^(200[5-9]|201[0-5])$')
GROUP BY latitude, longitude, date
ORDER BY daily_avg_wind_speed DESC
LIMIT 5