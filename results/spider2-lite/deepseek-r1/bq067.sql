WITH multi_person_accidents AS (
  SELECT consecutive_number
  FROM `bigquery-public-data.nhtsa_traffic_fatalities.person_2016`
  GROUP BY consecutive_number
  HAVING COUNT(DISTINCT person_number) > 1
),
accident_labels AS (
  SELECT consecutive_number,
         CASE WHEN COUNT(CASE WHEN injury_severity = 4 THEN 1 END) > 1 
              THEN 1 ELSE 0 END AS label
  FROM `bigquery-public-data.nhtsa_traffic_fatalities.person_2016`
  GROUP BY consecutive_number
),
accident_predictors AS (
  SELECT consecutive_number,
         state_number,
         day_of_week,
         hour_of_crash,
         number_of_drunk_drivers,
         CASE WHEN work_zone_name != 'None' OR work_zone != 0 
              THEN 1 ELSE 0 END AS work_zone_indicator
  FROM `bigquery-public-data.nhtsa_traffic_fatalities.accident_2016`
),
vehicle_body_type AS (
  SELECT consecutive_number,
         body_type
  FROM `bigquery-public-data.nhtsa_traffic_fatalities.vehicle_2016`
  WHERE vehicle_number = 1
),
speed_difference AS (
  SELECT consecutive_number,
         AVG(ABS(travel_speed - speed_limit)) AS avg_speed_diff
  FROM `bigquery-public-data.nhtsa_traffic_fatalities.vehicle_2016`
  WHERE travel_speed BETWEEN 0 AND 151
    AND speed_limit BETWEEN 5 AND 80
  GROUP BY consecutive_number
)
SELECT 
  a.consecutive_number,
  l.label,
  a.state_number,
  v.body_type,
  a.number_of_drunk_drivers,
  a.day_of_week,
  a.hour_of_crash,
  a.work_zone_indicator,
  CASE 
    WHEN s.avg_speed_diff IS NULL THEN NULL
    WHEN s.avg_speed_diff < 20 THEN 0
    WHEN s.avg_speed_diff < 40 THEN 1
    WHEN s.avg_speed_diff < 60 THEN 2
    WHEN s.avg_speed_diff < 80 THEN 3
    ELSE 4
  END AS avg_speed_diff_level
FROM multi_person_accidents m
JOIN accident_predictors a ON m.consecutive_number = a.consecutive_number
JOIN accident_labels l ON m.consecutive_number = l.consecutive_number
LEFT JOIN vehicle_body_type v ON m.consecutive_number = v.consecutive_number
LEFT JOIN speed_difference s ON m.consecutive_number = s.consecutive_number