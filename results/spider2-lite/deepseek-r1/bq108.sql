WITH accident_stats AS (
  SELECT 
    `consecutive_number`,
    COUNT(DISTINCT `person_number`) AS total_persons,
    COUNT(DISTINCT CASE WHEN `injury_severity` = 4 THEN `person_number` END) AS severe_count
  FROM `bigquery-public-data.nhtsa_traffic_fatalities.person_2015`
  WHERE `month_of_crash` BETWEEN 1 AND 8
  GROUP BY `consecutive_number`
  HAVING COUNT(DISTINCT `person_number`) > 1
)
SELECT 
  100.0 * SUM(CASE WHEN `severe_count` > 1 THEN 1 ELSE 0 END) / COUNT(*) AS percentage
FROM accident_stats