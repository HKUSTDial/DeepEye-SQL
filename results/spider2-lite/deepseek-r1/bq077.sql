WITH monthly_counts AS (
  SELECT 
    `year`,
    EXTRACT(MONTH FROM `date`) AS month,
    COUNT(`unique_key`) AS monthly_count
  FROM `bigquery-public-data.chicago_crime.crime`
  WHERE `primary_type` = 'MOTOR VEHICLE THEFT'
    AND `year` BETWEEN 2010 AND 2016
  GROUP BY `year`, month
)
SELECT 
  `year`,
  MAX(monthly_count) AS highest_number_of_motor_thefts_in_one_month
FROM monthly_counts
GROUP BY `year`
ORDER BY `year`