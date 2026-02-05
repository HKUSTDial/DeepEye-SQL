WITH epa_1990 AS (
  SELECT 
    ROUND(latitude, 2) AS lat_rounded,
    ROUND(longitude, 2) AS lon_rounded,
    city_name,
    AVG(arithmetic_mean) AS epa_pm25_1990
  FROM `bigquery-public-data.epa_historical_air_quality.air_quality_annual_summary`
  WHERE year = 1990
    AND parameter_name = 'Acceptable PM2.5 AQI & Speciation Mass'
    AND units_of_measure = 'Micrograms/cubic meter (LC)'
  GROUP BY ROUND(latitude, 2), ROUND(longitude, 2), city_name
),
openaq_2020 AS (
  SELECT 
    ROUND(latitude, 2) AS lat_rounded,
    ROUND(longitude, 2) AS lon_rounded,
    city,
    AVG(value) AS openaq_pm25_2020
  FROM `bigquery-public-data.openaq.global_air_quality`
  WHERE EXTRACT(YEAR FROM timestamp) = 2020
    AND pollutant = 'pm25'
  GROUP BY ROUND(latitude, 2), ROUND(longitude, 2), city
)
SELECT 
  COALESCE(epa_1990.city_name, openaq_2020.city) AS city,
  (epa_1990.epa_pm25_1990 - openaq_2020.openaq_pm25_2020) AS difference
FROM epa_1990
JOIN openaq_2020
  ON epa_1990.lat_rounded = openaq_2020.lat_rounded
  AND epa_1990.lon_rounded = openaq_2020.lon_rounded
ORDER BY difference DESC
LIMIT 3