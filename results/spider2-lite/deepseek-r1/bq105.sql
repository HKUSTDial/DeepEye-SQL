WITH state_population AS (
  SELECT 
    za.state_name,
    SUM(p.population) AS state_population
  FROM `bigquery-public-data.census_bureau_usa.population_by_zip_2010` p
  JOIN `bigquery-public-data.utility_us.zipcode_area` za ON p.zipcode = za.zipcode
  WHERE p.gender IS NULL AND p.minimum_age IS NULL AND p.maximum_age IS NULL
  GROUP BY za.state_name
),
distracted_accidents_2015 AS (
  SELECT 
    a.state_name,
    COUNT(DISTINCT d.consecutive_number) AS distracted_accidents
  FROM `bigquery-public-data.nhtsa_traffic_fatalities.distract_2015` d
  JOIN `bigquery-public-data.nhtsa_traffic_fatalities.accident_2015` a ON d.consecutive_number = a.consecutive_number AND d.state_number = a.state_number
  WHERE d.driver_distracted_by_name NOT IN ('Not Distracted', 'Unknown if Distracted', 'Not Reported')
  GROUP BY a.state_name
),
distracted_accidents_2016 AS (
  SELECT 
    a.state_name,
    COUNT(DISTINCT d.consecutive_number) AS distracted_accidents
  FROM `bigquery-public-data.nhtsa_traffic_fatalities.distract_2016` d
  JOIN `bigquery-public-data.nhtsa_traffic_fatalities.accident_2016` a ON d.consecutive_number = a.consecutive_number AND d.state_number = a.state_number
  WHERE d.driver_distracted_by_name NOT IN ('Not Distracted', 'Unknown if Distracted', 'Not Reported')
  GROUP BY a.state_name
),
combined_2015 AS (
  SELECT 
    2015 AS year,
    da.state_name,
    da.distracted_accidents,
    sp.state_population,
    (da.distracted_accidents * 100000.0) / sp.state_population AS rate_per_100k
  FROM distracted_accidents_2015 da
  JOIN state_population sp ON da.state_name = sp.state_name
),
combined_2016 AS (
  SELECT 
    2016 AS year,
    da.state_name,
    da.distracted_accidents,
    sp.state_population,
    (da.distracted_accidents * 100000.0) / sp.state_population AS rate_per_100k
  FROM distracted_accidents_2016 da
  JOIN state_population sp ON da.state_name = sp.state_name
),
all_years AS (
  SELECT * FROM combined_2015
  UNION ALL
  SELECT * FROM combined_2016
),
ranked AS (
  SELECT 
    year,
    state_name,
    distracted_accidents,
    state_population,
    rate_per_100k,
    ROW_NUMBER() OVER (PARTITION BY year ORDER BY rate_per_100k DESC) AS rank
  FROM all_years
)
SELECT 
  year,
  state_name,
  distracted_accidents,
  state_population,
  rate_per_100k
FROM ranked
WHERE rank <= 5
ORDER BY year, rank