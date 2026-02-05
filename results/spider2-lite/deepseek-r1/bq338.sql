WITH county_tracts AS (
  SELECT `geo_id`
  FROM `bigquery-public-data.geo_census_tracts.us_census_tracts_national`
  WHERE `state_fips_code` = '36' AND `county_fips_code` = '047'
),
tract_data AS (
  SELECT 
    ct.`geo_id`,
    t2011.`total_pop` AS pop_2011,
    t2018.`total_pop` AS pop_2018,
    t2011.`median_income` AS income_2011,
    t2018.`median_income` AS income_2018,
    ((t2018.`total_pop` - t2011.`total_pop`) / t2011.`total_pop`) * 100 AS pop_increase_pct,
    (t2018.`median_income` - t2011.`median_income`) AS income_increase_abs
  FROM county_tracts ct
  JOIN `bigquery-public-data.census_bureau_acs.censustract_2011_5yr` t2011
    ON ct.`geo_id` = t2011.`geo_id`
  JOIN `bigquery-public-data.census_bureau_acs.censustract_2018_5yr` t2018
    ON ct.`geo_id` = t2018.`geo_id`
  WHERE t2011.`total_pop` > 1000 AND t2018.`total_pop` > 1000
),
ranked_tracts AS (
  SELECT 
    `geo_id`,
    `pop_2011`,
    `pop_2018`,
    `pop_increase_pct`,
    `income_2011`,
    `income_2018`,
    `income_increase_abs`,
    RANK() OVER (ORDER BY `pop_increase_pct` DESC) AS pop_rank,
    RANK() OVER (ORDER BY `income_increase_abs` DESC) AS income_rank
  FROM tract_data
)
SELECT `geo_id`
FROM ranked_tracts
WHERE `pop_rank` <= 20 AND `income_rank` <= 20
ORDER BY `geo_id`