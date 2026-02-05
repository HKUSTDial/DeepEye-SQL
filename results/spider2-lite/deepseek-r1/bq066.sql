WITH natality AS (
  SELECT 
    `County_of_Residence_FIPS` AS fips,
    EXTRACT(YEAR FROM `Year`) AS birth_year,
    SUM(`Births`) AS total_births,
    SUM(CASE WHEN `Maternal_Morbidity_Desc` = 'None checked' THEN `Births` ELSE 0 END) AS births_no_morbidity
  FROM `bigquery-public-data.sdoh_cdc_wonder_natality.county_natality_by_maternal_morbidity`
  WHERE EXTRACT(YEAR FROM `Year`) BETWEEN 2016 AND 2018
  GROUP BY `County_of_Residence_FIPS`, `Year`
),
census AS (
  SELECT 
    `geo_id`,
    `poverty` / `pop_determined_poverty_status` AS poverty_rate,
    2015 AS census_year
  FROM `bigquery-public-data.census_bureau_acs.county_2015_5yr`
  WHERE `pop_determined_poverty_status` > 0
  UNION ALL
  SELECT 
    `geo_id`,
    `poverty` / `pop_determined_poverty_status` AS poverty_rate,
    2016 AS census_year
  FROM `bigquery-public-data.census_bureau_acs.county_2016_5yr`
  WHERE `pop_determined_poverty_status` > 0
  UNION ALL
  SELECT 
    `geo_id`,
    `poverty` / `pop_determined_poverty_status` AS poverty_rate,
    2017 AS census_year
  FROM `bigquery-public-data.census_bureau_acs.county_2017_5yr`
  WHERE `pop_determined_poverty_status` > 0
)
SELECT 
  CORR(c.poverty_rate, n.births_no_morbidity / n.total_births) AS pearson_correlation
FROM natality n
JOIN census c 
  ON n.fips = c.`geo_id` 
 AND n.birth_year = c.census_year + 1