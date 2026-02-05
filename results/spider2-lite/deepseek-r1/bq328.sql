WITH latest_gdp_values AS (
  SELECT ind.country_code, ind.value AS gdp_value
  FROM `bigquery-public-data.world_bank_wdi.indicators_data` ind
  INNER JOIN (
    SELECT country_code, MAX(year) AS max_year
    FROM `bigquery-public-data.world_bank_wdi.indicators_data`
    WHERE indicator_code = 'NY.GDP.MKTP.KD'
    GROUP BY country_code
  ) latest ON ind.country_code = latest.country_code AND ind.year = latest.max_year
  WHERE ind.indicator_code = 'NY.GDP.MKTP.KD'
),
region_gdp AS (
  SELECT cs.region, lgv.gdp_value
  FROM latest_gdp_values lgv
  JOIN `bigquery-public-data.world_bank_wdi.country_summary` cs
    ON lgv.country_code = cs.country_code
  WHERE cs.region IS NOT NULL
),
median_gdp_per_region AS (
  SELECT region, APPROX_QUANTILES(gdp_value, 2)[OFFSET(1)] AS median_gdp
  FROM region_gdp
  GROUP BY region
)
SELECT region, median_gdp
FROM median_gdp_per_region
ORDER BY median_gdp DESC
LIMIT 1