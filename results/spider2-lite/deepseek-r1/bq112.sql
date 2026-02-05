WITH county_fips AS (
  SELECT `county_fips_code`
  FROM `bigquery-public-data.geo_us_boundaries.counties`
  WHERE `county_name` = 'Allegheny County'
),
annual_wages AS (
  SELECT
    CAST(REGEXP_EXTRACT(_TABLE_SUFFIX, r'^(\d{4})_q[1-4]$') AS INT64) AS year,
    AVG(`avg_wkly_wage_10_total_all_industries` * 52) AS avg_annual_wage
  FROM `bigquery-public-data.bls_qcew.*`,
    county_fips
  WHERE
    _TABLE_SUFFIX BETWEEN '1998_q1' AND '2017_q4'
    AND `area_fips` = county_fips.`county_fips_code`
  GROUP BY year
),
annual_cpi AS (
  SELECT
    `year`,
    AVG(`value`) AS avg_cpi
  FROM `bigquery-public-data.bls.cpi_u`
  WHERE
    `item_name` = 'All items'
    AND `area_name` = 'U.S. city average'
    AND `year` BETWEEN 1998 AND 2017
  GROUP BY `year`
),
wage_endpoints AS (
  SELECT
    MAX(CASE WHEN year = 1998 THEN avg_annual_wage END) AS wage_1998,
    MAX(CASE WHEN year = 2017 THEN avg_annual_wage END) AS wage_2017
  FROM annual_wages
),
cpi_endpoints AS (
  SELECT
    MAX(CASE WHEN year = 1998 THEN avg_cpi END) AS cpi_1998,
    MAX(CASE WHEN year = 2017 THEN avg_cpi END) AS cpi_2017
  FROM annual_cpi
)
SELECT
  ROUND(((w.wage_2017 - w.wage_1998) / w.wage_1998) * 100, 2) AS wage_growth_rate,
  ROUND(((c.cpi_2017 - c.cpi_1998) / c.cpi_1998) * 100, 2) AS cpi_growth_rate,
  ((w.wage_2017 - w.wage_1998) / w.wage_1998) >= ((c.cpi_2017 - c.cpi_1998) / c.cpi_1998) AS kept_pace
FROM wage_endpoints w, cpi_endpoints c