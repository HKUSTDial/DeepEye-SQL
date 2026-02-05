SELECT
  TRIM(confirmed.county_name) AS county_name,
  confirmed.state,
  census.median_age,
  census.total_pop,
  (confirmed._2020_08_27 / census.total_pop) * 100000 AS cases_per_100k,
  (deaths._2020_08_27 / census.total_pop) * 100000 AS deaths_per_100k,
  (deaths._2020_08_27 / confirmed._2020_08_27) * 100 AS case_fatality_rate
FROM
  `bigquery-public-data.census_bureau_acs.county_2020_5yr` AS census
JOIN
  `bigquery-public-data.covid19_usafacts.confirmed_cases` AS confirmed
  ON LPAD(census.geo_id, 5, '0') = confirmed.county_fips_code
JOIN
  `bigquery-public-data.covid19_usafacts.deaths` AS deaths
  ON LPAD(census.geo_id, 5, '0') = deaths.county_fips_code AND confirmed.state = deaths.state
WHERE
  census.total_pop > 50000
  AND confirmed._2020_08_27 > 0
ORDER BY
  case_fatality_rate DESC
LIMIT 3