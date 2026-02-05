SELECT 
  'Google Hiring' AS source,
  report_year AS year,
  race_asian,
  race_black,
  race_hispanic_latinx,
  race_white,
  gender_us_women,
  gender_us_men
FROM `bigquery-public-data.google_dei.dar_non_intersectional_hiring`
WHERE report_year = 2021 AND workforce = 'overall'
UNION ALL
SELECT 
  'Google Representation' AS source,
  report_year AS year,
  race_asian,
  race_black,
  race_hispanic_latinx,
  race_white,
  gender_us_women,
  gender_us_men
FROM `bigquery-public-data.google_dei.dar_non_intersectional_representation`
WHERE report_year = 2021 AND workforce = 'overall'
UNION ALL
SELECT 
  CONCAT('BLS Tech Sector: ', sector) AS source,
  year,
  percent_asian AS race_asian,
  percent_black_or_african_american AS race_black,
  percent_hispanic_or_latino AS race_hispanic_latinx,
  percent_white AS race_white,
  percent_women AS gender_us_women,
  1 - percent_women AS gender_us_men
FROM `bigquery-public-data.bls.cpsaat18`
WHERE year = 2021 
  AND sector IN ('Internet publishing and broadcasting and web search portals', 'Computer systems design and related services')
ORDER BY source