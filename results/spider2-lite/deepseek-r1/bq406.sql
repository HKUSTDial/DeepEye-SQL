WITH data AS (
  SELECT 
    MAX(CASE WHEN report_year = 2014 AND LOWER(workforce) = 'overall' THEN race_asian END) AS race_asian_2014,
    MAX(CASE WHEN report_year = 2014 AND LOWER(workforce) = 'overall' THEN race_black END) AS race_black_2014,
    MAX(CASE WHEN report_year = 2014 AND LOWER(workforce) = 'overall' THEN race_hispanic_latinx END) AS race_hispanic_latinx_2014,
    MAX(CASE WHEN report_year = 2014 AND LOWER(workforce) = 'overall' THEN race_native_american END) AS race_native_american_2014,
    MAX(CASE WHEN report_year = 2014 AND LOWER(workforce) = 'overall' THEN race_white END) AS race_white_2014,
    MAX(CASE WHEN report_year = 2014 AND LOWER(workforce) = 'overall' THEN gender_us_women END) AS gender_us_women_2014,
    MAX(CASE WHEN report_year = 2014 AND LOWER(workforce) = 'overall' THEN gender_us_men END) AS gender_us_men_2014,
    MAX(CASE WHEN report_year = 2014 AND LOWER(workforce) = 'overall' THEN gender_global_women END) AS gender_global_women_2014,
    MAX(CASE WHEN report_year = 2014 AND LOWER(workforce) = 'overall' THEN gender_global_men END) AS gender_global_men_2014,
    MAX(CASE WHEN report_year = 2024 AND LOWER(workforce) = 'overall' THEN race_asian END) AS race_asian_2024,
    MAX(CASE WHEN report_year = 2024 AND LOWER(workforce) = 'overall' THEN race_black END) AS race_black_2024,
    MAX(CASE WHEN report_year = 2024 AND LOWER(workforce) = 'overall' THEN race_hispanic_latinx END) AS race_hispanic_latinx_2024,
    MAX(CASE WHEN report_year = 2024 AND LOWER(workforce) = 'overall' THEN race_native_american END) AS race_native_american_2024,
    MAX(CASE WHEN report_year = 2024 AND LOWER(workforce) = 'overall' THEN race_white END) AS race_white_2024,
    MAX(CASE WHEN report_year = 2024 AND LOWER(workforce) = 'overall' THEN gender_us_women END) AS gender_us_women_2024,
    MAX(CASE WHEN report_year = 2024 AND LOWER(workforce) = 'overall' THEN gender_us_men END) AS gender_us_men_2024,
    MAX(CASE WHEN report_year = 2024 AND LOWER(workforce) = 'overall' THEN gender_global_women END) AS gender_global_women_2024,
    MAX(CASE WHEN report_year = 2024 AND LOWER(workforce) = 'overall' THEN gender_global_men END) AS gender_global_men_2024
  FROM `bigquery-public-data.google_dei.dar_non_intersectional_representation`
)
SELECT 
  ((race_asian_2024 - race_asian_2014) / race_asian_2014) * 100 AS asian_growth_rate,
  ((race_black_2024 - race_black_2014) / race_black_2014) * 100 AS black_growth_rate,
  ((race_hispanic_latinx_2024 - race_hispanic_latinx_2014) / race_hispanic_latinx_2014) * 100 AS latinx_growth_rate,
  ((race_native_american_2024 - race_native_american_2014) / race_native_american_2014) * 100 AS native_american_growth_rate,
  ((race_white_2024 - race_white_2014) / race_white_2014) * 100 AS white_growth_rate,
  ((gender_us_women_2024 - gender_us_women_2014) / gender_us_women_2014) * 100 AS us_women_growth_rate,
  ((gender_us_men_2024 - gender_us_men_2014) / gender_us_men_2014) * 100 AS us_men_growth_rate,
  ((gender_global_women_2024 - gender_global_women_2014) / gender_global_women_2014) * 100 AS global_women_growth_rate,
  ((gender_global_men_2024 - gender_global_men_2014) / gender_global_men_2014) * 100 AS global_men_growth_rate
FROM data