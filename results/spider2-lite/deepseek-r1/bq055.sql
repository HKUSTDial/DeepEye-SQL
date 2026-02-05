WITH google_2021 AS (
  SELECT 
    race_white,
    race_black,
    race_asian,
    race_hispanic_latinx
  FROM `bigquery-public-data.google_dei.dar_non_intersectional_hiring`
  WHERE report_year = 2021
    AND workforce = 'overall'
),
bls_2021 AS (
  SELECT 
    AVG(percent_white) AS avg_white,
    AVG(percent_black_or_african_american) AS avg_black,
    AVG(percent_asian) AS avg_asian,
    AVG(percent_hispanic_or_latino) AS avg_hispanic
  FROM `bigquery-public-data.bls.cpsaat18`
  WHERE year = 2021
    AND sector IN ('Internet publishing and broadcasting and web search portals',
                   'Software publishers',
                   'Data processing, hosting, and related services',
                   'Computer systems design and related services')
),
race_data AS (
  SELECT 
    'White' AS race,
    g.race_white AS google_pct,
    b.avg_white AS bls_pct
  FROM google_2021 g, bls_2021 b
  UNION ALL
  SELECT 
    'Black or African American' AS race,
    g.race_black,
    b.avg_black
  FROM google_2021 g, bls_2021 b
  UNION ALL
  SELECT 
    'Asian' AS race,
    g.race_asian,
    b.avg_asian
  FROM google_2021 g, bls_2021 b
  UNION ALL
  SELECT 
    'Hispanic or Latinx' AS race,
    g.race_hispanic_latinx,
    b.avg_hispanic
  FROM google_2021 g, bls_2021 b
)
SELECT 
  race,
  google_pct - bls_pct AS difference
FROM race_data
ORDER BY ABS(google_pct - bls_pct) DESC
LIMIT 3