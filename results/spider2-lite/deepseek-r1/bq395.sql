WITH state_totals AS (
  SELECT 
    SUBSTR(CoC_Number, 1, 2) AS state_abbr,
    SUM(CASE WHEN Count_Year = 2015 THEN Unsheltered_Homeless ELSE 0 END) AS total_2015,
    SUM(CASE WHEN Count_Year = 2018 THEN Unsheltered_Homeless ELSE 0 END) AS total_2018
  FROM `bigquery-public-data.sdoh_hud_pit_homelessness.hud_pit_by_coc`
  WHERE Count_Year IN (2015, 2018)
  GROUP BY state_abbr
  HAVING SUM(CASE WHEN Count_Year = 2015 THEN Unsheltered_Homeless ELSE 0 END) > 0
),
state_pct_change AS (
  SELECT 
    state_abbr,
    (total_2018 - total_2015) * 100.0 / total_2015 AS pct_change
  FROM state_totals
),
national_avg AS (
  SELECT AVG(pct_change) AS avg_pct_change
  FROM state_pct_change
)
SELECT 
  state_abbr
FROM state_pct_change
CROSS JOIN national_avg
ORDER BY ABS(pct_change - avg_pct_change)
LIMIT 5