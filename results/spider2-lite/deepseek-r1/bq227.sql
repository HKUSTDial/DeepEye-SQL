WITH top5 AS (
  SELECT minor_category, ROW_NUMBER() OVER (ORDER BY SUM(value) DESC) AS rank_2008
  FROM `bigquery-public-data.london_crime.crime_by_lsoa`
  WHERE year = 2008
  GROUP BY minor_category
  ORDER BY SUM(value) DESC
  LIMIT 5
),
year_totals AS (
  SELECT year, SUM(value) AS year_total
  FROM `bigquery-public-data.london_crime.crime_by_lsoa`
  WHERE year >= 2008
  GROUP BY year
),
category_year_totals AS (
  SELECT year, minor_category, SUM(value) AS category_total
  FROM `bigquery-public-data.london_crime.crime_by_lsoa`
  WHERE year >= 2008
  GROUP BY year, minor_category
),
combined AS (
  SELECT yt.year, t.rank_2008,
         ROUND(COALESCE(cy.category_total, 0) * 100.0 / yt.year_total, 2) AS percentage
  FROM year_totals yt
  CROSS JOIN top5 t
  LEFT JOIN category_year_totals cy ON yt.year = cy.year AND t.minor_category = cy.minor_category
)
SELECT year,
       MAX(CASE WHEN rank_2008 = 1 THEN percentage END) AS rank1_share,
       MAX(CASE WHEN rank_2008 = 2 THEN percentage END) AS rank2_share,
       MAX(CASE WHEN rank_2008 = 3 THEN percentage END) AS rank3_share,
       MAX(CASE WHEN rank_2008 = 4 THEN percentage END) AS rank4_share,
       MAX(CASE WHEN rank_2008 = 5 THEN percentage END) AS rank5_share
FROM combined
GROUP BY year
ORDER BY year