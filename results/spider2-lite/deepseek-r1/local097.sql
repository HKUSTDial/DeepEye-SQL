WITH start_years AS (
  SELECT DISTINCT CAST(`year` AS INTEGER) AS start_year FROM `Movie`
), period_counts AS (
  SELECT s.start_year, COUNT(m.`MID`) AS movie_count
  FROM start_years s
  INNER JOIN `Movie` m ON CAST(m.`year` AS INTEGER) BETWEEN s.start_year AND s.start_year + 9
  GROUP BY s.start_year
)
SELECT start_year, movie_count
FROM period_counts
ORDER BY movie_count DESC, start_year ASC
LIMIT 1