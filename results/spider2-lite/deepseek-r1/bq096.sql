WITH filtered_sightings AS (
  SELECT eventdate
  FROM `bigquery-public-data.gbif.occurrences`
  WHERE species = 'Sterna paradisaea'
    AND decimallatitude > 40
    AND `month` > 1
),
daily_counts AS (
  SELECT DATE(eventdate) AS sighting_date, COUNT(*) AS daily_count
  FROM filtered_sightings
  GROUP BY sighting_date
  HAVING daily_count > 10
),
per_year_first_days AS (
  SELECT EXTRACT(YEAR FROM sighting_date) AS year, MIN(sighting_date) AS first_day
  FROM daily_counts
  GROUP BY year
)
SELECT year, first_day
FROM per_year_first_days
ORDER BY first_day ASC
LIMIT 1