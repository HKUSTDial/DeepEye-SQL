WITH filtered_patents AS (
  SELECT
    "publication_number",
    "publication_date",
    ARRAY_SIZE("inventor") AS "inventor_count"
  FROM "PATENTS"."PATENTS"."PUBLICATIONS"
  WHERE "country_code" = 'CA'
    AND "publication_date" BETWEEN 19600101 AND 20201231
    AND ARRAY_SIZE("inventor") > 0
),
patents_with_year AS (
  SELECT
    "publication_number",
    "publication_date",
    "inventor_count",
    FLOOR("publication_date" / 10000) AS "year"
  FROM filtered_patents
),
intervals AS (
  SELECT
    "publication_number",
    "publication_date",
    "inventor_count",
    "year",
    1960 + FLOOR(("year" - 1960) / 5) * 5 AS "interval_start"
  FROM patents_with_year
)
SELECT
  CONCAT("interval_start", '-', "interval_start" + 4) AS five_year_interval,
  AVG("inventor_count") AS avg_inventors_per_patent,
  COUNT("publication_number") AS total_publications
FROM intervals
GROUP BY "interval_start"
ORDER BY "interval_start"