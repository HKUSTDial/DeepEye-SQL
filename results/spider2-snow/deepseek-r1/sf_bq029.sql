WITH patent_inventor_counts AS (
  SELECT 
    "publication_number",
    "country_code",
    "publication_date",
    COUNT(DISTINCT inv.value) AS inventor_count
  FROM "PATENTS"."PATENTS"."PUBLICATIONS"
  JOIN LATERAL FLATTEN(INPUT => "inventor") inv
  WHERE "country_code" = 'CA'
    AND "publication_date" IS NOT NULL
    AND FLOOR("publication_date" / 10000) BETWEEN 1960 AND 2020
  GROUP BY "publication_number", "country_code", "publication_date"
  HAVING inventor_count > 0
),
period_data AS (
  SELECT 
    FLOOR((FLOOR("publication_date" / 10000) - 1960) / 5) * 5 + 1960 AS period_start_year,
    COUNT("publication_number") AS patent_count,
    AVG(inventor_count) AS avg_inventors
  FROM patent_inventor_counts
  GROUP BY period_start_year
)
SELECT 
  period_start_year,
  period_start_year + 4 AS period_end_year,
  patent_count AS total_publications,
  avg_inventors AS avg_inventors_per_patent
FROM period_data
ORDER BY period_start_year