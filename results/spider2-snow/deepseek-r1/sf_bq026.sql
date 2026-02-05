WITH a61_patents AS (
  SELECT DISTINCT p."publication_number", p."assignee", p."filing_date", p."country_code"
  FROM "PATENTS"."PATENTS"."PUBLICATIONS" p
  JOIN LATERAL FLATTEN(input => p."cpc") c
  WHERE STARTSWITH(c."VALUE"::VARCHAR, 'A61')
),
assignee_flattened AS (
  SELECT TRIM(f."VALUE"::VARCHAR) AS assignee_name, COUNT(DISTINCT a."publication_number") AS a61_count
  FROM a61_patents a
  JOIN LATERAL FLATTEN(input => a."assignee") f
  GROUP BY assignee_name
),
top_assignee AS (
  SELECT assignee_name
  FROM assignee_flattened
  ORDER BY a61_count DESC
  LIMIT 1
),
all_patents_assignee_flattened AS (
  SELECT p."publication_number", p."filing_date", p."country_code", TRIM(f."VALUE"::VARCHAR) AS assignee_name
  FROM "PATENTS"."PATENTS"."PUBLICATIONS" p
  JOIN LATERAL FLATTEN(input => p."assignee") f
),
assignee_patents AS (
  SELECT * FROM all_patents_assignee_flattened
  WHERE assignee_name = (SELECT assignee_name FROM top_assignee)
),
assignee_yearly AS (
  SELECT assignee_name, SUBSTRING(CAST("filing_date" AS VARCHAR), 1, 4) AS year, COUNT(DISTINCT "publication_number") AS patent_count
  FROM assignee_patents
  GROUP BY assignee_name, year
),
busiest_year AS (
  SELECT year
  FROM assignee_yearly
  ORDER BY patent_count DESC
  LIMIT 1
),
jurisdiction_counts AS (
  SELECT ap."country_code", COUNT(DISTINCT ap."publication_number") AS patent_count
  FROM assignee_patents ap
  WHERE SUBSTRING(CAST(ap."filing_date" AS VARCHAR), 1, 4) = (SELECT year FROM busiest_year)
  GROUP BY ap."country_code"
  ORDER BY patent_count DESC, ap."country_code"
  LIMIT 5
)
SELECT LISTAGG("country_code", ', ') WITHIN GROUP (ORDER BY patent_count DESC, "country_code") AS top_jurisdictions
FROM jurisdiction_counts