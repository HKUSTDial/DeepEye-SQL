WITH numbers AS (
  SELECT ROW_NUMBER() OVER (ORDER BY 1) - 1 AS n
  FROM TABLE(GENERATOR(ROWCOUNT => 180))
),
months AS (
  SELECT DATEADD('month', n, '2008-01-01')::DATE AS month_start
  FROM numbers
),
filtered_patents AS (
  SELECT DISTINCT
    p."publication_number",
    TO_DATE(TO_CHAR(p."filing_date"), 'YYYYMMDD') AS filing_date
  FROM "PATENTS"."PATENTS"."PUBLICATIONS" p
  CROSS JOIN LATERAL FLATTEN(INPUT => p."abstract_localized") abs
  WHERE p."country_code" = 'US'
    AND p."filing_date" >= 20080101
    AND p."filing_date" <= 20221231
    AND abs.value::STRING ILIKE '%internet of things%'
)
SELECT
  TO_CHAR(m.month_start, 'YYYY-MM') AS month,
  COUNT(DISTINCT fp."publication_number") AS publication_count
FROM months m
LEFT JOIN filtered_patents fp
  ON DATE_TRUNC('month', fp.filing_date) = m.month_start
GROUP BY m.month_start
ORDER BY m.month_start