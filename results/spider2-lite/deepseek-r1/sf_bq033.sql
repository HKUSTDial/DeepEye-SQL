WITH months AS (
  SELECT DATEADD('month', ROW_NUMBER() OVER (ORDER BY NULL) - 1, '2008-01-01'::DATE) AS month_start
  FROM TABLE(GENERATOR(ROWCOUNT => 180))
  QUALIFY month_start <= '2022-12-01'::DATE
),
iot_counts AS (
  SELECT 
    DATE_TRUNC('month', TO_DATE(TO_CHAR(p."filing_date"), 'YYYYMMDD')) AS month_start,
    COUNT(DISTINCT p."publication_number") AS pub_count
  FROM "PATENTS"."PATENTS"."PUBLICATIONS" AS p
  INNER JOIN LATERAL FLATTEN(INPUT => p."abstract_localized") AS abs
  WHERE p."country_code" = 'US'
    AND p."filing_date" >= 20080101 AND p."filing_date" <= 20221231
    AND abs.value:"text"::STRING ILIKE '%internet of things%'
  GROUP BY month_start
)
SELECT 
  m.month_start,
  COALESCE(i.pub_count, 0) AS publication_count
FROM months m
LEFT JOIN iot_counts i ON m.month_start = i.month_start
ORDER BY m.month_start;