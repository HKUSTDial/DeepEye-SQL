WITH target_patents AS (
  SELECT 
    "application_number" AS target_app_num,
    TRY_TO_DATE(CAST("filing_date" AS VARCHAR), 'YYYYMMDD') AS target_filing_date_parsed
  FROM "PATENTS"."PATENTS"."PUBLICATIONS"
  WHERE "application_kind" = 'U'
    AND "grant_date" IS NOT NULL
    AND EXTRACT(YEAR FROM TRY_TO_DATE(CAST("grant_date" AS VARCHAR), 'YYYYMMDD')) = 2010
    AND target_filing_date_parsed IS NOT NULL
), citing_patents AS (
  SELECT 
    tp.target_app_num,
    tp.target_filing_date_parsed,
    cp."application_number" AS citing_app_num,
    TRY_TO_DATE(CAST(cp."filing_date" AS VARCHAR), 'YYYYMMDD') AS citing_filing_date
  FROM target_patents tp
  INNER JOIN "PATENTS"."PATENTS"."PUBLICATIONS" cp
    ON ARRAY_CONTAINS(tp.target_app_num::VARIANT, cp."citation")
  WHERE TRY_TO_DATE(CAST(cp."filing_date" AS VARCHAR), 'YYYYMMDD') IS NOT NULL
)
SELECT COUNT(*) AS patent_count
FROM (
  SELECT 
    target_app_num
  FROM citing_patents
  WHERE citing_filing_date BETWEEN target_filing_date_parsed AND DATEADD(year, 10, target_filing_date_parsed)
  GROUP BY target_app_num
  HAVING COUNT(DISTINCT citing_app_num) = 1
)