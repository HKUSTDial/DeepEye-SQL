WITH target_patents AS (
  SELECT
    "application_number",
    "filing_date",
    TO_DATE(CAST("filing_date" AS VARCHAR), 'YYYYMMDD') AS filing_date_dt
  FROM
    "PATENTS"."PATENTS"."PUBLICATIONS"
  WHERE
    "application_kind" = 'U'
    AND "grant_date" BETWEEN 20100101 AND 20101231
),
citations_unpacked AS (
  SELECT
    "application_number" AS citing_app,
    f.value::STRING AS cited_app
  FROM
    "PATENTS"."PATENTS"."PUBLICATIONS",
    LATERAL FLATTEN(INPUT => "citation") f
)
SELECT
  COUNT(*) AS patent_count
FROM (
  SELECT
    tp."application_number"
  FROM
    target_patents tp
    INNER JOIN citations_unpacked cu ON tp."application_number" = cu.cited_app
    INNER JOIN "PATENTS"."PATENTS"."PUBLICATIONS" citing ON cu.citing_app = citing."application_number"
  WHERE
    TO_DATE(CAST(citing."filing_date" AS VARCHAR), 'YYYYMMDD') BETWEEN tp.filing_date_dt AND DATEADD(year, 10, tp.filing_date_dt)
    AND citing."application_number" != tp."application_number"
  GROUP BY
    tp."application_number"
  HAVING
    COUNT(DISTINCT citing."application_number") = 1
) sub