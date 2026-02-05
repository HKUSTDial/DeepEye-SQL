WITH assignee_a61_counts AS (
  SELECT 
    TRIM(assignee_flat.value::STRING) AS assignee_name,
    COUNT(DISTINCT "application_number") AS app_count
  FROM 
    "PATENTS"."PATENTS"."PUBLICATIONS",
    LATERAL FLATTEN(INPUT => "ipc") AS ipc_flat,
    LATERAL FLATTEN(INPUT => "assignee") AS assignee_flat
  WHERE 
    CONTAINS(ipc_flat.value::STRING, 'A61')
  GROUP BY 
    TRIM(assignee_flat.value::STRING)
),
top_assignee AS (
  SELECT assignee_name
  FROM assignee_a61_counts
  ORDER BY app_count DESC
  LIMIT 1
),
assignee_yearly_counts AS (
  SELECT
    EXTRACT(YEAR FROM TO_DATE(CAST("filing_date" AS STRING), 'YYYYMMDD')) AS year,
    COUNT(DISTINCT "application_number") AS year_count
  FROM
    "PATENTS"."PATENTS"."PUBLICATIONS",
    LATERAL FLATTEN(INPUT => "assignee") AS assignee_flat
  WHERE
    TRIM(assignee_flat.value::STRING) = (SELECT assignee_name FROM top_assignee)
    AND "filing_date" IS NOT NULL
  GROUP BY
    EXTRACT(YEAR FROM TO_DATE(CAST("filing_date" AS STRING), 'YYYYMMDD'))
)
SELECT year
FROM assignee_yearly_counts
ORDER BY year_count DESC
LIMIT 1