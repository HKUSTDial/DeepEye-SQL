WITH pub_a61 AS (
    SELECT p."application_number_formatted", p."filing_date", p."assignee"
    FROM "PATENTS"."PATENTS"."PUBLICATIONS" AS p,
    LATERAL FLATTEN(INPUT => p."ipc") AS ipc_flattened
    WHERE STARTSWITH(ipc_flattened.value::STRING, 'A61')
      AND p."application_number_formatted" IS NOT NULL
      AND p."filing_date" IS NOT NULL
),
pub_assignee AS (
    SELECT "application_number_formatted", 
           "filing_date",
           flattened_assignee.value::STRING AS assignee_name
    FROM pub_a61,
    LATERAL FLATTEN(INPUT => pub_a61."assignee") AS flattened_assignee
    WHERE flattened_assignee.value IS NOT NULL
),
app_assignee_year AS (
    SELECT 
           "application_number_formatted" AS app_num,
           assignee_name,
           MIN(FLOOR("filing_date" / 10000)) AS filing_year
    FROM pub_assignee
    GROUP BY "application_number_formatted", assignee_name
),
top_assignee AS (
    SELECT assignee_name, COUNT(*) AS app_count
    FROM app_assignee_year
    GROUP BY assignee_name
    ORDER BY app_count DESC
    LIMIT 1
),
year_counts AS (
    SELECT filing_year, COUNT(*) AS year_count
    FROM app_assignee_year
    WHERE assignee_name = (SELECT assignee_name FROM top_assignee)
    GROUP BY filing_year
    ORDER BY year_count DESC
    LIMIT 1
)
SELECT filing_year
FROM year_counts