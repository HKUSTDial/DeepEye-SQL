WITH CPC_FILTERED AS (
  SELECT 
    p."application_number",
    p."publication_date",
    p."country_code",
    TRIM(flattened_assignee.value) AS "assignee_name"
  FROM 
    "PATENTS"."PATENTS"."PUBLICATIONS" p,
    LATERAL FLATTEN(INPUT => p."cpc") AS flattened_cpc,
    LATERAL FLATTEN(INPUT => p."assignee") AS flattened_assignee
  WHERE 
    flattened_cpc.value::STRING LIKE '%A01B3%'
),
ASSIGNEE_TOTALS AS (
  SELECT 
    "assignee_name",
    COUNT(DISTINCT "application_number") AS "total_applications"
  FROM 
    CPC_FILTERED
  GROUP BY 
    "assignee_name"
),
YEAR_COUNTS AS (
  SELECT 
    "assignee_name",
    LEFT(CAST("publication_date" AS STRING), 4) AS "year",
    COUNT(DISTINCT "application_number") AS "applications_in_year"
  FROM 
    CPC_FILTERED
  GROUP BY 
    "assignee_name",
    "year"
),
TOP_YEAR_PER_ASSIGNEE AS (
  SELECT 
    "assignee_name",
    "year" AS "top_year",
    "applications_in_year" AS "applications_in_top_year",
    ROW_NUMBER() OVER (PARTITION BY "assignee_name" ORDER BY "applications_in_year" DESC, "year" DESC) AS "rn"
  FROM 
    YEAR_COUNTS
),
COUNTRY_COUNTS AS (
  SELECT 
    cf."assignee_name",
    LEFT(CAST(cf."publication_date" AS STRING), 4) AS "year",
    cf."country_code",
    COUNT(DISTINCT cf."application_number") AS "applications_in_country"
  FROM 
    CPC_FILTERED cf
  GROUP BY 
    cf."assignee_name",
    "year",
    cf."country_code"
),
TOP_COUNTRY_PER_YEAR AS (
  SELECT 
    "assignee_name",
    "year",
    "country_code" AS "top_country_code",
    ROW_NUMBER() OVER (PARTITION BY "assignee_name", "year" ORDER BY "applications_in_country" DESC, "country_code") AS "rn"
  FROM 
    COUNTRY_COUNTS
)
SELECT 
  at."assignee_name",
  at."total_applications",
  ty."top_year" AS "year_with_most_applications",
  ty."applications_in_top_year" AS "applications_in_that_year",
  tc."top_country_code" AS "country_code_with_most_applications_during_that_year"
FROM 
  ASSIGNEE_TOTALS at
LEFT JOIN 
  TOP_YEAR_PER_ASSIGNEE ty ON at."assignee_name" = ty."assignee_name" AND ty."rn" = 1
LEFT JOIN 
  TOP_COUNTRY_PER_YEAR tc ON at."assignee_name" = tc."assignee_name" AND ty."top_year" = tc."year" AND tc."rn" = 1
ORDER BY 
  at."total_applications" DESC
LIMIT 3