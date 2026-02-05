WITH applications_with_cpc AS (
  SELECT DISTINCT "application_number"
  FROM "PATENTS"."PATENTS"."PUBLICATIONS"
  , LATERAL FLATTEN(INPUT => "cpc") AS cpc_item
  WHERE cpc_item.value LIKE 'A01B3%'
    AND "application_number" IS NOT NULL
),
application_assignees AS (
  SELECT 
    a."application_number",
    TRIM(assignee_item.value::STRING, '" ') AS assignee_name,
    a."filing_date",
    a."country_code"
  FROM "PATENTS"."PATENTS"."PUBLICATIONS" a
  INNER JOIN applications_with_cpc b ON a."application_number" = b."application_number"
  , LATERAL FLATTEN(INPUT => a."assignee") AS assignee_item
  WHERE a."application_number" IS NOT NULL
    AND assignee_item.value IS NOT NULL
),
assignee_totals AS (
  SELECT 
    assignee_name,
    COUNT(DISTINCT "application_number") AS total_applications
  FROM application_assignees
  GROUP BY assignee_name
),
assignee_yearly AS (
  SELECT 
    assignee_name,
    SUBSTRING(CAST("filing_date" AS STRING), 1, 4) AS year,
    COUNT(DISTINCT "application_number") AS yearly_applications
  FROM application_assignees
  WHERE "filing_date" IS NOT NULL
  GROUP BY assignee_name, year
),
assignee_peak_year AS (
  SELECT 
    assignee_name,
    year AS peak_year,
    yearly_applications AS applications_in_peak_year,
    ROW_NUMBER() OVER (PARTITION BY assignee_name ORDER BY yearly_applications DESC, year DESC) AS rn
  FROM assignee_yearly
),
assignee_country_in_peak_year AS (
  SELECT 
    ap.assignee_name,
    ap.peak_year,
    aa."country_code",
    COUNT(DISTINCT aa."application_number") AS country_applications
  FROM application_assignees aa
  INNER JOIN assignee_peak_year ap 
    ON aa.assignee_name = ap.assignee_name 
    AND SUBSTRING(CAST(aa."filing_date" AS STRING), 1, 4) = ap.peak_year
  WHERE ap.rn = 1
  GROUP BY ap.assignee_name, ap.peak_year, aa."country_code"
),
assignee_top_country AS (
  SELECT 
    assignee_name,
    peak_year,
    "country_code" AS top_country_code,
    country_applications,
    ROW_NUMBER() OVER (PARTITION BY assignee_name, peak_year ORDER BY country_applications DESC, "country_code") AS rn_country
  FROM assignee_country_in_peak_year
)
SELECT 
  at.assignee_name,
  at.total_applications,
  apy.peak_year,
  apy.applications_in_peak_year,
  atc.top_country_code
FROM assignee_totals at
LEFT JOIN assignee_peak_year apy ON at.assignee_name = apy.assignee_name AND apy.rn = 1
LEFT JOIN assignee_top_country atc ON at.assignee_name = atc.assignee_name AND atc.rn_country = 1
ORDER BY at.total_applications DESC
LIMIT 3