WITH "google_data" AS (
  SELECT 
    "race_asian",
    "race_white",
    "race_hispanic_latinx",
    "race_black"
  FROM "GOOGLE_DEI"."GOOGLE_DEI"."DAR_NON_INTERSECTIONAL_HIRING"
  WHERE "report_year" = 2021 AND "workforce" = 'overall'
),
"bls_avg" AS (
  SELECT 
    AVG("percent_asian") AS "avg_asian",
    AVG("percent_white") AS "avg_white",
    AVG("percent_hispanic_or_latino") AS "avg_hispanic_latino",
    AVG("percent_black_or_african_american") AS "avg_black"
  FROM "GOOGLE_DEI"."BLS"."CPSAAT18"
  WHERE "year" = 2021
    AND (
      "sector" IN ('Internet publishing and broadcasting and web search portals', 'Software publishers', 'Data processing, hosting, and related services')
      OR "industry_group" = 'Computer systems design and related services'
    )
)
SELECT 
  'Asian' AS "race",
  "g"."race_asian" - "b"."avg_asian" AS "difference"
FROM "google_data" "g", "bls_avg" "b"
UNION ALL
SELECT 
  'White' AS "race",
  "g"."race_white" - "b"."avg_white" AS "difference"
FROM "google_data" "g", "bls_avg" "b"
UNION ALL
SELECT 
  'Hispanic/Latino' AS "race",
  "g"."race_hispanic_latinx" - "b"."avg_hispanic_latino" AS "difference"
FROM "google_data" "g", "bls_avg" "b"
UNION ALL
SELECT 
  'Black' AS "race",
  "g"."race_black" - "b"."avg_black" AS "difference"
FROM "google_data" "g", "bls_avg" "b"
ORDER BY ABS("difference") DESC
LIMIT 3