WITH first_office_action_rows AS (
  SELECT
    "app_id",
    "mail_dt",
    "allowed_claims",
    "rejection_101",
    ROW_NUMBER() OVER (PARTITION BY "app_id" ORDER BY "mail_dt") AS rn
  FROM "PATENTS_USPTO"."USPTO_OCE_OFFICE_ACTIONS"."OFFICE_ACTIONS"
),
first_office_actions AS (
  SELECT
    "app_id",
    "mail_dt" AS "first_office_action_date"
  FROM first_office_action_rows
  WHERE rn = 1
    AND "allowed_claims" = '0'
    AND "rejection_101" != '0'
),
granted_us_patents AS (
  SELECT
    foa."app_id",
    foa."first_office_action_date",
    ma."application_number",
    p."publication_number" AS "granted_publication_number",
    p."grant_date"
  FROM first_office_actions foa
  INNER JOIN "PATENTS_USPTO"."USPTO_OCE_OFFICE_ACTIONS"."MATCH_APP" ma
    ON foa."app_id" = ma."app_id"
  INNER JOIN "PATENTS_USPTO"."PATENTS"."PUBLICATIONS" p
    ON ma."application_number" = p."application_number"
  WHERE p."country_code" = 'US'
    AND p."grant_date" IS NOT NULL
    AND p."grant_date" >= 20100101
    AND p."grant_date" <= 20231231
),
first_publications AS (
  SELECT
    "application_number",
    MIN("publication_date") AS "first_publication_date"
  FROM "PATENTS_USPTO"."PATENTS"."PUBLICATIONS"
  GROUP BY "application_number"
),
first_publication_details AS (
  SELECT
    fp."application_number",
    fp."first_publication_date",
    MIN(p."publication_number") AS "first_publication_number"
  FROM first_publications fp
  INNER JOIN "PATENTS_USPTO"."PATENTS"."PUBLICATIONS" p
    ON fp."application_number" = p."application_number"
    AND fp."first_publication_date" = p."publication_date"
  GROUP BY fp."application_number", fp."first_publication_date"
),
all_patent_claims AS (
  SELECT "pat_no", "claim_txt"
  FROM "PATENTS_USPTO"."USPTO_OCE_CLAIMS"."PATENT_CLAIMS_FULLTEXT"
  UNION ALL
  SELECT "pat_no", "claim_txt"
  FROM "PATENTS_USPTO"."USPTO_OCE_CLAIMS"."PATENT_CLAIMS_FULLTEXT_2014"
),
patent_claims_length AS (
  SELECT
    "pat_no",
    SUM(LENGTH("claim_txt")) AS "claims_length"
  FROM all_patent_claims
  GROUP BY "pat_no"
)
SELECT
  SPLIT_PART(gup."granted_publication_number", '-', 2) AS "patent_number",
  fpd."first_publication_number",
  TO_DATE(CAST(fpd."first_publication_date" AS VARCHAR), 'YYYYMMDD') AS "first_publication_date",
  pcl."claims_length",
  TO_DATE(CAST(gup."grant_date" AS VARCHAR), 'YYYYMMDD') AS "grant_date",
  TO_DATE(gup."first_office_action_date", 'YYYY-MM-DD') AS "first_office_action_date"
FROM granted_us_patents gup
INNER JOIN first_publication_details fpd
  ON gup."application_number" = fpd."application_number"
INNER JOIN patent_claims_length pcl
  ON SPLIT_PART(gup."granted_publication_number", '-', 2) = pcl."pat_no"
ORDER BY pcl."claims_length" DESC
LIMIT 5