WITH rejected_apps AS (
    SELECT 
        "app_id", 
        MIN("mail_dt") AS "first_office_action_date"
    FROM "PATENTS_USPTO"."USPTO_OCE_OFFICE_ACTIONS"."OFFICE_ACTIONS"
    WHERE "rejection_101" = '1' AND "allowed_claims" = '0'
    GROUP BY "app_id"
),
granted_us_patents AS (
    SELECT 
        "application_number",
        MIN("publication_number") AS "first_publication_number",
        MIN("publication_date") AS "first_publication_date",
        "grant_date"
    FROM "PATENTS_USPTO"."PATENTS"."PUBLICATIONS"
    WHERE "country_code" = 'US' 
        AND "grant_date" BETWEEN 20100000 AND 20231231
    GROUP BY "application_number", "grant_date"
),
patent_claim_lengths AS (
    SELECT 
        "pat_no",
        SUM(LENGTH("claim_txt")) AS "claim_length"
    FROM "PATENTS_USPTO"."USPTO_OCE_CLAIMS"."PATENT_CLAIMS_FULLTEXT"
    GROUP BY "pat_no"
)
SELECT DISTINCT
    pds."pat_no",
    ra."first_office_action_date",
    gup."first_publication_number",
    gup."first_publication_date",
    pcl."claim_length",
    gup."grant_date"
FROM rejected_apps ra
JOIN "PATENTS_USPTO"."USPTO_OCE_OFFICE_ACTIONS"."MATCH_APP" ma 
    ON ra."app_id" = ma."app_id"
JOIN granted_us_patents gup 
    ON ma."application_number" = gup."application_number"
JOIN "PATENTS_USPTO"."USPTO_OCE_CLAIMS"."PATENT_DOCUMENT_STATS" pds 
    ON ra."app_id" = pds."appl_id"
JOIN patent_claim_lengths pcl 
    ON pds."pat_no" = pcl."pat_no"
ORDER BY pcl."claim_length" DESC
LIMIT 5