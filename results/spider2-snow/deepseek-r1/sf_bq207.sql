WITH independent_claims AS (
    SELECT "pat_no", "claim_no", "word_ct"
    FROM "PATENTS_USPTO"."USPTO_OCE_CLAIMS"."PATENT_CLAIMS_STATS"
    WHERE "ind_flg" = '1'
    UNION ALL
    SELECT "pat_no", "claim_no", "word_ct"
    FROM "PATENTS_USPTO"."USPTO_OCE_CLAIMS"."PATENT_CLAIMS_STATS_2014"
    WHERE "ind_flg" = '1'
),
claims_with_publication AS (
    SELECT ic."pat_no", ic."claim_no", ic."word_ct", m."publication_number"
    FROM independent_claims ic
    JOIN (
        SELECT "pat_no", "publication_number"
        FROM "PATENTS_USPTO"."USPTO_OCE_CLAIMS"."MATCH"
        UNION ALL
        SELECT "pat_no", "publication_number"
        FROM "PATENTS_USPTO"."USPTO_OCE_CLAIMS"."MATCH_2014"
    ) m ON ic."pat_no" = m."pat_no"
),
claims_with_publication_info AS (
    SELECT cp."pat_no", cp."claim_no", cp."word_ct", cp."publication_number", p."application_number", p."publication_date"
    FROM claims_with_publication cp
    JOIN "PATENTS_USPTO"."PATENTS"."PUBLICATIONS" p ON cp."publication_number" = p."publication_number"
),
earliest_publication_per_app AS (
    SELECT "application_number", MIN("publication_date") as "earliest_date"
    FROM "PATENTS_USPTO"."PATENTS"."PUBLICATIONS"
    GROUP BY "application_number"
)
SELECT 
    cpi."publication_number" as "earliest_publication_number",
    cpi."application_number",
    cpi."claim_no",
    cpi."word_ct"
FROM claims_with_publication_info cpi
JOIN earliest_publication_per_app epa 
    ON cpi."application_number" = epa."application_number" 
    AND cpi."publication_date" = epa."earliest_date"
ORDER BY CAST(cpi."word_ct" AS INTEGER) DESC
LIMIT 100