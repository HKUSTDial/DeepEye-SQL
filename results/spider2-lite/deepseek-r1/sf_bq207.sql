WITH independent_claims AS (
    SELECT "appl_id", "pat_no", "claim_no", "word_ct"
    FROM "PATENTS_USPTO"."USPTO_OCE_CLAIMS"."PATENT_CLAIMS_STATS"
    WHERE "ind_flg" = '1'
),
claims_with_pub AS (
    SELECT ic."appl_id", ic."pat_no", ic."claim_no", ic."word_ct", m."publication_number"
    FROM independent_claims ic
    JOIN "PATENTS_USPTO"."USPTO_OCE_CLAIMS"."MATCH" m ON ic."pat_no" = m."pat_no"
),
claims_with_pub_app AS (
    SELECT cp."appl_id", cp."pat_no", cp."claim_no", cp."word_ct", cp."publication_number",
           p."application_number", p."publication_date"
    FROM claims_with_pub cp
    JOIN "PATENTS_USPTO"."PATENTS"."PUBLICATIONS" p ON cp."publication_number" = p."publication_number"
),
ranked_claims AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY "application_number" ORDER BY "publication_date" ASC) AS rn
    FROM claims_with_pub_app
)
SELECT "publication_number", "application_number", "claim_no", "word_ct"
FROM ranked_claims
WHERE rn = 1
ORDER BY CAST("word_ct" AS INTEGER) DESC
LIMIT 100