WITH counts AS (
    SELECT "zip_code", COUNT(*) AS institution_count
    FROM "FDA"."FDIC_BANKS"."INSTITUTIONS"
    WHERE "state" = 'FL' AND "fdic_insured" = TRUE
    GROUP BY "zip_code"
)
SELECT "zip_code"
FROM counts
WHERE institution_count = (SELECT MAX(institution_count) FROM counts)