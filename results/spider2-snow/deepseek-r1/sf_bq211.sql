WITH families_with_multiple_apps AS (
    SELECT "family_id"
    FROM "PATENTS"."PATENTS"."PUBLICATIONS"
    WHERE "application_number_formatted" IS NOT NULL
    GROUP BY "family_id"
    HAVING COUNT(DISTINCT "application_number_formatted") > 1
)
SELECT COUNT(*)
FROM "PATENTS"."PATENTS"."PUBLICATIONS" p
WHERE p."country_code" = 'CN'
  AND p."grant_date" BETWEEN 20100101 AND 20231231
  AND p."family_id" IN (SELECT "family_id" FROM families_with_multiple_apps)