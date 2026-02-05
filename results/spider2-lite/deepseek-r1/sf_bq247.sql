WITH "top_families" AS (
  SELECT "family_id", COUNT(*) AS "pub_count"
  FROM "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS"
  WHERE "family_id" != '-1'
  GROUP BY "family_id"
  ORDER BY "pub_count" DESC
  LIMIT 6
)
SELECT DISTINCT "top_families"."family_id", "ABS_AND_EMB"."abstract"
FROM "top_families"
INNER JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."PUBLICATIONS" ON "top_families"."family_id" = "PUBLICATIONS"."family_id"
INNER JOIN "PATENTS_GOOGLE"."PATENTS_GOOGLE"."ABS_AND_EMB" ON "PUBLICATIONS"."publication_number" = "ABS_AND_EMB"."publication_number"
WHERE "ABS_AND_EMB"."abstract" IS NOT NULL AND TRIM("ABS_AND_EMB"."abstract") != ''
ORDER BY "top_families"."family_id"