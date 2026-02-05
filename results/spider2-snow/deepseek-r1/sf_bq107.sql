WITH LONGEST_REFERENCE AS (
  SELECT "name", "length"
  FROM "GENOMICS_CANNABIS"."GENOMICS_CANNABIS"."MNPR01_REFERENCE_201703"
  WHERE "length" = (SELECT MAX("length") FROM "GENOMICS_CANNABIS"."GENOMICS_CANNABIS"."MNPR01_REFERENCE_201703")
  LIMIT 1
),
VARIANT_COUNTS AS (
  SELECT "reference_name", COUNT(DISTINCT "variant_id") AS "variant_count"
  FROM (
    SELECT "reference_name", "variant_id", "call"
    FROM "GENOMICS_CANNABIS"."GENOMICS_CANNABIS"."MNPR01_201703"
    WHERE "reference_name" IN (SELECT "name" FROM LONGEST_REFERENCE)
    UNION ALL
    SELECT "reference_name", "variant_id", "call"
    FROM "GENOMICS_CANNABIS"."GENOMICS_CANNABIS"."MNPR01_TRANSCRIPTOME_201703"
    WHERE "reference_name" IN (SELECT "name" FROM LONGEST_REFERENCE)
  ) AS ALL_VARIANTS
  JOIN LATERAL FLATTEN(INPUT => "call") AS FLATTENED_CALL
  WHERE TRY_TO_NUMBER(FLATTENED_CALL."VALUE"::STRING) > 0
  GROUP BY "reference_name"
)
SELECT 
  LR."name" AS "reference_name",
  LR."length" AS "reference_length",
  COALESCE(VC."variant_count", 0) AS "variant_count",
  CASE 
    WHEN LR."length" > 0 THEN COALESCE(VC."variant_count", 0)::FLOAT / LR."length"
    ELSE 0 
  END AS "variant_density"
FROM LONGEST_REFERENCE LR
LEFT JOIN VARIANT_COUNTS VC ON LR."name" = VC."reference_name"