WITH longest_reference AS (
  SELECT "name", "length"
  FROM "GENOMICS_CANNABIS"."GENOMICS_CANNABIS"."MNPR01_REFERENCE_201703"
  ORDER BY "length" DESC
  LIMIT 1
),
all_variants AS (
  SELECT * FROM "GENOMICS_CANNABIS"."GENOMICS_CANNABIS"."MNPR01_201703"
  UNION ALL
  SELECT * FROM "GENOMICS_CANNABIS"."GENOMICS_CANNABIS"."MNPR01_TRANSCRIPTOME_201703"
),
present_variants AS (
  SELECT DISTINCT av."variant_id", av."reference_name"
  FROM all_variants av
  INNER JOIN LATERAL FLATTEN(INPUT => av."call") call_flat
  WHERE call_flat.value > 0
)
SELECT 
  lr."name" AS reference_name,
  COUNT(pv."variant_id") / lr."length" AS variant_density
FROM longest_reference lr
LEFT JOIN present_variants pv ON lr."name" = pv."reference_name"
GROUP BY lr."name", lr."length"