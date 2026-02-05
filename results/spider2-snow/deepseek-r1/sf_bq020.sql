WITH variant_counts AS (
    SELECT "reference_name", COUNT(DISTINCT "variant_id") AS variant_count
    FROM (
        SELECT "reference_name", "variant_id" 
        FROM "GENOMICS_CANNABIS"."GENOMICS_CANNABIS"."MNPR01_201703"
        UNION ALL
        SELECT "reference_name", "variant_id" 
        FROM "GENOMICS_CANNABIS"."GENOMICS_CANNABIS"."MNPR01_TRANSCRIPTOME_201703"
    ) AS all_variants
    GROUP BY "reference_name"
), reference_lengths AS (
    SELECT "name", "length" 
    FROM "GENOMICS_CANNABIS"."GENOMICS_CANNABIS"."MNPR01_REFERENCE_201703"
), density AS (
    SELECT vc."reference_name", 
           vc.variant_count * 1.0 / rl."length" AS density
    FROM variant_counts vc
    JOIN reference_lengths rl 
        ON vc."reference_name" = rl."name"
)
SELECT "reference_name"
FROM density
ORDER BY density DESC
LIMIT 1