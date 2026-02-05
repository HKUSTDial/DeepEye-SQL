SELECT 
  "reference_bases",
  MIN("start_position") AS "min_start_position",
  MAX("start_position") AS "max_start_position",
  COUNT(*) AS "count",
  COUNT(*) * 1.0 / SUM(COUNT(*)) OVER () AS "proportion"
FROM "HUMAN_GENOME_VARIANTS"."HUMAN_GENOME_VARIANTS"."_1000_GENOMES_PHASE_3_OPTIMIZED_SCHEMA_VARIANTS_20150220"
WHERE "reference_bases" IN ('AT', 'TA')
GROUP BY "reference_bases"