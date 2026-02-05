WITH total AS (
  SELECT COUNT(*) AS total_count
  FROM "HUMAN_GENOME_VARIANTS"."HUMAN_GENOME_VARIANTS"."_1000_GENOMES_PHASE_3_VARIANTS_20150220"
),
stats AS (
  SELECT 
    "reference_bases",
    MIN("start_position") AS min_start,
    MAX("start_position") AS max_start,
    COUNT(*) AS cnt
  FROM "HUMAN_GENOME_VARIANTS"."HUMAN_GENOME_VARIANTS"."_1000_GENOMES_PHASE_3_VARIANTS_20150220"
  WHERE "reference_bases" IN ('AT', 'TA')
  GROUP BY "reference_bases"
)
SELECT 
  stats."reference_bases",
  stats.min_start,
  stats.max_start,
  stats.cnt * 1.0 / total.total_count AS proportion
FROM stats, total