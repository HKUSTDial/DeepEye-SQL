SELECT "sample_id", SUM("cnt") AS "count_positions"
FROM (
  SELECT cf.value:"call_set_name"::STRING AS "sample_id", COUNT(*) AS "cnt"
  FROM "HUMAN_GENOME_VARIANTS"."HUMAN_GENOME_VARIANTS"."SIMONS_GENOME_DIVERSITY_PROJECT_SAMPLE_VARIANTS" AS t,
  LATERAL FLATTEN(INPUT => t."call") AS cf
  WHERE ARRAY_SIZE(t."alternate_bases") = 1 AND cf.value:"genotype"::STRING IN ('0/0', '0|0')
  GROUP BY "sample_id"
  UNION ALL
  SELECT cf.value:"call_set_name"::STRING AS "sample_id", COUNT(*) AS "cnt"
  FROM "HUMAN_GENOME_VARIANTS"."HUMAN_GENOME_VARIANTS"."_1000_GENOMES_PHASE_3_OPTIMIZED_SCHEMA_VARIANTS_20150220" AS t,
  LATERAL FLATTEN(INPUT => t."call") AS cf
  WHERE ARRAY_SIZE(t."alternate_bases") = 1 AND cf.value:"genotype"::STRING IN ('0/0', '0|0')
  GROUP BY "sample_id"
  UNION ALL
  SELECT cf.value:"call_set_name"::STRING AS "sample_id", COUNT(*) AS "cnt"
  FROM "HUMAN_GENOME_VARIANTS"."HUMAN_GENOME_VARIANTS"."_1000_GENOMES_PHASE_3_VARIANTS_20150220" AS t,
  LATERAL FLATTEN(INPUT => t."call") AS cf
  WHERE ARRAY_SIZE(t."alternate_bases") = 1 AND cf.value:"genotype"::STRING IN ('0/0', '0|0')
  GROUP BY "sample_id"
  UNION ALL
  SELECT cf.value:"call_set_name"::STRING AS "sample_id", COUNT(*) AS "cnt"
  FROM "HUMAN_GENOME_VARIANTS"."HUMAN_GENOME_VARIANTS"."PLATINUM_GENOMES_DEEPVARIANT_VARIANTS_20180823" AS t,
  LATERAL FLATTEN(INPUT => t."call") AS cf
  WHERE ARRAY_SIZE(t."alternate_bases") = 1 AND cf.value:"genotype"::STRING IN ('0/0', '0|0')
  GROUP BY "sample_id"
) AS all_counts
GROUP BY "sample_id"
ORDER BY "count_positions" DESC
LIMIT 10