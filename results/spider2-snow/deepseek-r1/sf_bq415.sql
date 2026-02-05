SELECT
  call_flat.VALUE:sample_id::STRING AS sample_id,
  COUNT(*) AS homozygous_reference_count
FROM "HUMAN_GENOME_VARIANTS"."HUMAN_GENOME_VARIANTS"."SIMONS_GENOME_DIVERSITY_PROJECT_SAMPLE_VARIANTS",
LATERAL FLATTEN(INPUT => "call") AS call_flat
WHERE ARRAY_SIZE("alternate_bases") = 1
  AND (call_flat.VALUE:GT::STRING IN ('0/0', '0|0'))
GROUP BY sample_id
ORDER BY homozygous_reference_count DESC
LIMIT 10