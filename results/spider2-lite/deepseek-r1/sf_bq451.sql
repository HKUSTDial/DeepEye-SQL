WITH filtered_variants AS (
  SELECT 
    "reference_name", 
    "start", 
    "call"
  FROM "_1000_GENOMES"."_1000_GENOMES"."VARIANTS"
  WHERE "reference_name" = 'X'
    AND "VT" = 'SNP'
    AND NOT (("start" BETWEEN 59999 AND 2699519) 
             OR ("start" BETWEEN 154931042 AND 155260559))
),
sample_calls AS (
  SELECT 
    call.value:"sample"::TEXT AS sample_id,
    call.value:"genotype" AS genotype_array
  FROM filtered_variants,
  LATERAL FLATTEN(INPUT => "call") AS call
  WHERE ARRAY_SIZE(call.value:"genotype") > 0
),
classified_genotypes AS (
  SELECT 
    sample_id,
    genotype_array,
    CASE 
      WHEN genotype_array[0]::INT = 0 AND genotype_array[1]::INT = 0 
        THEN 'homozygous_reference'
      WHEN genotype_array[0]::INT > 0 
        AND genotype_array[0]::INT = genotype_array[1]::INT 
        THEN 'homozygous_alternate'
      WHEN (genotype_array[0]::INT != genotype_array[1]::INT 
            OR genotype_array[0]::INT IS NULL 
            OR genotype_array[1]::INT IS NULL)
        AND (genotype_array[0]::INT > 0 OR genotype_array[1]::INT > 0)
        THEN 'heterozygous'
      ELSE 'other'
    END AS genotype_category
  FROM sample_calls
),
aggregated AS (
  SELECT 
    sample_id,
    COUNT(*) AS total_callable_sites,
    COUNT_IF(genotype_category = 'homozygous_reference') AS homozygous_reference_count,
    COUNT_IF(genotype_category = 'homozygous_alternate') AS homozygous_alternate_count,
    COUNT_IF(genotype_category = 'heterozygous') AS heterozygous_count
  FROM classified_genotypes
  WHERE genotype_category IN ('homozygous_reference', 'homozygous_alternate', 'heterozygous')
  GROUP BY sample_id
)
SELECT 
  sample_id,
  total_callable_sites,
  homozygous_reference_count,
  homozygous_alternate_count,
  heterozygous_count,
  homozygous_alternate_count + heterozygous_count AS total_snvs,
  CASE 
    WHEN homozygous_alternate_count + heterozygous_count > 0 
    THEN (heterozygous_count * 100.0) / (homozygous_alternate_count + heterozygous_count)
    ELSE 0 
  END AS percent_heterozygous_among_snvs,
  CASE 
    WHEN homozygous_alternate_count + heterozygous_count > 0 
    THEN (homozygous_alternate_count * 100.0) / (homozygous_alternate_count + heterozygous_count)
    ELSE 0 
  END AS percent_homozygous_alternate_among_snvs
FROM aggregated
ORDER BY 
  percent_heterozygous_among_snvs DESC,
  sample_id