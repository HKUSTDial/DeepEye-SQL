WITH filtered_variants AS (
  SELECT 
    "start",
    "call"
  FROM "_1000_GENOMES"."_1000_GENOMES"."VARIANTS"
  WHERE "reference_name" = 'X'
    AND "VT" = 'SNP'
    AND NOT ("start" BETWEEN 59999 AND 2699519)
    AND NOT ("start" BETWEEN 154931042 AND 155260559)
),
flattened_calls AS (
  SELECT 
    call_obj."VALUE":"sample_id"::TEXT AS sample_id,
    call_obj."VALUE":"genotype" AS genotype_array
  FROM filtered_variants v,
  LATERAL FLATTEN(INPUT => v."call") call_obj
  WHERE ARRAY_SIZE(call_obj."VALUE":"genotype") > 0
),
alleles_extracted AS (
  SELECT 
    sample_id,
    CASE WHEN ARRAY_SIZE(genotype_array) >= 1 THEN genotype_array[0]::INT ELSE NULL END AS allele1,
    CASE WHEN ARRAY_SIZE(genotype_array) >= 2 THEN genotype_array[1]::INT ELSE NULL END AS allele2
  FROM flattened_calls
),
classified_calls AS (
  SELECT 
    sample_id,
    CASE 
      WHEN allele1 = 0 AND allele2 = 0 THEN 'homozygous_reference'
      WHEN allele1 = allele2 AND allele1 > 0 THEN 'homozygous_alternate'
      WHEN (allele1 != allele2 OR allele1 IS NULL OR allele2 IS NULL) 
           AND (allele1 > 0 OR allele2 > 0) THEN 'heterozygous'
      ELSE 'other'
    END AS genotype_category
  FROM alleles_extracted
  WHERE allele1 IS NOT NULL OR allele2 IS NOT NULL
),
aggregated AS (
  SELECT 
    sample_id,
    COUNT(*) AS total_callable_sites,
    SUM(CASE WHEN genotype_category = 'homozygous_reference' THEN 1 ELSE 0 END) AS homozygous_reference_count,
    SUM(CASE WHEN genotype_category = 'homozygous_alternate' THEN 1 ELSE 0 END) AS homozygous_alternate_count,
    SUM(CASE WHEN genotype_category = 'heterozygous' THEN 1 ELSE 0 END) AS heterozygous_count
  FROM classified_calls
  WHERE genotype_category != 'other'
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
    THEN heterozygous_count * 100.0 / (homozygous_alternate_count + heterozygous_count) 
    ELSE NULL 
  END AS heterozygous_percentage,
  CASE 
    WHEN homozygous_alternate_count + heterozygous_count > 0 
    THEN homozygous_alternate_count * 100.0 / (homozygous_alternate_count + heterozygous_count) 
    ELSE NULL 
  END AS homozygous_alternate_percentage
FROM aggregated
ORDER BY heterozygous_percentage DESC, sample_id;