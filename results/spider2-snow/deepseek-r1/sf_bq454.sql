WITH autosomal_variants AS (
  SELECT
    "reference_name",
    "start",
    "end",
    "reference_bases",
    "alternate_bases",
    "AFR_AF",
    "EUR_AF",
    "ASN_AF",
    "AMR_AF",
    "call"
  FROM "_1000_GENOMES"."_1000_GENOMES"."VARIANTS"
  WHERE "reference_name" NOT IN ('X', 'Y', 'MT')
),
flattened_calls AS (
  SELECT
    av."reference_name",
    av."start",
    av."end",
    av."reference_bases",
    av."alternate_bases",
    av."AFR_AF",
    av."EUR_AF",
    av."ASN_AF",
    av."AMR_AF",
    cf."VALUE":"sample_id"::STRING AS sample_id,
    cf."VALUE":"GT"::ARRAY AS genotype
  FROM autosomal_variants av,
  LATERAL FLATTEN(INPUT => av."call") cf
  WHERE (genotype[0]::INT > 0 OR genotype[1]::INT > 0)
),
joined_with_sample_info AS (
  SELECT
    fc."reference_name",
    fc."start",
    fc."end",
    fc."reference_bases",
    fc."alternate_bases",
    fc."AFR_AF",
    fc."EUR_AF",
    fc."ASN_AF",
    fc."AMR_AF",
    fc.sample_id,
    si."Super_Population"
  FROM flattened_calls fc
  JOIN "_1000_GENOMES"."_1000_GENOMES"."SAMPLE_INFO" si ON fc.sample_id = si."Sample"
),
variant_superpop_counts AS (
  SELECT
    "reference_name",
    "start",
    "end",
    "reference_bases",
    "alternate_bases",
    "Super_Population",
    COUNT(DISTINCT sample_id) AS sample_count,
    CASE "Super_Population"
      WHEN 'AFR' THEN "AFR_AF" >= 0.05
      WHEN 'EUR' THEN "EUR_AF" >= 0.05
      WHEN 'ASN' THEN "ASN_AF" >= 0.05
      WHEN 'AMR' THEN "AMR_AF" >= 0.05
      ELSE FALSE
    END AS is_common
  FROM joined_with_sample_info
  GROUP BY
    "reference_name",
    "start",
    "end",
    "reference_bases",
    "alternate_bases",
    "Super_Population",
    "AFR_AF",
    "EUR_AF",
    "ASN_AF",
    "AMR_AF"
),
superpop_total AS (
  SELECT
    "Super_Population",
    COUNT(DISTINCT "Sample") AS total_population_size
  FROM "_1000_GENOMES"."_1000_GENOMES"."SAMPLE_INFO"
  GROUP BY "Super_Population"
)
SELECT
  vsc."Super_Population",
  st.total_population_size,
  vsc.is_common,
  vsc.sample_count,
  COUNT(*) AS variant_count
FROM variant_superpop_counts vsc
JOIN superpop_total st ON vsc."Super_Population" = st."Super_Population"
GROUP BY vsc."Super_Population", st.total_population_size, vsc.is_common, vsc.sample_count
ORDER BY vsc."Super_Population", vsc.is_common, vsc.sample_count