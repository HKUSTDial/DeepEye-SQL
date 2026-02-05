WITH filtered_variants AS (
  SELECT *
  FROM "_1000_GENOMES"."_1000_GENOMES"."VARIANTS"
  WHERE "reference_name" = '17' AND "start" >= 41196311 AND "start" <= 41277499
),
variants_with_alt AS (
  SELECT 
    f.*,
    alt.value::STRING AS "alternate_base",
    alt.index AS "alt_index"
  FROM filtered_variants f
  CROSS JOIN LATERAL FLATTEN(INPUT => f."alternate_bases") alt
),
genotype_data AS (
  SELECT 
    v."reference_name",
    v."start",
    v."end",
    v."reference_bases",
    v."alternate_base",
    v."VT",
    v."AF",
    v."ASN_AF",
    v."AFR_AF",
    v."EUR_AF",
    v."AMR_AF",
    v."alt_index",
    call.value:genotype[0]::INT AS "allele1",
    call.value:genotype[1]::INT AS "allele2"
  FROM variants_with_alt v
  CROSS JOIN LATERAL FLATTEN(INPUT => v."call") call
),
genotype_counts AS (
  SELECT 
    "reference_name",
    "start",
    "end",
    "reference_bases",
    "alternate_base",
    "VT",
    "AF",
    "ASN_AF",
    "AFR_AF",
    "EUR_AF",
    "AMR_AF",
    "alt_index",
    COUNT(*) AS "total_genotypes",
    SUM(CASE WHEN "allele1" = 0 AND "allele2" = 0 THEN 1 ELSE 0 END) AS "observed_hom_ref",
    SUM(CASE WHEN ("allele1" = 0 AND "allele2" = "alt_index" + 1) OR ("allele1" = "alt_index" + 1 AND "allele2" = 0) THEN 1 ELSE 0 END) AS "observed_het",
    SUM(CASE WHEN "allele1" = "alt_index" + 1 AND "allele2" = "alt_index" + 1 THEN 1 ELSE 0 END) AS "observed_hom_alt"
  FROM genotype_data
  WHERE ("allele1" = 0 OR "allele1" = "alt_index" + 1) AND ("allele2" = 0 OR "allele2" = "alt_index" + 1)
  GROUP BY "reference_name", "start", "end", "reference_bases", "alternate_base", "VT", "AF", "ASN_AF", "AFR_AF", "EUR_AF", "AMR_AF", "alt_index"
)
SELECT 
  "reference_name",
  "start",
  "end",
  "reference_bases",
  "alternate_base",
  "VT",
  "total_genotypes",
  "observed_hom_ref",
  "observed_het",
  "observed_hom_alt",
  (2 * "observed_hom_alt" + "observed_het") / (2 * "total_genotypes") AS "alt_allele_frequency",
  "total_genotypes" * POWER(1 - (2 * "observed_hom_alt" + "observed_het") / (2 * "total_genotypes"), 2) AS "expected_hom_ref",
  "total_genotypes" * 2 * ((2 * "observed_hom_alt" + "observed_het") / (2 * "total_genotypes")) * (1 - (2 * "observed_hom_alt" + "observed_het") / (2 * "total_genotypes")) AS "expected_het",
  "total_genotypes" * POWER((2 * "observed_hom_alt" + "observed_het") / (2 * "total_genotypes"), 2) AS "expected_hom_alt",
  (POWER("observed_hom_ref" - "total_genotypes" * POWER(1 - (2 * "observed_hom_alt" + "observed_het") / (2 * "total_genotypes"), 2), 2) / NULLIF("total_genotypes" * POWER(1 - (2 * "observed_hom_alt" + "observed_het") / (2 * "total_genotypes"), 2), 0)) +
  (POWER("observed_het" - "total_genotypes" * 2 * ((2 * "observed_hom_alt" + "observed_het") / (2 * "total_genotypes")) * (1 - (2 * "observed_hom_alt" + "observed_het") / (2 * "total_genotypes")), 2) / NULLIF("total_genotypes" * 2 * ((2 * "observed_hom_alt" + "observed_het") / (2 * "total_genotypes")) * (1 - (2 * "observed_hom_alt" + "observed_het") / (2 * "total_genotypes")), 0)) +
  (POWER("observed_hom_alt" - "total_genotypes" * POWER((2 * "observed_hom_alt" + "observed_het") / (2 * "total_genotypes"), 2), 2) / NULLIF("total_genotypes" * POWER((2 * "observed_hom_alt" + "observed_het") / (2 * "total_genotypes"), 2), 0)) AS "chi_squared",
  "AF",
  "ASN_AF",
  "AFR_AF",
  "EUR_AF",
  "AMR_AF"
FROM genotype_counts
ORDER BY "start", "alt_index"