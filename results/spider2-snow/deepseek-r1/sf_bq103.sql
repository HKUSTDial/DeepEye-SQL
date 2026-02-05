WITH region_variants AS (
  SELECT
    "start_position",
    "AN",
    "alternate_bases"
  FROM "GNOMAD"."GNOMAD"."V3_GENOMES__CHR1"
  WHERE "start_position" <= 55064852
    AND "end_position" >= 55039447
),
alternate_flattened AS (
  SELECT
    rv."start_position",
    rv."AN",
    alt.value:AC::NUMBER AS AC,
    alt.value:vep AS vep
  FROM region_variants rv,
  LATERAL FLATTEN(INPUT => rv."alternate_bases") alt
),
vep_flattened AS (
  SELECT
    af."start_position",
    af.AC,
    vep.value:SYMBOL::STRING AS gene_symbol
  FROM alternate_flattened af
  LEFT JOIN LATERAL FLATTEN(INPUT => af.vep) vep
)
SELECT
  (SELECT COUNT(*) FROM region_variants) AS number_of_variants,
  (SELECT SUM(AC) FROM alternate_flattened) AS total_allele_count,
  (SELECT SUM("AN") FROM region_variants) AS total_number_of_alleles,
  (SELECT COUNT(DISTINCT gene_symbol) FROM vep_flattened WHERE gene_symbol IS NOT NULL) AS distinct_gene_symbols,
  25406 / NULLIF((SELECT COUNT(*) FROM region_variants), 0) AS mutation_density;