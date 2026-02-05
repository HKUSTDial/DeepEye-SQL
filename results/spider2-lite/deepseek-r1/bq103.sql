WITH filtered_variants AS (
  SELECT
    `AN`,
    alternate_bases
  FROM `bigquery-public-data.gnomAD.v3_genomes__chr1`
  WHERE reference_name = 'chr1'
    AND start_position >= 55039446
    AND start_position < 55064852
),
variant_stats AS (
  SELECT
    COUNT(*) AS num_variants,
    SUM(`AN`) AS total_AN
  FROM filtered_variants
),
alt_stats AS (
  SELECT
    SUM(alt.AC) AS total_AC
  FROM filtered_variants
  CROSS JOIN UNNEST(alternate_bases) AS alt
),
gene_stats AS (
  SELECT
    COUNT(DISTINCT vep.SYMBOL) AS distinct_genes
  FROM filtered_variants
  CROSS JOIN UNNEST(alternate_bases) AS alt
  CROSS JOIN UNNEST(alt.vep) AS vep
  WHERE vep.SYMBOL IS NOT NULL AND vep.SYMBOL != ''
)
SELECT
  variant_stats.num_variants AS number_of_variants,
  alt_stats.total_AC AS total_allele_count,
  variant_stats.total_AN AS total_number_of_alleles,
  gene_stats.distinct_genes AS distinct_gene_symbols,
  25406.0 / variant_stats.num_variants AS mutation_density
FROM variant_stats, alt_stats, gene_stats