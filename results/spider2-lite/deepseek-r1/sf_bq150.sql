WITH tp53_expression AS (
  SELECT DISTINCT "sample_barcode", LOG(10, "normalized_count") AS log_expression
  FROM "TCGA_HG19_DATA_V0"."TCGA_HG19_DATA_V0"."RNASEQ_GENE_EXPRESSION_UNC_RSEM"
  WHERE "HGNC_gene_symbol" = 'TP53'
    AND "project_short_name" = 'TCGA-BRCA'
    AND "normalized_count" > 0
),
tp53_mutations AS (
  SELECT DISTINCT "sample_barcode_tumor", "Variant_Classification"
  FROM "TCGA_HG19_DATA_V0"."TCGA_HG19_DATA_V0"."SOMATIC_MUTATION_MC3"
  WHERE ("Hugo_Symbol" = 'TP53' OR "SYMBOL" = 'TP53')
    AND "project_short_name" = 'TCGA-BRCA'
  UNION
  SELECT DISTINCT "sample_barcode_tumor", "Variant_Classification"
  FROM "TCGA_HG19_DATA_V0"."TCGA_HG19_DATA_V0"."SOMATIC_MUTATION_DCC"
  WHERE "Hugo_Symbol" = 'TP53'
    AND "project_short_name" = 'TCGA-BRCA'
),
combined_data AS (
  SELECT DISTINCT e."sample_barcode", e.log_expression, m."Variant_Classification"
  FROM tp53_expression e
  JOIN tp53_mutations m ON e."sample_barcode" = m."sample_barcode_tumor"
),
group_stats AS (
  SELECT 
    "Variant_Classification",
    COUNT(*) AS n_j,
    AVG(log_expression) AS group_mean
  FROM combined_data
  GROUP BY "Variant_Classification"
),
overall_stats AS (
  SELECT 
    COUNT(*) AS total_samples,
    COUNT(DISTINCT "Variant_Classification") AS num_mutation_types,
    AVG(log_expression) AS grand_mean
  FROM combined_data
),
ss_calculations AS (
  SELECT 
    gs."Variant_Classification",
    gs.n_j,
    gs.group_mean,
    os.total_samples,
    os.num_mutation_types,
    os.grand_mean,
    gs.n_j * POWER(gs.group_mean - os.grand_mean, 2) AS ssb_contribution
  FROM group_stats gs
  CROSS JOIN overall_stats os
),
ssb_data AS (
  SELECT SUM(ssb_contribution) AS ssb
  FROM ss_calculations
),
ssw_calculations AS (
  SELECT 
    cd.log_expression,
    gs.group_mean,
    POWER(cd.log_expression - gs.group_mean, 2) AS deviation_sq
  FROM combined_data cd
  JOIN group_stats gs ON cd."Variant_Classification" = gs."Variant_Classification"
),
ssw_data AS (
  SELECT SUM(deviation_sq) AS ssw
  FROM ssw_calculations
),
final_stats AS (
  SELECT 
    os.total_samples,
    os.num_mutation_types,
    (sb.ssb / (os.num_mutation_types - 1)) AS mean_square_between,
    (sw.ssw / (os.total_samples - os.num_mutation_types)) AS mean_square_within,
    (sb.ssb / (os.num_mutation_types - 1)) / (sw.ssw / (os.total_samples - os.num_mutation_types)) AS f_statistic
  FROM overall_stats os
  CROSS JOIN ssb_data sb
  CROSS JOIN ssw_data sw
  WHERE os.num_mutation_types > 1 AND os.total_samples > os.num_mutation_types
)
SELECT 
  total_samples,
  num_mutation_types,
  mean_square_between,
  mean_square_within,
  f_statistic
FROM final_stats