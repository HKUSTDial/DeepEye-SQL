WITH expr_per_sample AS (
  SELECT "sample_barcode", LOG(10, AVG("normalized_count")) AS log_expr
  FROM "TCGA_HG19_DATA_V0"."TCGA_HG19_DATA_V0"."RNASEQ_GENE_EXPRESSION_UNC_RSEM"
  WHERE "project_short_name" = 'TCGA-BRCA'
    AND "HGNC_gene_symbol" = 'TP53'
    AND "normalized_count" > 0
  GROUP BY "sample_barcode"
),
mut_per_sample AS (
  SELECT "sample_barcode_tumor", MIN("Variant_Classification") AS mutation_type
  FROM "TCGA_HG19_DATA_V0"."TCGA_HG19_DATA_V0"."SOMATIC_MUTATION_MC3"
  WHERE "project_short_name" = 'TCGA-BRCA'
    AND ("Hugo_Symbol" = 'TP53' OR "SYMBOL" = 'TP53')
  GROUP BY "sample_barcode_tumor"
),
joined_data AS (
  SELECT e."sample_barcode", e.log_expr, m.mutation_type
  FROM expr_per_sample e
  INNER JOIN mut_per_sample m
    ON e."sample_barcode" = m."sample_barcode_tumor"
),
group_stats AS (
  SELECT mutation_type,
         COUNT(*) AS group_size,
         AVG(log_expr) AS group_mean
  FROM joined_data
  GROUP BY mutation_type
),
overall_stats AS (
  SELECT COUNT(*) AS total_samples,
         COUNT(DISTINCT mutation_type) AS num_groups,
         AVG(log_expr) AS grand_mean
  FROM joined_data
),
anova_components AS (
  SELECT o.total_samples,
         o.num_groups,
         SUM(g.group_size * POWER(g.group_mean - o.grand_mean, 2)) AS ssb,
         SUM(POWER(d.log_expr - g.group_mean, 2)) AS ssw,
         o.num_groups - 1 AS df_between,
         o.total_samples - o.num_groups AS df_within
  FROM overall_stats o
  CROSS JOIN group_stats g
  INNER JOIN joined_data d ON g.mutation_type = d.mutation_type
  GROUP BY o.total_samples, o.num_groups, o.grand_mean
)
SELECT total_samples AS total_number_of_samples,
       num_groups AS number_of_mutation_types,
       ssb / NULLIF(df_between, 0) AS mean_square_between,
       ssw / NULLIF(df_within, 0) AS mean_square_within,
       (ssb / NULLIF(df_between, 0)) / NULLIF(ssw / NULLIF(df_within, 0), 0) AS f_statistic
FROM anova_components;