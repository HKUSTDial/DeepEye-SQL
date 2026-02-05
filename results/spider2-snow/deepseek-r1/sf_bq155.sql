WITH cohort AS (
  SELECT "case_barcode"
  FROM "TCGA_HG38_DATA_V0"."TCGA_BIOCLIN_V0"."CLINICAL"
  WHERE "project_short_name" = 'TCGA-BRCA'
    AND "age_at_diagnosis" <= 80
    AND "pathologic_stage" IN ('Stage I', 'Stage II', 'Stage IIA')
),
rna_avg AS (
  SELECT r."case_barcode", AVG(LOG(10, r."HTSeq__Counts" + 1)) AS avg_log_expr
  FROM "TCGA_HG38_DATA_V0"."TCGA_HG38_DATA_V0"."RNASEQ_GENE_EXPRESSION" r
  INNER JOIN cohort c ON r."case_barcode" = c."case_barcode"
  WHERE r."project_short_name" = 'TCGA-BRCA'
    AND r."gene_name" = 'SNORA31'
  GROUP BY r."case_barcode"
),
mirna_avg AS (
  SELECT m."case_barcode", m."mirna_id", AVG(m."reads_per_million_miRNA_mapped") AS avg_mirna_expr
  FROM "TCGA_HG38_DATA_V0"."TCGA_HG38_DATA_V0"."MIRNASEQ_EXPRESSION" m
  INNER JOIN cohort c ON m."case_barcode" = c."case_barcode"
  WHERE m."project_short_name" = 'TCGA-BRCA'
  GROUP BY m."case_barcode", m."mirna_id"
),
correlation_data AS (
  SELECT ma."mirna_id",
         CORR(ra.avg_log_expr, ma.avg_mirna_expr) AS pearson_corr,
         COUNT(*) AS sample_count
  FROM mirna_avg ma
  INNER JOIN rna_avg ra ON ma."case_barcode" = ra."case_barcode"
  GROUP BY ma."mirna_id"
  HAVING COUNT(*) > 25 AND ABS(pearson_corr) BETWEEN 0.3 AND 1.0
)
SELECT "mirna_id",
       pearson_corr * SQRT((sample_count - 2) / (1 - pearson_corr * pearson_corr)) AS t_statistic
FROM correlation_data