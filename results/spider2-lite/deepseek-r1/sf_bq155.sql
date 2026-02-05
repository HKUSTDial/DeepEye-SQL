WITH cohort_cases AS (
    SELECT DISTINCT "case_barcode"
    FROM "TCGA_HG38_DATA_V0"."TCGA_BIOCLIN_V0"."CLINICAL"
    WHERE "project_short_name" = 'TCGA-BRCA'
        AND "age_at_diagnosis" <= 80
        AND "age_at_diagnosis" IS NOT NULL
        AND "pathologic_stage" IN ('Stage I', 'Stage II', 'Stage IIA')
),
snora31_expr AS (
    SELECT "case_barcode", LOG(10, AVG("HTSeq__Counts") + 1) AS snora31_log_expr
    FROM "TCGA_HG38_DATA_V0"."TCGA_HG38_DATA_V0"."RNASEQ_GENE_EXPRESSION"
    WHERE "project_short_name" = 'TCGA-BRCA'
        AND "gene_name" = 'SNORA31'
    GROUP BY "case_barcode"
),
mirna_expr AS (
    SELECT "case_barcode", "mirna_id", AVG("reads_per_million_miRNA_mapped") AS mirna_avg_expr
    FROM "TCGA_HG38_DATA_V0"."TCGA_HG38_DATA_V0"."MIRNASEQ_EXPRESSION"
    WHERE "project_short_name" = 'TCGA-BRCA'
    GROUP BY "case_barcode", "mirna_id"
),
joined_data AS (
    SELECT m."mirna_id", s.snora31_log_expr, m.mirna_avg_expr
    FROM cohort_cases c
    INNER JOIN snora31_expr s ON c."case_barcode" = s."case_barcode"
    INNER JOIN mirna_expr m ON c."case_barcode" = m."case_barcode"
),
corr_data AS (
    SELECT "mirna_id", CORR(snora31_log_expr, mirna_avg_expr) AS pearson_corr, COUNT(*) AS sample_count
    FROM joined_data
    GROUP BY "mirna_id"
)
SELECT "mirna_id", pearson_corr, sample_count, CASE WHEN ABS(pearson_corr) < 1 THEN pearson_corr * SQRT((sample_count - 2) / (1 - pearson_corr * pearson_corr)) ELSE NULL END AS t_statistic
FROM corr_data
WHERE sample_count > 25 AND ABS(pearson_corr) BETWEEN 0.3 AND 1.0