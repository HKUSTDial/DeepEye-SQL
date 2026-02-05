WITH mutated_samples AS (
    SELECT DISTINCT "case_barcode", "sample_barcode_tumor"
    FROM "TCGA"."TCGA_VERSIONED"."SOMATIC_MUTATION_HG19_MC3_2017_02"
    WHERE "project_short_name" = 'TCGA-BLCA'
      AND ("Hugo_Symbol" = 'CDKN2A' OR "SYMBOL" = 'CDKN2A')
),
expression_data AS (
    SELECT 
        mut."case_barcode",
        mut."sample_barcode_tumor",
        MAX(CASE WHEN expr."HGNC_gene_symbol" = 'MDM2' THEN expr."normalized_count" END) AS MDM2_expr,
        MAX(CASE WHEN expr."HGNC_gene_symbol" = 'TP53' THEN expr."normalized_count" END) AS TP53_expr,
        MAX(CASE WHEN expr."HGNC_gene_symbol" = 'CDKN1A' THEN expr."normalized_count" END) AS CDKN1A_expr,
        MAX(CASE WHEN expr."HGNC_gene_symbol" = 'CCNE1' THEN expr."normalized_count" END) AS CCNE1_expr
    FROM mutated_samples AS mut
    JOIN "TCGA"."TCGA_VERSIONED"."RNASEQ_HG19_GDC_2017_02" AS expr
        ON expr."sample_barcode" = mut."sample_barcode_tumor"
       AND expr."HGNC_gene_symbol" IN ('MDM2', 'TP53', 'CDKN1A', 'CCNE1')
    GROUP BY mut."case_barcode", mut."sample_barcode_tumor"
)
SELECT 
    clin.*,
    expr.MDM2_expr,
    expr.TP53_expr,
    expr.CDKN1A_expr,
    expr.CCNE1_expr
FROM "TCGA"."TCGA_VERSIONED"."CLINICAL_GDC_R39" AS clin
JOIN expression_data AS expr
    ON clin."submitter_id" = expr."case_barcode"
WHERE clin."proj__project_id" = 'TCGA-BLCA'