WITH eligible_cases AS (
  SELECT DISTINCT "case_barcode"
  FROM "TCGA"."TCGA_VERSIONED"."RNASEQ_HG38_GDC_R35"
  WHERE "project_short_name" = 'TCGA-BRCA'
    AND "sample_type_name" = 'Solid Tissue Normal'
  UNION
  SELECT DISTINCT "case_barcode"
  FROM "TCGA"."TCGA_VERSIONED"."RNASEQ_HG38_GDC_R39"
  WHERE "project_short_name" = 'TCGA-BRCA'
    AND "sample_type_name" = 'Solid Tissue Normal'
  UNION
  SELECT DISTINCT "case_barcode"
  FROM "TCGA"."TCGA_VERSIONED"."RNASEQ_HG38_GDC_R40"
  WHERE "project_short_name" = 'TCGA-BRCA'
    AND "sample_type_name" = 'Solid Tissue Normal'
),
all_samples AS (
  SELECT "Ensembl_gene_id", "gene_name", "fpkm_uq_unstranded"
  FROM "TCGA"."TCGA_VERSIONED"."RNASEQ_HG38_GDC_R35"
  WHERE "case_barcode" IN (SELECT "case_barcode" FROM eligible_cases)
    AND "sample_type_name" = 'Solid Tissue Normal'
    AND "gene_type" = 'protein_coding'
  UNION ALL
  SELECT "Ensembl_gene_id", "gene_name", "fpkm_uq_unstranded"
  FROM "TCGA"."TCGA_VERSIONED"."RNASEQ_HG38_GDC_R39"
  WHERE "case_barcode" IN (SELECT "case_barcode" FROM eligible_cases)
    AND "sample_type_name" = 'Solid Tissue Normal'
    AND "gene_type" = 'protein_coding'
  UNION ALL
  SELECT "Ensembl_gene_id", "gene_name", "fpkm_uq_unstranded"
  FROM "TCGA"."TCGA_VERSIONED"."RNASEQ_HG38_GDC_R40"
  WHERE "case_barcode" IN (SELECT "case_barcode" FROM eligible_cases)
    AND "sample_type_name" = 'Solid Tissue Normal'
    AND "gene_type" = 'protein_coding'
)
SELECT "Ensembl_gene_id", "gene_name", VARIANCE("fpkm_uq_unstranded") AS expression_variance
FROM all_samples
GROUP BY "Ensembl_gene_id", "gene_name"
ORDER BY expression_variance DESC
LIMIT 5