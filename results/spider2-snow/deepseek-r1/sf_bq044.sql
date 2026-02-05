WITH mutations AS (
  SELECT 
    "case_id",
    "case_barcode",
    "sample_barcode_tumor",
    "Variant_Classification",
    "VARIANT_CLASS"
  FROM "TCGA"."TCGA_VERSIONED"."MASKED_SOMATIC_MUTATION_HG38_GDC_R35"
  WHERE "project_short_name" = 'TCGA-BLCA'
    AND ("SYMBOL" = 'CDKN2A' OR "Hugo_Symbol" = 'CDKN2A')
  
  UNION ALL
  
  SELECT 
    "case_id",
    "case_barcode",
    "sample_barcode_tumor",
    "Variant_Classification",
    "VARIANT_CLASS"
  FROM "TCGA"."TCGA_VERSIONED"."MASKED_SOMATIC_MUTATION_HG38_GDC_R36"
  WHERE "project_short_name" = 'TCGA-BLCA'
    AND ("SYMBOL" = 'CDKN2A' OR "Hugo_Symbol" = 'CDKN2A')
  
  UNION ALL
  
  SELECT 
    "case_id",
    "case_barcode",
    "sample_barcode_tumor",
    "Variant_Classification",
    "VARIANT_CLASS"
  FROM "TCGA"."TCGA_VERSIONED"."MASKED_SOMATIC_MUTATION_HG38_GDC_R39"
  WHERE "project_short_name" = 'TCGA-BLCA'
    AND ("SYMBOL" = 'CDKN2A' OR "Hugo_Symbol" = 'CDKN2A')
),
clinical AS (
  SELECT 
    "submitter_id",
    "demo__gender",
    "demo__vital_status",
    "demo__days_to_death"
  FROM "TCGA"."TCGA_VERSIONED"."CLINICAL_GDC_R39"
  WHERE "proj__project_id" = 'TCGA-BLCA'
    AND "submitter_id" IN (SELECT DISTINCT "case_barcode" FROM mutations)
),
expression_data AS (
  SELECT 
    e."case_barcode",
    e."sample_barcode",
    e."gene_name",
    e."tpm_unstranded"
  FROM "TCGA"."TCGA_VERSIONED"."RNASEQ_HG38_GDC_R39" e
  WHERE e."project_short_name" = 'TCGA-BLCA'
    AND e."gene_name" IN ('MDM2', 'TP53', 'CDKN1A', 'CCNE1')
    AND e."case_barcode" IN (SELECT DISTINCT "case_barcode" FROM mutations)
)
SELECT 
  m."case_barcode",
  m."sample_barcode_tumor",
  m."Variant_Classification",
  m."VARIANT_CLASS",
  c."demo__gender",
  c."demo__vital_status",
  c."demo__days_to_death",
  MAX(CASE WHEN ed."gene_name" = 'MDM2' THEN ed."tpm_unstranded" END) AS "MDM2_tpm",
  MAX(CASE WHEN ed."gene_name" = 'TP53' THEN ed."tpm_unstranded" END) AS "TP53_tpm",
  MAX(CASE WHEN ed."gene_name" = 'CDKN1A' THEN ed."tpm_unstranded" END) AS "CDKN1A_tpm",
  MAX(CASE WHEN ed."gene_name" = 'CCNE1' THEN ed."tpm_unstranded" END) AS "CCNE1_tpm"
FROM mutations m
LEFT JOIN clinical c ON m."case_barcode" = c."submitter_id"
LEFT JOIN expression_data ed ON m."case_barcode" = ed."case_barcode" 
  AND m."sample_barcode_tumor" = ed."sample_barcode"
GROUP BY 
  m."case_barcode",
  m."sample_barcode_tumor",
  m."Variant_Classification",
  m."VARIANT_CLASS",
  c."demo__gender",
  c."demo__vital_status",
  c."demo__days_to_death"
ORDER BY m."case_barcode", m."sample_barcode_tumor"