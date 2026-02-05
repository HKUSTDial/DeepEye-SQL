WITH CDKN2A_PATIENTS AS (
  SELECT 
    DISTINCT m."case_id",
    m."case_barcode",
    m."Hugo_Symbol",
    m."Variant_Classification",
    m."VARIANT_CLASS"
  FROM "TCGA"."TCGA_VERSIONED"."MASKED_SOMATIC_MUTATION_HG38_GDC_R39" AS m
  WHERE m."project_short_name" = 'TCGA-BLCA'
    AND (m."Hugo_Symbol" = 'CDKN2A' OR m."SYMBOL" = 'CDKN2A')
  UNION
  SELECT 
    DISTINCT m."case_id",
    m."case_barcode",
    m."Hugo_Symbol",
    m."Variant_Classification",
    m."VARIANT_CLASS"
  FROM "TCGA"."TCGA_VERSIONED"."MASKED_SOMATIC_MUTATION_HG38_GDC_R35" AS m
  WHERE m."project_short_name" = 'TCGA-BLCA'
    AND (m."Hugo_Symbol" = 'CDKN2A' OR m."SYMBOL" = 'CDKN2A')
  UNION
  SELECT 
    DISTINCT m."case_id",
    m."case_barcode",
    m."Hugo_Symbol",
    m."Variant_Classification",
    m."VARIANT_CLASS"
  FROM "TCGA"."TCGA_VERSIONED"."MASKED_SOMATIC_MUTATION_HG38_GDC_R36" AS m
  WHERE m."project_short_name" = 'TCGA-BLCA'
    AND (m."Hugo_Symbol" = 'CDKN2A' OR m."SYMBOL" = 'CDKN2A')
),
CLINICAL_INFO AS (
  SELECT
    c."case_id",
    c."demo__gender",
    c."demo__vital_status",
    c."demo__days_to_death"
  FROM "TCGA"."TCGA_VERSIONED"."CLINICAL_GDC_R39" AS c
),
EXPRESSION_DATA AS (
  SELECT
    r."case_gdc_id",
    r."gene_name",
    r."tpm_unstranded"
  FROM "TCGA"."TCGA_VERSIONED"."RNASEQ_HG38_GDC_R39" AS r
  WHERE r."gene_name" IN ('MDM2', 'TP53', 'CDKN1A', 'CCNE1')
    AND r."project_short_name" = 'TCGA-BLCA'
)
SELECT
  cp."case_barcode",
  cp."Hugo_Symbol",
  cp."Variant_Classification",
  cp."VARIANT_CLASS",
  ci."demo__gender",
  ci."demo__vital_status",
  ci."demo__days_to_death",
  MAX(CASE WHEN ed."gene_name" = 'MDM2' THEN ed."tpm_unstranded" END) AS "MDM2_expression",
  MAX(CASE WHEN ed."gene_name" = 'TP53' THEN ed."tpm_unstranded" END) AS "TP53_expression",
  MAX(CASE WHEN ed."gene_name" = 'CDKN1A' THEN ed."tpm_unstranded" END) AS "CDKN1A_expression",
  MAX(CASE WHEN ed."gene_name" = 'CCNE1' THEN ed."tpm_unstranded" END) AS "CCNE1_expression"
FROM CDKN2A_PATIENTS AS cp
INNER JOIN CLINICAL_INFO AS ci ON cp."case_id" = ci."case_id"
LEFT JOIN EXPRESSION_DATA AS ed ON cp."case_id" = ed."case_gdc_id"
GROUP BY
  cp."case_barcode",
  cp."Hugo_Symbol",
  cp."Variant_Classification",
  cp."VARIANT_CLASS",
  ci."demo__gender",
  ci."demo__vital_status",
  ci."demo__days_to_death"