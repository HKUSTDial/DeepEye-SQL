WITH cdkn2a_cases AS (
  SELECT DISTINCT "case_barcode"
  FROM "TCGA"."TCGA_VERSIONED"."SOMATIC_MUTATION_HG19_DCC_2017_02"
  WHERE "project_short_name" = 'TCGA-BLCA'
    AND "Hugo_Symbol" = 'CDKN2A'
  UNION
  SELECT DISTINCT "case_barcode"
  FROM "TCGA"."TCGA_VERSIONED"."SOMATIC_MUTATION_HG19_MC3_2017_02"
  WHERE "project_short_name" = 'TCGA-BLCA'
    AND "Hugo_Symbol" = 'CDKN2A'
),
rna_data AS (
  SELECT "case_barcode", "HGNC_gene_symbol", AVG("normalized_count") AS "normalized_count"
  FROM "TCGA"."TCGA_VERSIONED"."RNASEQ_HG19_GDC_2017_02"
  WHERE "project_short_name" = 'TCGA-BLCA'
    AND "HGNC_gene_symbol" IN ('MDM2', 'TP53', 'CDKN1A', 'CCNE1')
    AND "case_barcode" IN (SELECT "case_barcode" FROM cdkn2a_cases)
  GROUP BY "case_barcode", "HGNC_gene_symbol"
)
SELECT 
  cl."case_id",
  cl."submitter_id",
  cl."primary_site",
  cl."disease_type",
  cl."demo__gender",
  cl."demo__race",
  cl."diag__age_at_diagnosis",
  cl."diag__ajcc_pathologic_stage",
  cl."diag__primary_diagnosis",
  cl."diag__tissue_or_organ_of_origin",
  cl."demo__vital_status",
  cl."demo__days_to_death",
  cl."diag__days_to_last_follow_up",
  r."HGNC_gene_symbol",
  r."normalized_count"
FROM "TCGA"."TCGA_VERSIONED"."CLINICAL_GDC_R39" cl
JOIN rna_data r ON cl."submitter_id" = r."case_barcode"
WHERE cl."proj__project_id" = 'TCGA-BLCA'
ORDER BY cl."case_id", r."HGNC_gene_symbol"