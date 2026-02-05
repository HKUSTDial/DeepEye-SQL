WITH kirp_patients AS (
  SELECT DISTINCT "case_barcode", "clinical_stage"
  FROM "TCGA_HG38_DATA_V0"."TCGA_BIOCLIN_V0"."CLINICAL"
  WHERE "disease_code" = 'KIRP' AND "clinical_stage" IS NOT NULL
),
patient_genes AS (
  SELECT 
    k."case_barcode",
    k."clinical_stage",
    r."gene_name",
    r."HTSeq__FPKM_UQ"
  FROM kirp_patients k
  INNER JOIN "TCGA_HG38_DATA_V0"."TCGA_HG38_DATA_V0"."RNASEQ_GENE_EXPRESSION" r
    ON k."case_barcode" = r."case_barcode"
  WHERE r."gene_name" IN ('MT-CO1','MT-CO2','MT-CO3')
),
split_data AS (
  SELECT *,
    CASE 
      WHEN ABS(HASH("case_barcode")) % 100 < 90 THEN 'training'
      ELSE 'test'
    END AS dataset
  FROM patient_genes
),
stage_avgs AS (
  SELECT 
    "clinical_stage",
    "gene_name",
    AVG("HTSeq__FPKM_UQ") AS avg_expr
  FROM split_data
  WHERE dataset = 'training'
  GROUP BY "clinical_stage", "gene_name"
),
stage_vectors AS (
  SELECT 
    "clinical_stage",
    MAX(CASE WHEN "gene_name" = 'MT-CO1' THEN avg_expr END) AS avg_CO1,
    MAX(CASE WHEN "gene_name" = 'MT-CO2' THEN avg_expr END) AS avg_CO2,
    MAX(CASE WHEN "gene_name" = 'MT-CO3' THEN avg_expr END) AS avg_CO3
  FROM stage_avgs
  GROUP BY "clinical_stage"
),
test_patients AS (
  SELECT 
    "case_barcode",
    MAX(CASE WHEN "gene_name" = 'MT-CO1' THEN "HTSeq__FPKM_UQ" END) AS expr_CO1,
    MAX(CASE WHEN "gene_name" = 'MT-CO2' THEN "HTSeq__FPKM_UQ" END) AS expr_CO2,
    MAX(CASE WHEN "gene_name" = 'MT-CO3' THEN "HTSeq__FPKM_UQ" END) AS expr_CO3
  FROM split_data
  WHERE dataset = 'test'
  GROUP BY "case_barcode"
),
distances AS (
  SELECT 
    t."case_barcode",
    s."clinical_stage",
    SQRT(
      POWER(t.expr_CO1 - s.avg_CO1, 2) +
      POWER(t.expr_CO2 - s.avg_CO2, 2) +
      POWER(t.expr_CO3 - s.avg_CO3, 2)
    ) AS distance
  FROM test_patients t
  CROSS JOIN stage_vectors s
),
ranked_distances AS (
  SELECT 
    "case_barcode",
    "clinical_stage",
    distance,
    ROW_NUMBER() OVER (PARTITION BY "case_barcode" ORDER BY distance) AS rn
  FROM distances
)
SELECT "case_barcode", "clinical_stage" AS predicted_clinical_stage
FROM ranked_distances
WHERE rn = 1