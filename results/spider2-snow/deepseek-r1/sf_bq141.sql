WITH kirp_patients AS (
  SELECT DISTINCT "case_barcode", "clinical_stage"
  FROM "TCGA_HG38_DATA_V0"."TCGA_BIOCLIN_V0"."CLINICAL"
  WHERE "disease_code" = 'KIRP' AND "clinical_stage" IS NOT NULL
),
expression_data AS (
  SELECT e."case_barcode", e."gene_name", e."HTSeq__FPKM_UQ"
  FROM "TCGA_HG38_DATA_V0"."TCGA_HG38_DATA_V0"."RNASEQ_GENE_EXPRESSION" e
  INNER JOIN kirp_patients c ON e."case_barcode" = c."case_barcode"
  WHERE e."gene_name" IN ('MT-CO1', 'MT-CO2', 'MT-CO3')
),
pivoted_expression AS (
  SELECT 
    "case_barcode",
    MAX(CASE WHEN "gene_name" = 'MT-CO1' THEN "HTSeq__FPKM_UQ" END) AS expr_co1,
    MAX(CASE WHEN "gene_name" = 'MT-CO2' THEN "HTSeq__FPKM_UQ" END) AS expr_co2,
    MAX(CASE WHEN "gene_name" = 'MT-CO3' THEN "HTSeq__FPKM_UQ" END) AS expr_co3
  FROM expression_data
  GROUP BY "case_barcode"
),
combined_patients AS (
  SELECT 
    p."case_barcode",
    p.expr_co1,
    p.expr_co2,
    p.expr_co3,
    k."clinical_stage",
    ABS(HASH(p."case_barcode")) % 10 AS hash_val
  FROM pivoted_expression p
  INNER JOIN kirp_patients k ON p."case_barcode" = k."case_barcode"
),
training_set AS (
  SELECT "clinical_stage", expr_co1, expr_co2, expr_co3
  FROM combined_patients
  WHERE hash_val < 9
),
test_set AS (
  SELECT "case_barcode", expr_co1, expr_co2, expr_co3
  FROM combined_patients
  WHERE hash_val = 9
),
stage_averages AS (
  SELECT 
    "clinical_stage",
    AVG(expr_co1) AS avg_co1,
    AVG(expr_co2) AS avg_co2,
    AVG(expr_co3) AS avg_co3
  FROM training_set
  GROUP BY "clinical_stage"
),
distances AS (
  SELECT 
    t."case_barcode",
    s."clinical_stage",
    SQRT(POWER(t.expr_co1 - s.avg_co1, 2) + POWER(t.expr_co2 - s.avg_co2, 2) + POWER(t.expr_co3 - s.avg_co3, 2)) AS distance
  FROM test_set t
  CROSS JOIN stage_averages s
),
ranked_distances AS (
  SELECT 
    "case_barcode",
    "clinical_stage" AS predicted_stage,
    ROW_NUMBER() OVER (PARTITION BY "case_barcode" ORDER BY distance ASC) AS rn
  FROM distances
)
SELECT "case_barcode", predicted_stage
FROM ranked_distances
WHERE rn = 1
ORDER BY "case_barcode"