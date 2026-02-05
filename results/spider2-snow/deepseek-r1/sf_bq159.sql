WITH BRCA_PATIENTS AS (
  SELECT 
    "bcr_patient_barcode" AS barcode,
    "histological_type"
  FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."CLINICAL_PANCAN_PATIENT_WITH_FOLLOWUP_FILTERED"
  WHERE "acronym" = 'BRCA'
    AND "histological_type" IS NOT NULL
),
CDH1_MUTATIONS AS (
  SELECT 
    "ParticipantBarcode" AS barcode,
    1 AS mutation_status
  FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."MC3_MAF_V5_ONE_PER_TUMOR_SAMPLE"
  WHERE "Study" = 'BRCA'
    AND "Hugo_Symbol" = 'CDH1'
    AND "FILTER" = 'PASS'
  GROUP BY "ParticipantBarcode"
),
PATIENT_STATUS AS (
  SELECT 
    p.barcode,
    p."histological_type",
    COALESCE(m.mutation_status, 0) AS mutation_status
  FROM BRCA_PATIENTS p
  LEFT JOIN CDH1_MUTATIONS m ON p.barcode = m.barcode
),
CONTINGENCY AS (
  SELECT 
    "histological_type",
    mutation_status,
    COUNT(*) AS observed_count
  FROM PATIENT_STATUS
  GROUP BY "histological_type", mutation_status
),
ROW_TOTALS AS (
  SELECT 
    "histological_type",
    SUM(observed_count) AS row_total
  FROM CONTINGENCY
  GROUP BY "histological_type"
),
COL_TOTALS AS (
  SELECT 
    mutation_status,
    SUM(observed_count) AS col_total
  FROM CONTINGENCY
  GROUP BY mutation_status
),
GRAND_TOTAL AS (
  SELECT SUM(observed_count) AS total
  FROM CONTINGENCY
),
FILTERED_CONTINGENCY AS (
  SELECT 
    c."histological_type",
    c.mutation_status,
    c.observed_count,
    r.row_total,
    col.col_total,
    g.total
  FROM CONTINGENCY c
  INNER JOIN ROW_TOTALS r ON c."histological_type" = r."histological_type"
  INNER JOIN COL_TOTALS col ON c.mutation_status = col.mutation_status
  CROSS JOIN GRAND_TOTAL g
  WHERE r.row_total > 10
    AND col.col_total > 10
),
FILTERED_ROW_TOTALS AS (
  SELECT 
    "histological_type",
    SUM(observed_count) AS row_total_filtered
  FROM FILTERED_CONTINGENCY
  GROUP BY "histological_type"
),
FILTERED_COL_TOTALS AS (
  SELECT 
    mutation_status,
    SUM(observed_count) AS col_total_filtered
  FROM FILTERED_CONTINGENCY
  GROUP BY mutation_status
),
FILTERED_GRAND_TOTAL AS (
  SELECT SUM(observed_count) AS total_filtered
  FROM FILTERED_CONTINGENCY
),
FILTERED_WITH_TOTALS AS (
  SELECT 
    fc."histological_type",
    fc.mutation_status,
    fc.observed_count,
    fr.row_total_filtered,
    fc2.col_total_filtered,
    fg.total_filtered
  FROM FILTERED_CONTINGENCY fc
  INNER JOIN FILTERED_ROW_TOTALS fr ON fc."histological_type" = fr."histological_type"
  INNER JOIN FILTERED_COL_TOTALS fc2 ON fc.mutation_status = fc2.mutation_status
  CROSS JOIN FILTERED_GRAND_TOTAL fg
),
EXPECTED_COUNTS AS (
  SELECT 
    "histological_type",
    mutation_status,
    observed_count,
    (row_total_filtered * col_total_filtered * 1.0) / total_filtered AS expected_count
  FROM FILTERED_WITH_TOTALS
)
SELECT 
  SUM(POWER(observed_count - expected_count, 2) / expected_count) AS chi_square_value
FROM EXPECTED_COUNTS