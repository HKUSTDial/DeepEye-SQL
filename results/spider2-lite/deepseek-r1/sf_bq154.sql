WITH expr_data AS (
  SELECT 
    "ParticipantBarcode",
    AVG(LOG(10, "normalized_count" + 1)) AS log_expr
  FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."EBPP_ADJUSTPANCAN_ILLUMINAHISEQ_RNASEQV2_GENEXP_FILTERED"
  WHERE "Study" = 'LGG' 
    AND "Symbol" = 'IGF2' 
    AND "normalized_count" IS NOT NULL
  GROUP BY "ParticipantBarcode"
),
clin_data AS (
  SELECT 
    "bcr_patient_barcode",
    "icd_o_3_histology"
  FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."CLINICAL_PANCAN_PATIENT_WITH_FOLLOWUP_FILTERED"
  WHERE "acronym" = 'LGG' 
    AND "icd_o_3_histology" IS NOT NULL
    AND NOT REGEXP_LIKE("icd_o_3_histology", '^\[.*\]$')
),
joined_data AS (
  SELECT 
    expr."ParticipantBarcode",
    expr.log_expr,
    clin."icd_o_3_histology"
  FROM expr_data expr
  JOIN clin_data clin 
    ON expr."ParticipantBarcode" = clin."bcr_patient_barcode"
),
numbered AS (
  SELECT 
    *,
    ROW_NUMBER() OVER (ORDER BY log_expr) AS row_num
  FROM joined_data
),
tie_groups AS (
  SELECT 
    log_expr,
    MIN(row_num) AS min_row,
    MAX(row_num) AS max_row,
    COUNT(*) AS cnt
  FROM numbered
  GROUP BY log_expr
),
ranked_data AS (
  SELECT 
    numbered."icd_o_3_histology",
    numbered."ParticipantBarcode",
    numbered.log_expr,
    (tie_groups.min_row + tie_groups.max_row) / 2.0 AS avg_rank
  FROM numbered
  JOIN tie_groups ON numbered.log_expr = tie_groups.log_expr
),
group_stats AS (
  SELECT 
    "icd_o_3_histology",
    COUNT(*) AS n_i,
    SUM(avg_rank) AS S_i,
    SUM(avg_rank * avg_rank) AS Q_i
  FROM ranked_data
  GROUP BY "icd_o_3_histology"
  HAVING COUNT(*) > 1
),
total_stats AS (
  SELECT 
    SUM(n_i) AS N,
    SUM(S_i) AS total_S,
    SUM(Q_i) AS total_Q
  FROM group_stats
)
SELECT 
  COUNT(DISTINCT gs."icd_o_3_histology") AS total_groups,
  ts.N AS total_samples,
  (ts.N - 1) * 
    (SUM(POWER(gs.S_i, 2) / gs.n_i) - POWER(ts.total_S, 2) / ts.N) / 
    (ts.total_Q - POWER(ts.total_S, 2) / ts.N) AS H_score
FROM group_stats gs
CROSS JOIN total_stats ts
GROUP BY ts.N, ts.total_S, ts.total_Q
ORDER BY H_score DESC