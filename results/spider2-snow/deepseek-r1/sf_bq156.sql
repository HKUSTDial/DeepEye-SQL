WITH mutated_participants AS (
  SELECT DISTINCT "ParticipantBarcode"
  FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."MC3_MAF_V5_ONE_PER_TUMOR_SAMPLE"
  WHERE "Hugo_Symbol" = 'TP53' AND "Study" = 'LGG' AND "FILTER" = 'PASS'
),
expression_data AS (
  SELECT "ParticipantBarcode", AVG(LOG(10, "normalized_count" + 1)) AS log_expr
  FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."EBPP_ADJUSTPANCAN_ILLUMINAHISEQ_RNASEQV2_GENEXP_FILTERED"
  WHERE "Symbol" = 'DRG2' AND "Study" = 'LGG' AND "SampleType" = 'Primary solid Tumor'
  GROUP BY "ParticipantBarcode"
),
aggregates AS (
  SELECT
    COUNT(CASE WHEN m."ParticipantBarcode" IS NOT NULL THEN e.log_expr END) AS N_y,
    SUM(CASE WHEN m."ParticipantBarcode" IS NOT NULL THEN e.log_expr END) AS S_y,
    SUM(CASE WHEN m."ParticipantBarcode" IS NOT NULL THEN e.log_expr * e.log_expr END) AS Q_y,
    COUNT(CASE WHEN m."ParticipantBarcode" IS NULL THEN e.log_expr END) AS N_n,
    SUM(CASE WHEN m."ParticipantBarcode" IS NULL THEN e.log_expr END) AS S_n,
    SUM(CASE WHEN m."ParticipantBarcode" IS NULL THEN e.log_expr * e.log_expr END) AS Q_n
  FROM expression_data e
  LEFT JOIN mutated_participants m ON e."ParticipantBarcode" = m."ParticipantBarcode"
)
SELECT 
  ROUND(
    ((S_y / N_y) - (S_n / N_n)) / 
    SQRT(
      ((Q_y - S_y * S_y / N_y) / (N_y - 1)) / N_y + 
      ((Q_n - S_n * S_n / N_n) / (N_n - 1)) / N_n
    ),
  2) AS t_score
FROM aggregates
WHERE N_y >= 10 AND N_n >= 10
  AND (Q_y - S_y * S_y / N_y) > 0
  AND (Q_n - S_n * S_n / N_n) > 0