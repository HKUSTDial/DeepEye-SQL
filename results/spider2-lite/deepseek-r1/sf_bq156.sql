WITH MUTATED_PARTICIPANTS AS (
  SELECT DISTINCT "ParticipantBarcode"
  FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."MC3_MAF_V5_ONE_PER_TUMOR_SAMPLE"
  WHERE "Study" = 'LGG'
    AND "Hugo_Symbol" = 'TP53'
    AND "FILTER" = 'PASS'
),
ALL_EXPRESSION AS (
  SELECT "ParticipantBarcode", LOG(10, "normalized_count" + 1) AS log_expr
  FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."EBPP_ADJUSTPANCAN_ILLUMINAHISEQ_RNASEQV2_GENEXP_FILTERED"
  WHERE "Study" = 'LGG'
    AND "Symbol" = 'DRG2'
),
LABELED_DATA AS (
  SELECT 
    a."ParticipantBarcode",
    a.log_expr,
    CASE WHEN m."ParticipantBarcode" IS NOT NULL THEN 1 ELSE 0 END AS has_mutation
  FROM ALL_EXPRESSION a
  LEFT JOIN MUTATED_PARTICIPANTS m ON a."ParticipantBarcode" = m."ParticipantBarcode"
),
MUTATED_STATS AS (
  SELECT
    COUNT(*) AS N_y,
    SUM(log_expr) AS S_y,
    SUM(log_expr * log_expr) AS Q_y
  FROM LABELED_DATA
  WHERE has_mutation = 1
),
NON_MUTATED_STATS AS (
  SELECT
    COUNT(*) AS N_n,
    SUM(log_expr) AS S_n,
    SUM(log_expr * log_expr) AS Q_n
  FROM LABELED_DATA
  WHERE has_mutation = 0
),
ALL_STATS AS (
  SELECT
    N_y,
    S_y,
    Q_y,
    N_n,
    S_n,
    Q_n,
    CASE WHEN N_y > 0 THEN S_y / N_y END AS g_y,
    CASE WHEN N_y > 1 THEN (Q_y - (S_y * S_y) / N_y) / (N_y - 1) END AS s2_y,
    CASE WHEN N_n > 0 THEN S_n / N_n END AS g_n,
    CASE WHEN N_n > 1 THEN (Q_n - (S_n * S_n) / N_n) / (N_n - 1) END AS s2_n
  FROM MUTATED_STATS, NON_MUTATED_STATS
)
SELECT
  ROUND(
    (g_y - g_n) / SQRT((s2_y / N_y) + (s2_n / N_n)),
    2
  ) AS t_score
FROM ALL_STATS
WHERE N_y >= 10
  AND N_n >= 10
  AND s2_y > 0
  AND s2_n > 0