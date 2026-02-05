WITH case_query AS (
  SELECT DISTINCT
    cyto.CaseNo,
    cyto.RefNo
  FROM
    `mitelman-db.prod.Cytogen` cyto
  WHERE
    cyto.Morph = '3111'
    AND cyto.Topo = '0401'
),
chromosome_counts AS (
  SELECT
    cc.Chr AS chromosome,
    COUNT(DISTINCT cc.CaseNo) AS total_cases,
    SUM(CASE WHEN cc.Type = 'Gain' THEN 1 ELSE 0 END) AS gain_count,
    SUM(CASE WHEN cc.Type = 'Loss' THEN 1 ELSE 0 END) AS loss_count
  FROM
    `mitelman-db.prod.CytoConverted` cc
  JOIN
    case_query cq ON cc.CaseNo = cq.CaseNo AND cc.RefNo = cq.RefNo
  WHERE
    cc.Type IN ('Gain', 'Loss')
  GROUP BY
    cc.Chr
),
mitelman_frequencies AS (
  SELECT
    chromosome,
    gain_count * 100.0 / total_cases AS freq_gain,
    loss_count * 100.0 / total_cases AS freq_loss
  FROM
    chromosome_counts
  WHERE
    total_cases > 0
)
SELECT
  chromosome,
  'gain' AS aberration_type,
  freq_gain AS frequency
FROM
  mitelman_frequencies
WHERE
  freq_gain IS NOT NULL

UNION ALL

SELECT
  chromosome,
  'loss' AS aberration_type,
  freq_loss AS frequency
FROM
  mitelman_frequencies
WHERE
  freq_loss IS NOT NULL

ORDER BY
  chromosome,
  aberration_type