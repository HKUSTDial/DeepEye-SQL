WITH cohort AS (
  SELECT DISTINCT c.`CaseNo`, c.`RefNo`
  FROM `mitelman-db.prod.Cytogen` c
  WHERE c.`Morph` = '3111' AND c.`Topo` = '0401'
),
events_with_bands AS (
  SELECT 
    cc.`CaseNo`,
    cc.`RefNo`,
    cc.`Type`,
    cb.`cytoband_name`,
    cb.`chromosome`,
    cb.`hg38_start`,
    cb.`hg38_stop`,
    CASE 
      WHEN cb.`chromosome` = 'chrX' THEN 23
      WHEN cb.`chromosome` = 'chrY' THEN 24
      ELSE CAST(REGEXP_EXTRACT(cb.`chromosome`, r'chr(\d+)') AS INT64)
    END as chr_ordinal
  FROM cohort co
  INNER JOIN `mitelman-db.prod.CytoConverted` cc
    ON co.`CaseNo` = cc.`CaseNo` AND co.`RefNo` = cc.`RefNo`
  INNER JOIN `mitelman-db.prod.CytoBands_hg38` cb
    ON cc.`Chr` = cb.`chromosome`
    AND cc.`Start` < cb.`hg38_stop`
    AND cc.`End` > cb.`hg38_start`
),
total_samples AS (
  SELECT COUNT(DISTINCT CONCAT(`CaseNo`, '|', `RefNo`)) as total_count
  FROM cohort
)
SELECT 
  eb.`cytoband_name`,
  eb.`chromosome`,
  eb.`hg38_start`,
  eb.`hg38_stop`,
  COUNT(DISTINCT CASE WHEN eb.`Type` = 'Gain' THEN CONCAT(eb.`CaseNo`, '|', eb.`RefNo`, '|', eb.`hg38_start`, '|', eb.`hg38_stop`) END) as gain_count,
  ROUND(COUNT(DISTINCT CASE WHEN eb.`Type` = 'Gain' THEN CONCAT(eb.`CaseNo`, '|', eb.`RefNo`, '|', eb.`hg38_start`, '|', eb.`hg38_stop`) END) * 100.0 / ts.`total_count`, 2) as gain_percentage,
  COUNT(DISTINCT CASE WHEN eb.`Type` = 'Loss' THEN CONCAT(eb.`CaseNo`, '|', eb.`RefNo`, '|', eb.`hg38_start`, '|', eb.`hg38_stop`) END) as loss_count,
  ROUND(COUNT(DISTINCT CASE WHEN eb.`Type` = 'Loss' THEN CONCAT(eb.`CaseNo`, '|', eb.`RefNo`, '|', eb.`hg38_start`, '|', eb.`hg38_stop`) END) * 100.0 / ts.`total_count`, 2) as loss_percentage,
  0 as amplification_count,
  0.00 as amplification_percentage,
  0 as homozygous_deletion_count,
  0.00 as homozygous_deletion_percentage
FROM events_with_bands eb
CROSS JOIN total_samples ts
GROUP BY eb.`cytoband_name`, eb.`chromosome`, eb.`hg38_start`, eb.`hg38_stop`, eb.`chr_ordinal`, ts.`total_count`
ORDER BY eb.`chr_ordinal`, eb.`hg38_start`, eb.`hg38_stop`