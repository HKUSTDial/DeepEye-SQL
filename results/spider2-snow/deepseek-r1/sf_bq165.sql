WITH cohort AS (
  SELECT DISTINCT "RefNo", "CaseNo"
  FROM "MITELMAN"."PROD"."CYTOGEN"
  WHERE "Morph" = '3111' AND "Topo" = '0401'
),
total_cohort AS (
  SELECT COUNT(*) AS total FROM cohort
),
events AS (
  SELECT cct."RefNo", cct."CaseNo", cct."Chr", cct."Start", cct."End", cct."Type"
  FROM "MITELMAN"."PROD"."CYTOCONVERTED" cct
  INNER JOIN cohort 
    ON cct."RefNo" = cohort."RefNo" 
    AND cct."CaseNo" = cohort."CaseNo"
  WHERE cct."Type" IN ('Amplification', 'Gain', 'Loss', 'Homozygous deletion')
),
band_events AS (
  SELECT 
    ev."Type",
    cb."chromosome",
    cb."cytoband_name",
    cb."hg38_start",
    cb."hg38_stop",
    ev."RefNo",
    ev."CaseNo"
  FROM events ev
  JOIN "MITELMAN"."PROD"."CYTOBANDS_HG38" cb
    ON ev."Chr" = cb."chromosome"
    AND ev."Start" <= cb."hg38_stop"
    AND ev."End" >= cb."hg38_start"
),
aggregated AS (
  SELECT 
    "chromosome",
    "cytoband_name",
    "hg38_start",
    "hg38_stop",
    "Type",
    COUNT(DISTINCT "RefNo" || ':' || "CaseNo") AS event_count
  FROM band_events
  GROUP BY "chromosome", "cytoband_name", "hg38_start", "hg38_stop", "Type"
),
with_frequency AS (
  SELECT 
    a.*,
    ROUND(a.event_count * 100.0 / t.total, 2) AS frequency_percent
  FROM aggregated a
  CROSS JOIN total_cohort t
)
SELECT 
  "chromosome",
  "cytoband_name",
  "hg38_start",
  "hg38_stop",
  COALESCE(MAX(CASE WHEN "Type" = 'Amplification' THEN event_count END), 0) AS amplification_count,
  COALESCE(MAX(CASE WHEN "Type" = 'Amplification' THEN frequency_percent END), 0) AS amplification_frequency,
  COALESCE(MAX(CASE WHEN "Type" = 'Gain' THEN event_count END), 0) AS gain_count,
  COALESCE(MAX(CASE WHEN "Type" = 'Gain' THEN frequency_percent END), 0) AS gain_frequency,
  COALESCE(MAX(CASE WHEN "Type" = 'Loss' THEN event_count END), 0) AS loss_count,
  COALESCE(MAX(CASE WHEN "Type" = 'Loss' THEN frequency_percent END), 0) AS loss_frequency,
  COALESCE(MAX(CASE WHEN "Type" = 'Homozygous deletion' THEN event_count END), 0) AS homozygous_deletion_count,
  COALESCE(MAX(CASE WHEN "Type" = 'Homozygous deletion' THEN frequency_percent END), 0) AS homozygous_deletion_frequency
FROM with_frequency
GROUP BY "chromosome", "cytoband_name", "hg38_start", "hg38_stop"
ORDER BY 
  CASE 
    WHEN "chromosome" = 'chrX' THEN 23
    WHEN "chromosome" = 'chrY' THEN 24
    ELSE CAST(SUBSTR("chromosome", 4) AS INT)
  END,
  "hg38_start",
  "hg38_stop"