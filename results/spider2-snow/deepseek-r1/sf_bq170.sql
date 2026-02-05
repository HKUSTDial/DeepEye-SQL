WITH brca_segments AS (
  SELECT "case_barcode", "chromosome", "start_pos", "end_pos", "copy_number"
  FROM "TCGA_MITELMAN"."TCGA_VERSIONED"."COPY_NUMBER_SEGMENT_ALLELIC_HG38_GDC_R23"
  WHERE "project_short_name" = 'TCGA-BRCA'
),
overlaps AS (
  SELECT 
    c."cytoband_name",
    c."hg38_start",
    c."hg38_stop",
    s."case_barcode",
    s."copy_number",
    (ABS(c."hg38_stop" - c."hg38_start") + ABS(s."end_pos" - s."start_pos") - 
     ABS(c."hg38_stop" - s."end_pos") - ABS(c."hg38_start" - s."start_pos")) / 2 AS "overlap"
  FROM "TCGA_MITELMAN"."PROD"."CYTOBANDS_HG38" c
  INNER JOIN brca_segments s 
    ON c."chromosome" = s."chromosome"
    AND c."hg38_start" < s."end_pos" 
    AND s."start_pos" < c."hg38_stop"
),
weighted AS (
  SELECT 
    "cytoband_name",
    "hg38_start",
    "hg38_stop",
    "case_barcode",
    ROUND(SUM("overlap" * "copy_number") / SUM("overlap")) AS "rounded_copy_number"
  FROM overlaps
  GROUP BY "cytoband_name", "hg38_start", "hg38_stop", "case_barcode"
  HAVING SUM("overlap") > 0
),
classified AS (
  SELECT 
    "cytoband_name",
    "hg38_start",
    "hg38_stop",
    "case_barcode",
    "rounded_copy_number",
    CASE 
      WHEN "rounded_copy_number" = 0 THEN 'homozygous_deletions'
      WHEN "rounded_copy_number" = 1 THEN 'heterozygous_deletions'
      WHEN "rounded_copy_number" = 2 THEN 'normal_diploid'
      WHEN "rounded_copy_number" = 3 THEN 'gains'
      ELSE 'amplifications'
    END AS "cnv_type"
  FROM weighted
),
counts AS (
  SELECT 
    "cytoband_name",
    "hg38_start",
    "hg38_stop",
    COUNT(DISTINCT "case_barcode") AS "total_cases",
    COUNT(DISTINCT CASE WHEN "cnv_type" = 'homozygous_deletions' THEN "case_barcode" END) AS "homozygous_deletions",
    COUNT(DISTINCT CASE WHEN "cnv_type" = 'heterozygous_deletions' THEN "case_barcode" END) AS "heterozygous_deletions",
    COUNT(DISTINCT CASE WHEN "cnv_type" = 'normal_diploid' THEN "case_barcode" END) AS "normal_diploid",
    COUNT(DISTINCT CASE WHEN "cnv_type" = 'gains' THEN "case_barcode" END) AS "gains",
    COUNT(DISTINCT CASE WHEN "cnv_type" = 'amplifications' THEN "case_barcode" END) AS "amplifications"
  FROM classified
  GROUP BY "cytoband_name", "hg38_start", "hg38_stop"
)
SELECT 
  "cytoband_name",
  "hg38_start",
  "hg38_stop",
  ROUND(100.0 * "homozygous_deletions" / "total_cases", 2) AS "homozygous_deletions_percent",
  ROUND(100.0 * "heterozygous_deletions" / "total_cases", 2) AS "heterozygous_deletions_percent",
  ROUND(100.0 * "normal_diploid" / "total_cases", 2) AS "normal_diploid_percent",
  ROUND(100.0 * "gains" / "total_cases", 2) AS "gains_percent",
  ROUND(100.0 * "amplifications" / "total_cases", 2) AS "amplifications_percent"
FROM counts
ORDER BY "cytoband_name"