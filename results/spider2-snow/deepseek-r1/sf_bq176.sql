WITH cytoband_15q11 AS (
  SELECT "chromosome", "hg38_start", "hg38_stop"
  FROM "TCGA_MITELMAN"."PROD"."CYTOBANDS_HG38"
  WHERE "cytoband_name" = '15q11' AND "chromosome" = 'chr15'
), overlapping_segments AS (
  SELECT 
    s."case_barcode",
    s."segment_mean",
    s."start_pos",
    s."end_pos",
    c."hg38_start",
    c."hg38_stop",
    GREATEST(s."start_pos", c."hg38_start") AS "overlap_start",
    LEAST(s."end_pos", c."hg38_stop") AS "overlap_end"
  FROM "TCGA_MITELMAN"."TCGA_VERSIONED"."COPY_NUMBER_SEGMENT_MASKED_HG38_GDC_R14" s
  INNER JOIN cytoband_15q11 c ON 'chr' || s."chromosome" = c."chromosome"
  WHERE s."project_short_name" = 'TCGA-LAML'
    AND s."chromosome" = '15'
    AND s."start_pos" <= c."hg38_stop"
    AND s."end_pos" >= c."hg38_start"
), weighted_averages AS (
  SELECT 
    "case_barcode",
    SUM(2 * POWER(2, "segment_mean") * ("overlap_end" - "overlap_start" + 1)) / SUM("overlap_end" - "overlap_start" + 1) AS "weighted_avg_cn"
  FROM overlapping_segments
  WHERE "overlap_end" >= "overlap_start"
  GROUP BY "case_barcode"
), max_weighted AS (
  SELECT MAX("weighted_avg_cn") AS "max_avg"
  FROM weighted_averages
)
SELECT DISTINCT w."case_barcode"
FROM weighted_averages w
INNER JOIN max_weighted m ON w."weighted_avg_cn" = m."max_avg"