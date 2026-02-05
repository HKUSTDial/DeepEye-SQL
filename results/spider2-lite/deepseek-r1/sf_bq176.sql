WITH cytoband AS (
  SELECT "chromosome", "hg38_start", "hg38_stop"
  FROM "TCGA_MITELMAN"."PROD"."CYTOBANDS_HG38"
  WHERE "cytoband_name" = '15q11' AND "chromosome" = 'chr15'
),
segments AS (
  SELECT "case_barcode", "chromosome", "start_pos", "end_pos", "copy_number"
  FROM "TCGA_MITELMAN"."TCGA_VERSIONED"."COPY_NUMBER_SEGMENT_ALLELIC_HG38_GDC_R23"
  WHERE "project_short_name" = 'TCGA-LAML'
),
overlaps AS (
  SELECT 
    s."case_barcode",
    s."copy_number",
    GREATEST(0, LEAST(s."end_pos", c."hg38_stop") - GREATEST(s."start_pos", c."hg38_start") + 1) AS overlap_length
  FROM segments s
  INNER JOIN cytoband c
    ON s."chromosome" = c."chromosome"
    AND s."start_pos" <= c."hg38_stop"
    AND s."end_pos" >= c."hg38_start"
),
weighted_averages AS (
  SELECT 
    "case_barcode",
    SUM("copy_number" * overlap_length) / SUM(overlap_length) AS weighted_avg_copy_number
  FROM overlaps
  GROUP BY "case_barcode"
),
max_avg AS (
  SELECT MAX(weighted_avg_copy_number) AS max_weighted_avg
  FROM weighted_averages
)
SELECT wa."case_barcode"
FROM weighted_averages wa
CROSS JOIN max_avg ma
WHERE wa.weighted_avg_copy_number = ma.max_weighted_avg