WITH filtered_segments AS (
  SELECT "case_barcode", "sample_barcode", "chromosome", "start_pos", "end_pos", "copy_number"
  FROM "TCGA_MITELMAN"."TCGA_VERSIONED"."COPY_NUMBER_SEGMENT_ALLELIC_HG38_GDC_R23"
  WHERE "project_short_name" = 'TCGA-KIRC'
),
cytobands AS (
  SELECT "chromosome", "hg38_start", "hg38_stop", "cytoband_name"
  FROM "TCGA_MITELMAN"."PROD"."CYTOBANDS_HG38"
),
overlaps AS (
  SELECT s."case_barcode", s."sample_barcode", s."copy_number", c."cytoband_name", c."chromosome"
  FROM filtered_segments s
  JOIN cytobands c 
    ON s."chromosome" = c."chromosome"
   AND s."start_pos" <= c."hg38_stop"
   AND s."end_pos" >= c."hg38_start"
),
sample_cytoband_max AS (
  SELECT "case_barcode", "sample_barcode", "chromosome", "cytoband_name", MAX("copy_number") AS "max_copy"
  FROM overlaps
  GROUP BY "case_barcode", "sample_barcode", "chromosome", "cytoband_name"
),
classified AS (
  SELECT *,
    CASE 
      WHEN "max_copy" > 3 THEN 'amplification'
      WHEN "max_copy" = 3 THEN 'gain'
      WHEN "max_copy" = 0 THEN 'homozygous deletion'
      WHEN "max_copy" = 1 THEN 'heterozygous deletion'
      WHEN "max_copy" = 2 THEN 'normal'
    END AS "subtype"
  FROM sample_cytoband_max
),
counts AS (
  SELECT "chromosome", "cytoband_name", "subtype", COUNT(DISTINCT "case_barcode") AS "case_count"
  FROM classified
  GROUP BY "chromosome", "cytoband_name", "subtype"
),
total_cases AS (
  SELECT COUNT(DISTINCT "case_barcode") AS "total"
  FROM filtered_segments
)
SELECT c."chromosome", c."cytoband_name", c."subtype", 
       (c."case_count" * 100.0 / t."total") AS "frequency_percentage"
FROM counts c
CROSS JOIN total_cases t
ORDER BY c."chromosome", c."cytoband_name", c."subtype"