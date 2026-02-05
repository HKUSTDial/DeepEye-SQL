SELECT "cytoband_name" FROM (
  SELECT 
    "cytoband_name",
    RANK() OVER (ORDER BY COUNT(DISTINCT CASE WHEN "copy_number" > 3 THEN "aliquot_barcode" || '|' || "start_pos" || '|' || "end_pos" END) DESC) AS "rank_amp",
    RANK() OVER (ORDER BY COUNT(DISTINCT CASE WHEN "copy_number" = 3 THEN "aliquot_barcode" || '|' || "start_pos" || '|' || "end_pos" END) DESC) AS "rank_gain",
    RANK() OVER (ORDER BY COUNT(DISTINCT CASE WHEN "copy_number" = 1 THEN "aliquot_barcode" || '|' || "start_pos" || '|' || "end_pos" END) DESC) AS "rank_het_del"
  FROM "TCGA_MITELMAN"."TCGA_VERSIONED"."COPY_NUMBER_SEGMENT_ALLELIC_HG38_GDC_R23" cn
  JOIN "TCGA_MITELMAN"."PROD"."CYTOBANDS_HG38" c
    ON cn."chromosome" = c."chromosome"
    AND cn."chromosome" = 'chr1'
    AND cn."project_short_name" = 'TCGA-KIRC'
    AND cn."start_pos" < c."hg38_stop"
    AND cn."end_pos" > c."hg38_start"
  GROUP BY "cytoband_name"
) ranked
WHERE "rank_amp" <= 11 AND "rank_gain" <= 11 AND "rank_het_del" <= 11