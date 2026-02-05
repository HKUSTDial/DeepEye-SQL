WITH brca_cn AS (
  SELECT 
    "case_barcode",
    'chr' || "chromosome" AS "chromosome",
    "start_pos",
    "end_pos",
    2 * POWER(2, "segment_mean") AS absolute_cn
  FROM "TCGA_HG38_DATA_V0"."TCGA_HG38_DATA_V0"."COPY_NUMBER_SEGMENT_MASKED"
  WHERE "project_short_name" = 'TCGA-BRCA'
),
cytoband AS (
  SELECT DISTINCT
    "chromosome" || ':' || "start_pos" || '-' || "end_pos" AS "cytoband_name",
    "chromosome",
    "start_pos" AS cytoband_start,
    "end_pos" AS cytoband_end
  FROM "TCGA_HG38_DATA_V0"."TCGA_HG38_DATA_V0"."MIRNASEQ_ISOFORM_EXPRESSION"
),
overlaps AS (
  SELECT 
    c."case_barcode",
    cy."cytoband_name",
    cy."chromosome",
    cy.cytoband_start,
    cy.cytoband_end,
    c.absolute_cn,
    GREATEST(0, LEAST(c."end_pos", cy.cytoband_end) - GREATEST(c."start_pos", cy.cytoband_start) + 1) AS overlap_len
  FROM brca_cn c
  INNER JOIN cytoband cy 
    ON c."chromosome" = cy."chromosome"
   AND c."start_pos" <= cy.cytoband_end 
   AND c."end_pos" >= cy.cytoband_start
),
weighted_avg AS (
  SELECT 
    "case_barcode",
    "cytoband_name",
    "chromosome",
    cytoband_start,
    cytoband_end,
    SUM(absolute_cn * overlap_len) / NULLIF(SUM(overlap_len), 0) AS weighted_cn
  FROM overlaps
  GROUP BY "case_barcode", "cytoband_name", "chromosome", cytoband_start, cytoband_end
),
rounded_classified AS (
  SELECT 
    "case_barcode",
    "cytoband_name",
    "chromosome",
    cytoband_start,
    cytoband_end,
    ROUND(weighted_cn) AS rounded_cn,
    CASE 
      WHEN ROUND(weighted_cn) = 0 THEN 'homozygous deletions'
      WHEN ROUND(weighted_cn) = 1 THEN 'heterozygous deletions'
      WHEN ROUND(weighted_cn) = 2 THEN 'normal diploid state'
      WHEN ROUND(weighted_cn) = 3 THEN 'gains'
      ELSE 'amplifications'
    END AS cnv_type
  FROM weighted_avg
),
cytoband_counts AS (
  SELECT 
    "cytoband_name",
    "chromosome",
    cytoband_start,
    cytoband_end,
    cnv_type,
    COUNT(DISTINCT "case_barcode") AS case_count
  FROM rounded_classified
  GROUP BY "cytoband_name", "chromosome", cytoband_start, cytoband_end, cnv_type
),
total_cases_per_cytoband AS (
  SELECT 
    "cytoband_name",
    "chromosome",
    cytoband_start,
    cytoband_end,
    COUNT(DISTINCT "case_barcode") AS total_cases
  FROM rounded_classified
  GROUP BY "cytoband_name", "chromosome", cytoband_start, cytoband_end
)
SELECT 
  cc."cytoband_name",
  cc."chromosome",
  cc.cytoband_start AS "start_pos",
  cc.cytoband_end AS "end_pos",
  cc.cnv_type,
  ROUND(cc.case_count * 100.0 / tc.total_cases, 2) AS frequency_percentage
FROM cytoband_counts cc
INNER JOIN total_cases_per_cytoband tc 
  ON cc."cytoband_name" = tc."cytoband_name"
 AND cc."chromosome" = tc."chromosome"
 AND cc.cytoband_start = tc.cytoband_start
 AND cc.cytoband_end = tc.cytoband_end
ORDER BY cc."chromosome", cc.cytoband_start, cc.cnv_type