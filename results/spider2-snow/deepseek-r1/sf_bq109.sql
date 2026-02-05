WITH filtered AS (
  SELECT
    vdc."coloc_log2_h4_h3",
    vdc."right_study"
  FROM
    "OPEN_TARGETS_GENETICS_1"."GENETICS"."VARIANT_DISEASE_COLOC" vdc
    JOIN "OPEN_TARGETS_GENETICS_1"."GENETICS"."STUDIES" s ON vdc."left_study" = s."study_id"
  WHERE
    vdc."right_gene_id" = 'ENSG00000169174'
    AND vdc."coloc_h4" > 0.8
    AND vdc."coloc_h3" < 0.02
    AND vdc."right_bio_feature" = 'IPSC'
    AND vdc."left_chrom" = '1'
    AND vdc."left_pos" = 55029009
    AND vdc."left_ref" = 'C'
    AND vdc."left_alt" = 'T'
    AND s."trait_reported" ILIKE '%lesterol levels%'
),
aggregates AS (
  SELECT
    AVG("coloc_log2_h4_h3") AS average_log2_h4_h3,
    VARIANCE("coloc_log2_h4_h3") AS variance_log2_h4_h3,
    MAX("coloc_log2_h4_h3") - MIN("coloc_log2_h4_h3") AS max_min_diff_log2_h4_h3,
    MAX("coloc_log2_h4_h3") AS max_log2_h4_h3
  FROM filtered
),
max_study AS (
  SELECT "right_study"
  FROM filtered
  ORDER BY "coloc_log2_h4_h3" DESC
  LIMIT 1
)
SELECT
  aggregates.average_log2_h4_h3,
  aggregates.variance_log2_h4_h3,
  aggregates.max_min_diff_log2_h4_h3,
  max_study."right_study" AS qtl_source_max_log2_h4_h3
FROM aggregates, max_study