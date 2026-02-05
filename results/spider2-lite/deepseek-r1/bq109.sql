WITH filtered_data AS (
  SELECT v.coloc_log2_h4_h3, v.right_study
  FROM `open-targets-genetics.genetics.variant_disease_coloc` v
  INNER JOIN `open-targets-genetics.genetics.studies` s
    ON v.right_study = s.study_id
  WHERE v.right_gene_id = 'ENSG00000169174'
    AND v.coloc_h4 > 0.8
    AND v.coloc_h3 < 0.02
    AND v.right_bio_feature = 'IPSC'
    AND v.left_chrom = '1'
    AND v.left_pos = 55029009
    AND v.left_ref = 'C'
    AND v.left_alt = 'T'
    AND CONTAINS_SUBSTR(s.trait_reported, 'lesterol levels')
)
SELECT
  AVG(coloc_log2_h4_h3) AS avg_log2_h4_h3,
  VAR_SAMP(coloc_log2_h4_h3) AS var_log2_h4_h3,
  MAX(coloc_log2_h4_h3) - MIN(coloc_log2_h4_h3) AS max_min_diff_log2_h4_h3,
  ARRAY_AGG(right_study ORDER BY coloc_log2_h4_h3 DESC LIMIT 1)[OFFSET(0)] AS max_log2_h4_h3_right_study
FROM filtered_data