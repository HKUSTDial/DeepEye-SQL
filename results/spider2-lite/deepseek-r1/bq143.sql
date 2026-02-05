WITH proteomics_data AS (
  SELECT 
    p.gene_symbol,
    p.protein_abundance_log2ratio,
    m.case_submitter_id,
    r.sample_type_name
  FROM `isb-cgc-bq.CPTAC.quant_proteome_CPTAC_CCRCC_discovery_study_pdc_current` p
  JOIN `isb-cgc-bq.PDC_metadata.aliquot_to_case_mapping_current` m
    ON p.aliquot_id = m.aliquot_id
  JOIN `isb-cgc-bq.CPTAC.RNAseq_hg38_gdc_current` r
    ON m.case_submitter_id = r.case_barcode
    AND p.gene_symbol = r.gene_name
  WHERE r.sample_type_name IN ('Primary Tumor', 'Solid Tissue Normal')
    AND r.primary_site = 'Kidney'
),
rnaseq_transformed AS (
  SELECT 
    gene_name,
    case_barcode,
    sample_type_name,
    LOG(fpkm_unstranded + 1, 2) AS log_fpkm
  FROM `isb-cgc-bq.CPTAC.RNAseq_hg38_gdc_current`
  WHERE sample_type_name IN ('Primary Tumor', 'Solid Tissue Normal')
    AND primary_site = 'Kidney'
),
joined_data AS (
  SELECT 
    p.gene_symbol,
    p.protein_abundance_log2ratio,
    r.log_fpkm,
    p.sample_type_name
  FROM proteomics_data p
  JOIN rnaseq_transformed r
    ON p.case_submitter_id = r.case_barcode
    AND p.gene_symbol = r.gene_name
    AND p.sample_type_name = r.sample_type_name
),
gene_correlations AS (
  SELECT 
    gene_symbol,
    sample_type_name,
    CORR(protein_abundance_log2ratio, log_fpkm) AS correlation
  FROM joined_data
  GROUP BY gene_symbol, sample_type_name
  HAVING correlation IS NOT NULL
),
filtered_correlations AS (
  SELECT 
    gene_symbol,
    sample_type_name,
    correlation
  FROM gene_correlations
  WHERE ABS(correlation) <= 0.5
)
SELECT 
  sample_type_name,
  AVG(correlation) AS avg_correlation
FROM filtered_correlations
GROUP BY sample_type_name