WITH PAAD_patients AS (
  SELECT DISTINCT `bcr_patient_barcode` AS patient_barcode
  FROM `isb-cgc-bq.pancancer_atlas.Filtered_clinical_PANCAN_patient_with_followup`
  WHERE `acronym` = 'PAAD'
),
high_quality_mutations AS (
  SELECT 
    m.`ParticipantBarcode`, 
    m.`Hugo_Symbol`
  FROM `isb-cgc-bq.pancancer_atlas.Filtered_MC3_MAF_V5_one_per_tumor_sample` m
  LEFT JOIN `isb-cgc-bq.pancancer_atlas.merged_sample_quality_annotations` q
    ON m.`Tumor_AliquotBarcode` = q.`aliquot_barcode`
  WHERE m.`Study` = 'PAAD'
    AND m.`Hugo_Symbol` IN ('KRAS', 'TP53')
    AND (m.`FILTER` = 'PASS' OR m.`FILTER` IS NULL)
    AND (q.`Do_not_use` = 'False' OR q.`Do_not_use` IS NULL)
),
patient_mutations AS (
  SELECT 
    p.patient_barcode,
    MAX(CASE WHEN h.`Hugo_Symbol` = 'KRAS' THEN 1 ELSE 0 END) AS has_KRAS,
    MAX(CASE WHEN h.`Hugo_Symbol` = 'TP53' THEN 1 ELSE 0 END) AS has_TP53
  FROM PAAD_patients p
  LEFT JOIN high_quality_mutations h ON p.patient_barcode = h.`ParticipantBarcode`
  GROUP BY p.patient_barcode
),
contingency_counts AS (
  SELECT 
    SUM(CASE WHEN has_KRAS = 1 AND has_TP53 = 1 THEN 1 ELSE 0 END) AS both_mutations,
    SUM(CASE WHEN has_KRAS = 1 AND has_TP53 = 0 THEN 1 ELSE 0 END) AS kras_only,
    SUM(CASE WHEN has_KRAS = 0 AND has_TP53 = 1 THEN 1 ELSE 0 END) AS tp53_only,
    SUM(CASE WHEN has_KRAS = 0 AND has_TP53 = 0 THEN 1 ELSE 0 END) AS neither,
    COUNT(*) AS total_patients
  FROM patient_mutations
),
chi_squared_calc AS (
  SELECT 
    (POWER((both_mutations*neither - kras_only*tp53_only), 2) * total_patients) / 
    NULLIF(((both_mutations+kras_only)*(both_mutations+tp53_only)*(kras_only+neither)*(tp53_only+neither)), 0) AS chi_squared
  FROM contingency_counts
)
SELECT chi_squared FROM chi_squared_calc