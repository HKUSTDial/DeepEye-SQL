WITH paad_patients AS (
  SELECT DISTINCT bcr_patient_barcode
  FROM `isb-cgc-bq.pancancer_atlas.Filtered_clinical_PANCAN_patient_with_followup`
  WHERE acronym = 'PAAD'
),
kras_mutations AS (
  SELECT DISTINCT ParticipantBarcode
  FROM `isb-cgc-bq.pancancer_atlas.Filtered_MC3_MAF_V5_one_per_tumor_sample`
  WHERE Hugo_Symbol = 'KRAS' AND FILTER = 'PASS' AND Study = 'PAAD'
),
tp53_mutations AS (
  SELECT DISTINCT ParticipantBarcode
  FROM `isb-cgc-bq.pancancer_atlas.Filtered_MC3_MAF_V5_one_per_tumor_sample`
  WHERE Hugo_Symbol = 'TP53' AND FILTER = 'PASS' AND Study = 'PAAD'
),
both_mutations AS (
  SELECT k.ParticipantBarcode
  FROM kras_mutations k
  JOIN tp53_mutations t ON k.ParticipantBarcode = t.ParticipantBarcode
),
neither_mutation AS (
  SELECT p.bcr_patient_barcode
  FROM paad_patients p
  LEFT JOIN kras_mutations k ON p.bcr_patient_barcode = k.ParticipantBarcode
  LEFT JOIN tp53_mutations t ON p.bcr_patient_barcode = t.ParticipantBarcode
  WHERE k.ParticipantBarcode IS NULL AND t.ParticipantBarcode IS NULL
)
SELECT 
  (SELECT COUNT(*) FROM both_mutations) - 
  (SELECT COUNT(*) FROM neither_mutation) AS net_difference