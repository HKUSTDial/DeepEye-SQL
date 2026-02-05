WITH brca_patients AS (
  SELECT "bcr_patient_barcode", "histological_type"
  FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."CLINICAL_PANCAN_PATIENT_WITH_FOLLOWUP_FILTERED"
  WHERE "acronym" = 'BRCA'
),
cdh1_mutations AS (
  SELECT DISTINCT "ParticipantBarcode"
  FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."MC3_MAF_V5_ONE_PER_TUMOR_SAMPLE"
  WHERE "Study" = 'BRCA' AND "Hugo_Symbol" = 'CDH1'
)
SELECT 
  bp."histological_type",
  COUNT(DISTINCT bp."bcr_patient_barcode") AS total_patients,
  COUNT(DISTINCT cm."ParticipantBarcode") AS patients_with_cdh1_mutation,
  (COUNT(DISTINCT cm."ParticipantBarcode") * 100.0 / COUNT(DISTINCT bp."bcr_patient_barcode")) AS mutation_percentage
FROM brca_patients bp
LEFT JOIN cdh1_mutations cm ON bp."bcr_patient_barcode" = cm."ParticipantBarcode"
GROUP BY bp."histological_type"
HAVING total_patients > 0
ORDER BY mutation_percentage DESC
LIMIT 5