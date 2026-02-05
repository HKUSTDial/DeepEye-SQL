WITH brca_patients AS (
    SELECT DISTINCT "bcr_patient_barcode", "histological_type"
    FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."CLINICAL_PANCAN_PATIENT_WITH_FOLLOWUP_FILTERED"
    WHERE "acronym" = 'BRCA' AND "histological_type" IS NOT NULL
),
cdh1_mutated AS (
    SELECT DISTINCT "ParticipantBarcode"
    FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."MC3_MAF_V5_ONE_PER_TUMOR_SAMPLE"
    WHERE "Study" = 'BRCA' AND "Hugo_Symbol" = 'CDH1' AND "FILTER" = 'PASS'
)
SELECT
    bp."histological_type",
    (COUNT(DISTINCT cm."ParticipantBarcode") * 100.0 / COUNT(DISTINCT bp."bcr_patient_barcode")) AS mutation_percentage
FROM brca_patients bp
LEFT JOIN cdh1_mutated cm ON bp."bcr_patient_barcode" = cm."ParticipantBarcode"
GROUP BY bp."histological_type"
ORDER BY mutation_percentage DESC
LIMIT 5