WITH paad_patients AS (
    SELECT DISTINCT "bcr_patient_barcode" AS patient_barcode
    FROM "PANCANCER_ATLAS_2"."PANCANCER_ATLAS"."FILTERED_CLINICAL_PANCAN_PATIENT_WITH_FOLLOWUP"
    WHERE "acronym" = 'PAAD'
),
mutations AS (
    SELECT DISTINCT "ParticipantBarcode" AS patient_barcode, "Hugo_Symbol" AS gene
    FROM "PANCANCER_ATLAS_2"."PANCANCER_ATLAS"."FILTERED_MC3_MAF_V5_ONE_PER_TUMOR_SAMPLE"
    WHERE "Study" = 'PAAD' AND "FILTER" = 'PASS'
      AND "Hugo_Symbol" IN ('KRAS', 'TP53')
),
patient_flags AS (
    SELECT p.patient_barcode,
           MAX(CASE WHEN m.gene = 'KRAS' THEN 1 ELSE 0 END) AS has_KRAS,
           MAX(CASE WHEN m.gene = 'TP53' THEN 1 ELSE 0 END) AS has_TP53
    FROM paad_patients p
    LEFT JOIN mutations m ON p.patient_barcode = m.patient_barcode
    GROUP BY p.patient_barcode
)
SELECT 
    (SUM(CASE WHEN has_KRAS = 1 AND has_TP53 = 1 THEN 1 ELSE 0 END) -
     SUM(CASE WHEN has_KRAS = 0 AND has_TP53 = 0 THEN 1 ELSE 0 END)) AS net_difference
FROM patient_flags