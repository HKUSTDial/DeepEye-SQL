WITH patients AS (
    SELECT DISTINCT "bcr_patient_barcode" AS patient_id
    FROM "PANCANCER_ATLAS_2"."PANCANCER_ATLAS"."FILTERED_CLINICAL_PANCAN_PATIENT_WITH_FOLLOWUP"
    WHERE "acronym" = 'PAAD'
),
kras_mut AS (
    SELECT DISTINCT "ParticipantBarcode" AS patient_id
    FROM "PANCANCER_ATLAS_2"."PANCANCER_ATLAS"."FILTERED_MC3_MAF_V5_ONE_PER_TUMOR_SAMPLE"
    WHERE "Hugo_Symbol" = 'KRAS' AND "FILTER" = 'PASS' AND "Study" = 'PAAD'
),
tp53_mut AS (
    SELECT DISTINCT "ParticipantBarcode" AS patient_id
    FROM "PANCANCER_ATLAS_2"."PANCANCER_ATLAS"."FILTERED_MC3_MAF_V5_ONE_PER_TUMOR_SAMPLE"
    WHERE "Hugo_Symbol" = 'TP53' AND "FILTER" = 'PASS' AND "Study" = 'PAAD'
),
patient_status AS (
    SELECT
        p.patient_id,
        CASE WHEN k.patient_id IS NOT NULL THEN 1 ELSE 0 END AS has_kras,
        CASE WHEN t.patient_id IS NOT NULL THEN 1 ELSE 0 END AS has_tp53
    FROM patients p
    LEFT JOIN kras_mut k ON p.patient_id = k.patient_id
    LEFT JOIN tp53_mut t ON p.patient_id = t.patient_id
),
contingency AS (
    SELECT
        SUM(CASE WHEN has_kras = 1 AND has_tp53 = 1 THEN 1 ELSE 0 END) AS a,
        SUM(CASE WHEN has_kras = 1 AND has_tp53 = 0 THEN 1 ELSE 0 END) AS b,
        SUM(CASE WHEN has_kras = 0 AND has_tp53 = 1 THEN 1 ELSE 0 END) AS c,
        SUM(CASE WHEN has_kras = 0 AND has_tp53 = 0 THEN 1 ELSE 0 END) AS d,
        COUNT(*) AS n
    FROM patient_status
)
SELECT
    (POWER(a - ((a + b) * (a + c) * 1.0 / n), 2) / NULLIF(((a + b) * (a + c) * 1.0 / n), 0) +
     POWER(b - ((a + b) * (b + d) * 1.0 / n), 2) / NULLIF(((a + b) * (b + d) * 1.0 / n), 0) +
     POWER(c - ((c + d) * (a + c) * 1.0 / n), 2) / NULLIF(((c + d) * (a + c) * 1.0 / n), 0) +
     POWER(d - ((c + d) * (b + d) * 1.0 / n), 2) / NULLIF(((c + d) * (b + d) * 1.0 / n), 0)
    ) AS chi_squared
FROM contingency