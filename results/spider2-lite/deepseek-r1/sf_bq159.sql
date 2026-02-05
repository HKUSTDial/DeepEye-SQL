WITH brca_patients AS (
    SELECT 
        "bcr_patient_barcode" AS "patient_id",
        "histological_type"
    FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."CLINICAL_PANCAN_PATIENT_WITH_FOLLOWUP_FILTERED"
    WHERE "acronym" = 'BRCA' 
        AND "histological_type" IS NOT NULL 
        AND "histological_type" != ''
),
cdh1_mutations AS (
    SELECT DISTINCT "ParticipantBarcode" AS "patient_id"
    FROM "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."MC3_MAF_V5_ONE_PER_TUMOR_SAMPLE"
    WHERE "Hugo_Symbol" = 'CDH1' 
        AND "FILTER" = 'PASS'
        AND "Study" = 'BRCA'
),
patient_data AS (
    SELECT 
        b."patient_id",
        b."histological_type",
        CASE WHEN m."patient_id" IS NOT NULL THEN 1 ELSE 0 END AS has_cdh1_mutation
    FROM brca_patients b
    LEFT JOIN cdh1_mutations m ON b."patient_id" = m."patient_id"
),
contingency AS (
    SELECT 
        "histological_type",
        COUNT(*) AS total_patients,
        SUM(has_cdh1_mutation) AS mutated_count,
        COUNT(*) - SUM(has_cdh1_mutation) AS non_mutated_count
    FROM patient_data
    GROUP BY "histological_type"
    HAVING COUNT(*) > 10
),
column_totals AS (
    SELECT 
        SUM(mutated_count) AS total_mutated,
        SUM(non_mutated_count) AS total_non_mutated,
        SUM(total_patients) AS grand_total
    FROM contingency
),
filtered_contingency AS (
    SELECT 
        c.*
    FROM contingency c
    CROSS JOIN column_totals ct
    WHERE ct.total_mutated > 10 AND ct.total_non_mutated > 10
),
expected_counts AS (
    SELECT 
        "histological_type",
        total_patients,
        mutated_count AS observed_mutated,
        non_mutated_count AS observed_non_mutated,
        (total_patients * total_mutated / grand_total) AS expected_mutated,
        (total_patients * total_non_mutated / grand_total) AS expected_non_mutated
    FROM filtered_contingency
    CROSS JOIN (SELECT total_mutated, total_non_mutated, grand_total FROM column_totals WHERE total_mutated > 10 AND total_non_mutated > 10) ct
),
chi_square_components AS (
    SELECT 
        "histological_type",
        POWER(observed_mutated - expected_mutated, 2) / NULLIF(expected_mutated, 0) AS mutated_component,
        POWER(observed_non_mutated - expected_non_mutated, 2) / NULLIF(expected_non_mutated, 0) AS non_mutated_component
    FROM expected_counts
)
SELECT 
    SUM(mutated_component + non_mutated_component) AS chi_square_value
FROM chi_square_components;