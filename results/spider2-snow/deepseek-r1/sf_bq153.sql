SELECT
    c."icd_o_3_histology",
    AVG(LOG(10, e."normalized_count" + 1)) AS avg_log_expr
FROM
    "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."CLINICAL_PANCAN_PATIENT_WITH_FOLLOWUP_FILTERED" c
    INNER JOIN "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."EBPP_ADJUSTPANCAN_ILLUMINAHISEQ_RNASEQV2_GENEXP_FILTERED" e
        ON c."bcr_patient_barcode" = e."ParticipantBarcode"
WHERE
    c."acronym" = 'LGG'
    AND c."icd_o_3_histology" NOT LIKE '[%]'
    AND e."Symbol" = 'IGF2'
    AND e."normalized_count" IS NOT NULL
    AND e."Study" = 'LGG'
GROUP BY
    c."icd_o_3_histology"