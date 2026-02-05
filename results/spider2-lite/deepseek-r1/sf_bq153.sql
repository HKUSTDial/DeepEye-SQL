WITH patient_avg AS (
  SELECT 
    "ParticipantBarcode",
    AVG(LOG(10, "normalized_count" + 1)) AS patient_log_avg
  FROM 
    "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."EBPP_ADJUSTPANCAN_ILLUMINAHISEQ_RNASEQV2_GENEXP_FILTERED"
  WHERE 
    "Symbol" = 'IGF2'
  GROUP BY 
    "ParticipantBarcode"
)
SELECT 
  c."icd_o_3_histology",
  AVG(pa.patient_log_avg) AS avg_log_expr
FROM 
  "PANCANCER_ATLAS_1"."PANCANCER_ATLAS_FILTERED"."CLINICAL_PANCAN_PATIENT_WITH_FOLLOWUP_FILTERED" c
INNER JOIN 
  patient_avg pa ON c."bcr_patient_barcode" = pa."ParticipantBarcode"
WHERE 
  c."acronym" = 'LGG'
  AND c."icd_o_3_histology" NOT LIKE '[%]'
GROUP BY 
  c."icd_o_3_histology"