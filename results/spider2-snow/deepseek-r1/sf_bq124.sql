WITH alive_patients AS (
  SELECT "id" AS patient_id
  FROM "FHIR_SYNTHEA"."FHIR_SYNTHEA"."PATIENT"
  WHERE "deceased":"dateTime" IS NULL
), diabetes_hypertension_patients AS (
  SELECT DISTINCT SPLIT_PART("subject":"reference"::STRING, '/', -1) AS patient_id
  FROM "FHIR_SYNTHEA"."FHIR_SYNTHEA"."CONDITION",
  LATERAL FLATTEN(INPUT => "code":"coding") AS coding
  WHERE (UPPER(coding.value:"display"::STRING) LIKE '%DIABETES%' OR UPPER(coding.value:"display"::STRING) LIKE '%HYPERTENSION%')
), medication_counts AS (
  SELECT SPLIT_PART("subject":"reference"::STRING, '/', -1) AS patient_id
  FROM "FHIR_SYNTHEA"."FHIR_SYNTHEA"."MEDICATION_REQUEST"
  WHERE "status" = 'active'
  GROUP BY patient_id
  HAVING COUNT(DISTINCT "medication":"reference"::STRING) >= 7
)
SELECT COUNT(DISTINCT a.patient_id) AS patient_count
FROM alive_patients a
INNER JOIN diabetes_hypertension_patients d ON a.patient_id = d.patient_id
INNER JOIN medication_counts m ON a.patient_id = m.patient_id