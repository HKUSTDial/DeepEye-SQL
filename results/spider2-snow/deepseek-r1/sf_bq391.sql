WITH eligible_patients AS (
  SELECT DISTINCT p."id" AS patient_id
  FROM "FHIR_SYNTHEA"."FHIR_SYNTHEA"."PATIENT" p,
  LATERAL FLATTEN(INPUT => p."name") name_flat
  WHERE p."deceased" IS NULL
    AND name_flat.value:"family"::string LIKE 'A%'
),
patient_conditions_distinct AS (
  SELECT DISTINCT
    SPLIT_PART(c."subject":"reference"::string, '/', -1) AS patient_id,
    c."code":"coding"[0]:"code"::string AS condition_code
  FROM "FHIR_SYNTHEA"."FHIR_SYNTHEA"."CONDITION" c
  INNER JOIN eligible_patients ep ON SPLIT_PART(c."subject":"reference"::string, '/', -1) = ep.patient_id
  WHERE condition_code IS NOT NULL
),
condition_counts AS (
  SELECT patient_id, COUNT(*) AS distinct_condition_count
  FROM patient_conditions_distinct
  GROUP BY patient_id
  HAVING COUNT(*) = 1
),
patients_with_one_condition AS (
  SELECT pcd.patient_id, pcd.condition_code
  FROM patient_conditions_distinct pcd
  INNER JOIN condition_counts cc ON pcd.patient_id = cc.patient_id
),
medication_counts AS (
  SELECT
    SPLIT_PART(mr."subject":"reference"::string, '/', -1) AS patient_id,
    COUNT(DISTINCT mr."medication":"coding"[0]:"code"::string) AS distinct_medication_count
  FROM "FHIR_SYNTHEA"."FHIR_SYNTHEA"."MEDICATION_REQUEST" mr
  WHERE mr."status" = 'active'
    AND mr."medication":"coding"[0]:"code"::string IS NOT NULL
  GROUP BY patient_id
),
patient_medication_counts AS (
  SELECT
    pwc.patient_id,
    pwc.condition_code,
    COALESCE(mc.distinct_medication_count, 0) AS medication_count
  FROM patients_with_one_condition pwc
  LEFT JOIN medication_counts mc ON pwc.patient_id = mc.patient_id
),
condition_max_meds AS (
  SELECT
    condition_code,
    MAX(medication_count) AS max_medication_count
  FROM patient_medication_counts
  GROUP BY condition_code
)
SELECT condition_code, max_medication_count
FROM condition_max_meds
ORDER BY max_medication_count DESC
LIMIT 8