WITH alive_patients AS (
  SELECT id AS patient_id
  FROM `bigquery-public-data.fhir_synthea.patient`
  WHERE deceased.dateTime IS NULL
),
diabetes_hypertension_patients AS (
  SELECT DISTINCT subject.patientId AS patient_id
  FROM `bigquery-public-data.fhir_synthea.condition`,
  UNNEST(code.coding) AS coding
  WHERE LOWER(coding.display) LIKE '%diabetes%' OR LOWER(coding.display) LIKE '%hypertension%'
),
patients_with_7_meds AS (
  SELECT subject.patientId AS patient_id
  FROM `bigquery-public-data.fhir_synthea.medication_request`,
  UNNEST(medication.codeableConcept.coding) AS coding
  WHERE status = 'active'
  GROUP BY subject.patientId
  HAVING COUNT(DISTINCT coding.code) >= 7
)
SELECT COUNT(DISTINCT a.patient_id) AS patient_count
FROM alive_patients a
INNER JOIN diabetes_hypertension_patients d ON a.patient_id = d.patient_id
INNER JOIN patients_with_7_meds m ON a.patient_id = m.patient_id