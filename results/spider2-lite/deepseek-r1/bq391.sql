WITH living_a_patients AS (
  SELECT `id`
  FROM `bigquery-public-data.fhir_synthea.patient`
  WHERE (`deceased`.`boolean` IS NULL OR `deceased`.`boolean` = FALSE)
    AND EXISTS (SELECT 1 FROM UNNEST(`name`) n WHERE n.family LIKE 'A%')
),
patient_conditions AS (
  SELECT 
    c.`subject`.`patientId` AS patient_id,
    cc.`code` AS condition_code,
    cc.`display` AS condition_display
  FROM `bigquery-public-data.fhir_synthea.condition` c
  CROSS JOIN UNNEST(c.`code`.`coding`) cc
  WHERE c.`subject`.`patientId` IN (SELECT `id` FROM living_a_patients)
),
patient_condition_count AS (
  SELECT 
    patient_id,
    COUNT(DISTINCT condition_code) AS distinct_condition_count
  FROM patient_conditions
  GROUP BY patient_id
),
patients_one_condition AS (
  SELECT patient_id
  FROM patient_condition_count
  WHERE distinct_condition_count = 1
),
patient_single_condition AS (
  SELECT 
    pc.patient_id,
    pc.condition_code,
    pc.condition_display
  FROM patient_conditions pc
  INNER JOIN patients_one_condition poc ON pc.patient_id = poc.patient_id
  QUALIFY ROW_NUMBER() OVER (PARTITION BY pc.patient_id ORDER BY pc.condition_code) = 1
),
patient_medication_counts AS (
  SELECT 
    mr.`subject`.`patientId` AS patient_id,
    COUNT(DISTINCT mc.`code`) AS distinct_medication_count
  FROM `bigquery-public-data.fhir_synthea.medication_request` mr
  CROSS JOIN UNNEST(mr.`medication`.`codeableConcept`.`coding`) mc
  WHERE mr.`status` = 'active'
    AND mr.`subject`.`patientId` IN (SELECT patient_id FROM patients_one_condition)
  GROUP BY mr.`subject`.`patientId`
),
patient_data AS (
  SELECT 
    psc.patient_id,
    psc.condition_code,
    psc.condition_display,
    COALESCE(pmc.distinct_medication_count, 0) AS med_count
  FROM patient_single_condition psc
  LEFT JOIN patient_medication_counts pmc ON psc.patient_id = pmc.patient_id
),
condition_max_meds AS (
  SELECT 
    condition_code,
    condition_display,
    MAX(med_count) AS max_meds
  FROM patient_data
  GROUP BY condition_code, condition_display
)
SELECT 
  condition_display,
  condition_code,
  max_meds
FROM condition_max_meds
ORDER BY max_meds DESC
LIMIT 8