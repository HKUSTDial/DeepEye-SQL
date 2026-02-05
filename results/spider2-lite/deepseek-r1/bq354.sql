WITH icd10_concepts AS (
  SELECT concept_id as icd10_concept_id, concept_code, 
         CASE concept_code
           WHEN 'L70' THEN 'Acne'
           WHEN 'L20' THEN 'Atopic dermatitis'
           WHEN 'L40' THEN 'Psoriasis'
           WHEN 'L80' THEN 'Vitiligo'
         END as condition_name
  FROM `bigquery-public-data.cms_synthetic_patient_data_omop.concept`
  WHERE vocabulary_id = 'ICD10CM'
  AND concept_code IN ('L70', 'L20', 'L40', 'L80')
),
mapped_standard_concepts AS (
  SELECT DISTINCT cr.concept_id_2 as standard_concept_id, 
         ic.concept_code, ic.condition_name
  FROM icd10_concepts ic
  JOIN `bigquery-public-data.cms_synthetic_patient_data_omop.concept_relationship` cr
    ON ic.icd10_concept_id = cr.concept_id_1
  WHERE cr.relationship_id = 'Maps to'
),
all_condition_concepts AS (
  SELECT DISTINCT ca.descendant_concept_id as condition_concept_id,
         msc.concept_code, msc.condition_name
  FROM mapped_standard_concepts msc
  JOIN `bigquery-public-data.cms_synthetic_patient_data_omop.concept_ancestor` ca
    ON msc.standard_concept_id = ca.ancestor_concept_id
  UNION ALL
  SELECT msc.standard_concept_id as condition_concept_id,
         msc.concept_code, msc.condition_name
  FROM mapped_standard_concepts msc
),
condition_participants AS (
  SELECT acc.condition_name, acc.concept_code,
         COUNT(DISTINCT co.person_id) as participant_count
  FROM all_condition_concepts acc
  JOIN `bigquery-public-data.cms_synthetic_patient_data_omop.condition_occurrence` co
    ON acc.condition_concept_id = co.condition_concept_id
  GROUP BY acc.condition_name, acc.concept_code
),
total_participants AS (
  SELECT COUNT(DISTINCT person_id) as total_count
  FROM `bigquery-public-data.cms_synthetic_patient_data_omop.person`
)
SELECT 
  cp.condition_name,
  cp.concept_code as icd10_code,
  cp.participant_count,
  tp.total_count,
  ROUND((cp.participant_count * 100.0 / tp.total_count), 2) as percentage
FROM condition_participants cp
CROSS JOIN total_participants tp
ORDER BY cp.concept_code