WITH quinapril_concept AS (
  SELECT concept_id
  FROM `bigquery-public-data.cms_synthetic_patient_data_omop.concept`
  WHERE vocabulary_id = 'RxNorm' AND concept_code = '35208'
),
descendant_concepts AS (
  SELECT descendant_concept_id
  FROM `bigquery-public-data.cms_synthetic_patient_data_omop.concept_ancestor`
  WHERE ancestor_concept_id IN (SELECT concept_id FROM quinapril_concept)
),
users_quinapril AS (
  SELECT DISTINCT person_id
  FROM `bigquery-public-data.cms_synthetic_patient_data_omop.drug_exposure`
  WHERE drug_concept_id IN (SELECT descendant_concept_id FROM descendant_concepts)
),
total_persons AS (
  SELECT COUNT(DISTINCT person_id) AS total_count
  FROM `bigquery-public-data.cms_synthetic_patient_data_omop.person`
)
SELECT 100.0 * (total_count - (SELECT COUNT(DISTINCT person_id) FROM users_quinapril)) / total_count AS percentage_not_using
FROM total_persons