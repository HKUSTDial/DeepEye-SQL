WITH quinapril_concept AS (
    SELECT "concept_id" FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT" WHERE "concept_code" = '35208' AND "vocabulary_id" IN ('RxNorm', 'RxNorm Extension')
), descendant_concepts AS (
    SELECT "descendant_concept_id" AS "concept_id" FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT_ANCESTOR" WHERE "ancestor_concept_id" IN (SELECT "concept_id" FROM quinapril_concept)
    UNION
    SELECT "concept_id" FROM quinapril_concept
), exposed_persons AS (
    SELECT DISTINCT "person_id" FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."DRUG_EXPOSURE" WHERE "drug_concept_id" IN (SELECT "concept_id" FROM descendant_concepts)
    UNION
    SELECT DISTINCT "person_id" FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."DOSE_ERA" WHERE "drug_concept_id" IN (SELECT "concept_id" FROM descendant_concepts)
    UNION
    SELECT DISTINCT "person_id" FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."DRUG_ERA" WHERE "drug_concept_id" IN (SELECT "concept_id" FROM descendant_concepts)
), total_persons AS (
    SELECT COUNT(DISTINCT "person_id") AS total_count FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."PERSON"
), exposed_count AS (
    SELECT COUNT(DISTINCT "person_id") AS exposed_count FROM exposed_persons
)
SELECT (total_count - exposed_count) * 100.0 / total_count AS percentage_not_exposed FROM total_persons, exposed_count