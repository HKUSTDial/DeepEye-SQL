WITH total_participants AS (
    SELECT COUNT(DISTINCT "person_id") AS total_count
    FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."PERSON"
),
acne_concepts AS (
    SELECT ca."descendant_concept_id" AS concept_id
    FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT" c
    JOIN "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT_RELATIONSHIP" cr ON c."concept_id" = cr."concept_id_1"
    JOIN "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT_ANCESTOR" ca ON cr."concept_id_2" = ca."ancestor_concept_id"
    WHERE c."vocabulary_id" = 'ICD10CM' 
    AND c."concept_code" LIKE 'L70%'
    AND cr."relationship_id" = 'Maps to'
    UNION
    SELECT ca."descendant_concept_id" AS concept_id
    FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT" c
    JOIN "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT_ANCESTOR" ca ON c."concept_id" = ca."ancestor_concept_id"
    WHERE c."vocabulary_id" = 'ICD10CM' 
    AND c."concept_code" LIKE 'L70%'
    AND c."standard_concept" = 'S'
),
atopic_dermatitis_concepts AS (
    SELECT ca."descendant_concept_id" AS concept_id
    FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT" c
    JOIN "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT_RELATIONSHIP" cr ON c."concept_id" = cr."concept_id_1"
    JOIN "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT_ANCESTOR" ca ON cr."concept_id_2" = ca."ancestor_concept_id"
    WHERE c."vocabulary_id" = 'ICD10CM' 
    AND c."concept_code" LIKE 'L20%'
    AND cr."relationship_id" = 'Maps to'
    UNION
    SELECT ca."descendant_concept_id" AS concept_id
    FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT" c
    JOIN "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT_ANCESTOR" ca ON c."concept_id" = ca."ancestor_concept_id"
    WHERE c."vocabulary_id" = 'ICD10CM' 
    AND c."concept_code" LIKE 'L20%'
    AND c."standard_concept" = 'S'
),
psoriasis_concepts AS (
    SELECT ca."descendant_concept_id" AS concept_id
    FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT" c
    JOIN "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT_RELATIONSHIP" cr ON c."concept_id" = cr."concept_id_1"
    JOIN "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT_ANCESTOR" ca ON cr."concept_id_2" = ca."ancestor_concept_id"
    WHERE c."vocabulary_id" = 'ICD10CM' 
    AND c."concept_code" LIKE 'L40%'
    AND cr."relationship_id" = 'Maps to'
    UNION
    SELECT ca."descendant_concept_id" AS concept_id
    FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT" c
    JOIN "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT_ANCESTOR" ca ON c."concept_id" = ca."ancestor_concept_id"
    WHERE c."vocabulary_id" = 'ICD10CM' 
    AND c."concept_code" LIKE 'L40%'
    AND c."standard_concept" = 'S'
),
vitiligo_concepts AS (
    SELECT ca."descendant_concept_id" AS concept_id
    FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT" c
    JOIN "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT_RELATIONSHIP" cr ON c."concept_id" = cr."concept_id_1"
    JOIN "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT_ANCESTOR" ca ON cr."concept_id_2" = ca."ancestor_concept_id"
    WHERE c."vocabulary_id" = 'ICD10CM' 
    AND c."concept_code" LIKE 'L80%'
    AND cr."relationship_id" = 'Maps to'
    UNION
    SELECT ca."descendant_concept_id" AS concept_id
    FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT" c
    JOIN "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONCEPT_ANCESTOR" ca ON c."concept_id" = ca."ancestor_concept_id"
    WHERE c."vocabulary_id" = 'ICD10CM' 
    AND c."concept_code" LIKE 'L80%'
    AND c."standard_concept" = 'S'
),
condition_counts AS (
    SELECT 'Acne' AS condition_name, COUNT(DISTINCT co."person_id") AS participant_count
    FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONDITION_OCCURRENCE" co
    JOIN acne_concepts ac ON co."condition_concept_id" = ac.concept_id
    UNION ALL
    SELECT 'Atopic Dermatitis' AS condition_name, COUNT(DISTINCT co."person_id") AS participant_count
    FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONDITION_OCCURRENCE" co
    JOIN atopic_dermatitis_concepts adc ON co."condition_concept_id" = adc.concept_id
    UNION ALL
    SELECT 'Psoriasis' AS condition_name, COUNT(DISTINCT co."person_id") AS participant_count
    FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONDITION_OCCURRENCE" co
    JOIN psoriasis_concepts pc ON co."condition_concept_id" = pc.concept_id
    UNION ALL
    SELECT 'Vitiligo' AS condition_name, COUNT(DISTINCT co."person_id") AS participant_count
    FROM "CMS_DATA"."CMS_SYNTHETIC_PATIENT_DATA_OMOP"."CONDITION_OCCURRENCE" co
    JOIN vitiligo_concepts vc ON co."condition_concept_id" = vc.concept_id
)
SELECT 
    cc.condition_name,
    cc.participant_count,
    ROUND((cc.participant_count * 100.0 / tp.total_count), 2) AS percentage
FROM condition_counts cc
CROSS JOIN total_participants tp
ORDER BY cc.condition_name