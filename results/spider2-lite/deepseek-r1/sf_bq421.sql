WITH preparation_steps AS (
    SELECT 
        m."SOPInstanceUID",
        prep_step.value:"ConceptNameCodeSequence"[0]."CodeMeaning"::TEXT AS concept_name,
        prep_step.value:"ConceptCodeSequence"[0]."CodeMeaning"::TEXT AS code_meaning,
        prep_step.value:"ConceptCodeSequence"[0]."CodingSchemeDesignator"::TEXT AS coding_scheme
    FROM "IDC"."IDC_V17"."DICOM_METADATA" m
    INNER JOIN LATERAL FLATTEN(INPUT => m."SpecimenDescriptionSequence") AS spec_desc
    INNER JOIN LATERAL FLATTEN(INPUT => spec_desc.value:"SpecimenPreparationSequence") AS prep_step
    WHERE m."Modality" = 'SM'
),
embedding_codes AS (
    SELECT DISTINCT 
        "SOPInstanceUID", 
        code_meaning AS embedding_medium
    FROM preparation_steps
    WHERE concept_name = 'Embedding medium' 
        AND coding_scheme = 'SCT'
        AND code_meaning IS NOT NULL
),
staining_codes AS (
    SELECT DISTINCT 
        "SOPInstanceUID", 
        code_meaning AS staining_substance
    FROM preparation_steps
    WHERE concept_name = 'Staining substance' 
        AND coding_scheme = 'SCT'
        AND code_meaning IS NOT NULL
)
SELECT 
    e.embedding_medium,
    s.staining_substance,
    COUNT(DISTINCT e."SOPInstanceUID") AS occurrence_count
FROM embedding_codes e
INNER JOIN staining_codes s ON e."SOPInstanceUID" = s."SOPInstanceUID"
GROUP BY e.embedding_medium, s.staining_substance
ORDER BY occurrence_count DESC