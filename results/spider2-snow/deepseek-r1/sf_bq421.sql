WITH content_items AS (
    SELECT 
        d."SOPInstanceUID",
        spec_desc.index AS specimen_index,
        spec_desc.value AS specimen_desc,
        prep_seq.index AS prep_index,
        content_item.value AS content_item_val,
        content_item_val:"ConceptNameCodeSequence"[0]:"CodeMeaning"::TEXT AS concept_name,
        content_item_val:"ConceptCodeSequence"[0]:"CodeMeaning"::TEXT AS substance_code_meaning,
        content_item_val:"ConceptCodeSequence"[0]:"CodingSchemeDesignator"::TEXT AS substance_coding_scheme
    FROM "IDC"."IDC_V17"."DICOM_ALL" AS d
    CROSS JOIN LATERAL FLATTEN(INPUT => d."SpecimenDescriptionSequence") AS spec_desc
    CROSS JOIN LATERAL FLATTEN(INPUT => spec_desc.value:"SpecimenPreparationSequence") AS prep_seq
    CROSS JOIN LATERAL FLATTEN(INPUT => prep_seq.value:"SpecimenPreparationStepContentItemSequence") AS content_item
    WHERE d."Modality" = 'SM'
),
filtered_items AS (
    SELECT 
        "SOPInstanceUID",
        specimen_index,
        prep_index,
        concept_name,
        substance_code_meaning
    FROM content_items
    WHERE substance_coding_scheme = 'SCT'
        AND concept_name IN ('Embedding Medium', 'Staining Substance')
),
specimen_aggregate AS (
    SELECT 
        "SOPInstanceUID",
        specimen_index,
        MAX(CASE WHEN concept_name = 'Embedding Medium' THEN substance_code_meaning END) AS embedding_medium,
        MAX(CASE WHEN concept_name = 'Staining Substance' THEN substance_code_meaning END) AS staining_substance
    FROM filtered_items
    GROUP BY "SOPInstanceUID", specimen_index
)
SELECT 
    embedding_medium,
    staining_substance,
    COUNT(*) AS occurrence_count
FROM specimen_aggregate
WHERE embedding_medium IS NOT NULL AND staining_substance IS NOT NULL
GROUP BY embedding_medium, staining_substance
ORDER BY occurrence_count DESC;