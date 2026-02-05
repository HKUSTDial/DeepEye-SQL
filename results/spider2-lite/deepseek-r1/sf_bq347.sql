WITH union_data AS (
    SELECT 
        "SOPInstanceUID",
        "Modality"
    FROM "IDC"."IDC_V17"."DICOM_ALL"
    WHERE "SeriesInstanceUID" = '1.3.6.1.4.1.14519.5.2.1.3671.4754.105976129314091491952445656147'
    UNION ALL
    SELECT 
        s."SOPInstanceUID",
        d."Modality"
    FROM "IDC"."IDC_V17"."SEGMENTATIONS" AS s
    JOIN "IDC"."IDC_V17"."DICOM_ALL" AS d
        ON s."SOPInstanceUID" = d."SOPInstanceUID"
    WHERE s."segmented_SeriesInstanceUID" = '1.3.6.1.4.1.14519.5.2.1.3671.4754.105976129314091491952445656147'
)
SELECT 
    "Modality",
    COUNT(*) AS instance_count
FROM union_data
GROUP BY "Modality"
ORDER BY instance_count DESC
LIMIT 1