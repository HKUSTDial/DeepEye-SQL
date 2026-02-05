SELECT DISTINCT "StudyInstanceUID"
FROM (
    SELECT DISTINCT d."StudyInstanceUID"
    FROM "IDC"."IDC_V17"."DICOM_ALL" d
    JOIN LATERAL FLATTEN(INPUT => d."ImageType") img
    WHERE d."collection_id" = 'qin_prostate_repeatability'
      AND d."Modality" = 'MR'
      AND UPPER(img.VALUE::STRING) LIKE '%AXIAL%'
      AND (UPPER(d."ProtocolName") LIKE '%T2%' OR UPPER(d."SeriesDescription") LIKE '%T2%')
) AS t2_studies
INTERSECT
SELECT DISTINCT "StudyInstanceUID"
FROM (
    SELECT DISTINCT s."StudyInstanceUID"
    FROM "IDC"."IDC_V17"."SEGMENTATIONS" s
    JOIN "IDC"."IDC_V17"."AUXILIARY_METADATA" a
        ON s."StudyInstanceUID" = a."StudyInstanceUID"
        AND s."SOPInstanceUID" = a."SOPInstanceUID"
    JOIN LATERAL FLATTEN(INPUT => s."SegmentedPropertyType") seg
    WHERE a."collection_id" = 'qin_prostate_repeatability'
      AND UPPER(seg.VALUE::STRING) LIKE '%PERIPHERAL ZONE%'
) AS segmentation_studies