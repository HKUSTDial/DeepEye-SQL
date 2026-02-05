SELECT COUNT(DISTINCT "StudyInstanceUID")
FROM "IDC"."IDC_V17"."DICOM_PIVOT"
WHERE "collection_id" IN ('Community', 'nsclc_radiomics')
  AND LOWER("SegmentedPropertyTypeCodeSequence") = '15825003'