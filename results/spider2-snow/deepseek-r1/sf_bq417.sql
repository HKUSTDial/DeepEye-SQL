SELECT 
    MAX(dp."PatientID") AS "PatientID",
    dp."StudyInstanceUID",
    dp."SeriesInstanceUID",
    MAX(dp."StudyDescription") AS "StudyDescription",
    MAX(dp."SeriesDescription") AS "SeriesDescription",
    MAX(dp."StudyDate") AS "StudyDate",
    MAX(dp."SeriesDate") AS "SeriesDate",
    MAX(dp."Modality") AS "Modality",
    MAX(dp."Manufacturer") AS "Manufacturer",
    MAX(dp."ManufacturerModelName") AS "ManufacturerModelName",
    MAX(am."series_aws_url") AS "AWS_Series_URL",
    REGEXP_REPLACE(MIN(da."gcs_url"), '/[^/]+$', '') AS "GCS_Series_URL",
    ROUND(SUM(COALESCE(da."instance_size", dp."instance_size")) / 1000000, 2) AS "Series_Size_MB"
FROM "IDC"."IDC_V17"."DICOM_PIVOT" dp
LEFT JOIN "IDC"."IDC_V17"."DICOM_ALL" da ON dp."SeriesInstanceUID" = da."SeriesInstanceUID"
LEFT JOIN "IDC"."IDC_V17"."AUXILIARY_METADATA" am ON dp."SeriesInstanceUID" = am."SeriesInstanceUID"
WHERE dp."PatientSex" = 'M'
  AND TRY_TO_NUMBER(REGEXP_SUBSTR(dp."PatientAge", '^[0-9]+')) = 18
  AND dp."BodyPartExamined" = 'MEDIASTINUM'
  AND dp."StudyDate" > '2014-09-01'
GROUP BY dp."StudyInstanceUID", dp."SeriesInstanceUID"