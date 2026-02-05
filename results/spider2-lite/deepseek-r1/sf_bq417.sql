SELECT 
    "PatientID",
    "StudyInstanceUID",
    "SeriesInstanceUID",
    "StudyDescription",
    "SeriesDescription",
    "series_aws_url" AS storage_location,
    ROUND(SUM("instance_size") / 1000000, 2) AS total_size_mb
FROM "IDC"."IDC_V17"."DICOM_ALL"
WHERE "PatientSex" = 'M'
    AND "BodyPartExamined" = 'MEDIASTINUM'
    AND "StudyDate" > '2014-09-01'
    AND TRY_TO_NUMBER(REGEXP_SUBSTR("PatientAge", '^[0-9]+')) = 18
GROUP BY 
    "PatientID",
    "StudyInstanceUID",
    "SeriesInstanceUID",
    "StudyDescription",
    "SeriesDescription",
    "series_aws_url"