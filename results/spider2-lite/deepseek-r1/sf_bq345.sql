SELECT
  "collection_id",
  "StudyInstanceUID",
  "SeriesInstanceUID",
  'https://viewer.imaging.datacommons.cancer.gov/viewer/' || "StudyInstanceUID" AS viewer_url,
  SUM("instance_size") / 1024.0 AS total_size_kb
FROM "IDC"."IDC_V17"."DICOM_ALL"
WHERE
  "Modality" IN ('SEG', 'RTSTRUCT')
  AND "SOPClassUID" = '1.2.840.10008.5.1.4.1.1.66.4'
  AND ARRAY_SIZE("ReferencedSeriesSequence") = 0
  AND ARRAY_SIZE("ReferencedImageSequence") = 0
  AND ARRAY_SIZE("SourceImageSequence") = 0
GROUP BY
  "collection_id",
  "StudyInstanceUID",
  "SeriesInstanceUID"
ORDER BY total_size_kb DESC