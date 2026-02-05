SELECT COUNT(DISTINCT "StudyInstanceUID")
FROM "IDC"."IDC_V17"."DICOM_ALL"
WHERE "collection_id" = 'qin_prostate_repeatability'
  AND (
    LOWER("SeriesDescription") LIKE '%dwi%'
    OR LOWER("SeriesDescription") LIKE '%t2 weighted axial%'
    OR LOWER("SeriesDescription") LIKE '%apparent diffusion coefficient%'
    OR ("Modality" = 'SEG' AND LOWER("SeriesDescription") LIKE '%t2 weighted axial%')
  )