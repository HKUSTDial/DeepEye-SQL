WITH eligible_images AS (
  SELECT DISTINCT d."SOPInstanceUID"
  FROM "IDC"."IDC_V17"."DICOM_ALL" d
  INNER JOIN "IDC"."IDC_V17"."ORIGINAL_COLLECTIONS_METADATA" o
    ON d."collection_name" = o."tcia_api_collection_id"
  INNER JOIN LATERAL FLATTEN(INPUT => d."SpecimenDescriptionSequence") sds
  INNER JOIN LATERAL FLATTEN(INPUT => sds.value:"SpecimenPreparationSequence") sps
  INNER JOIN LATERAL FLATTEN(INPUT => sps.value:"StainingSequence") stain
  WHERE o."collection_name" = 'TCGA-BRCA'
    AND d."Modality" = 'SM'
    AND (stain.value:"Stain"::STRING ILIKE '%eosin%' OR stain.value:"CodeMeaning"::STRING ILIKE '%eosin%')
)
SELECT SUM(COALESCE(TRY_CAST(d."NumberOfFrames" AS INTEGER), 0)) AS total_frames
FROM "IDC"."IDC_V17"."DICOM_ALL" d
INNER JOIN eligible_images e ON d."SOPInstanceUID" = e."SOPInstanceUID"