SELECT SUM(TRY_TO_NUMBER("NumberOfFrames"))
FROM (
  SELECT DISTINCT d.*
  FROM "IDC"."IDC_V17"."DICOM_ALL" d
  INNER JOIN LATERAL FLATTEN(INPUT => d."SpecimenDescriptionSequence", RECURSIVE => TRUE) f
  WHERE f."PATH" LIKE '%SpecimenPreparationSequence%'
    AND f."VALUE"::STRING ILIKE '%eosin%'
    AND d."collection_name" = 'TCGA-BRCA'
    AND d."Modality" = 'SM'
) d