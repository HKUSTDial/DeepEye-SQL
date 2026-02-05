WITH base AS (
  SELECT 
    *,
    CASE 
      WHEN "TransferSyntaxUID" IN ('1.2.840.10008.1.2.4.50', '1.2.840.10008.1.2.4.51', '1.2.840.10008.1.2.4.52', '1.2.840.10008.1.2.4.53', '1.2.840.10008.1.2.4.54', '1.2.840.10008.1.2.4.55', '1.2.840.10008.1.2.4.56', '1.2.840.10008.1.2.4.57', '1.2.840.10008.1.2.4.58') THEN 'jpeg'
      WHEN "TransferSyntaxUID" IN ('1.2.840.10008.1.2.4.90', '1.2.840.10008.1.2.4.91') THEN 'jpeg2000'
      ELSE 'other'
    END AS compression_type
  FROM "IDC"."IDC_V17"."DICOM_ALL"
  WHERE "Modality" = 'SM'
    AND "VolumetricProperties" = 'VOLUME'
    AND "collection_id" IN ('TCGA-LUAD', 'TCGA-LUSC')
),
filtered_compression AS (
  SELECT * FROM base WHERE compression_type != 'other'
),
specimen_match AS (
  SELECT DISTINCT
    fc."SOPInstanceUID",
    FIRST_VALUE(spec.value:"SpecimenIdentifier"::STRING) OVER (PARTITION BY fc."SOPInstanceUID" ORDER BY spec.seq) AS physical_slide_id
  FROM filtered_compression fc
  LEFT JOIN LATERAL FLATTEN(INPUT => fc."SpecimenDescriptionSequence") spec
  WHERE spec.value:"EmbeddingMedium"::STRING = 'Tissue freezing medium'
),
tissue_match AS (
  SELECT DISTINCT
    fc."SOPInstanceUID",
    FIRST_VALUE(anat.value:"CodeValue"::STRING) OVER (PARTITION BY fc."SOPInstanceUID" ORDER BY anat.seq) AS tissue_code
  FROM filtered_compression fc
  LEFT JOIN LATERAL FLATTEN(INPUT => fc."AnatomicRegionSequence") anat
  WHERE anat.value:"CodeValue"::STRING IN ('17621005', '86049000')
)
SELECT
  HASH(fc."SOPInstanceUID") AS digital_slide_id,
  HASH(fc."StudyInstanceUID") AS case_id,
  sm.physical_slide_id,
  fc."PatientID" AS patient_id,
  fc."collection_id" AS collection_id,
  fc."SOPInstanceUID" AS instance_id,
  fc."gcs_url" AS gcs_url,
  fc."Columns" AS width,
  fc."Rows" AS height,
  TRY_CAST(fc."PixelSpacing"[1]::STRING AS FLOAT) AS pixel_spacing_x,
  TRY_CAST(fc."PixelSpacing"[0]::STRING AS FLOAT) AS pixel_spacing_y,
  fc.compression_type,
  CASE tm.tissue_code
    WHEN '17621005' THEN 'normal'
    WHEN '86049000' THEN 'tumor'
  END AS tissue_type,
  CASE fc."collection_id"
    WHEN 'TCGA-LUAD' THEN 'luad'
    WHEN 'TCGA-LUSC' THEN 'lscc'
  END AS cancer_subtype
FROM filtered_compression fc
INNER JOIN specimen_match sm ON fc."SOPInstanceUID" = sm."SOPInstanceUID"
INNER JOIN tissue_match tm ON fc."SOPInstanceUID" = tm."SOPInstanceUID"
ORDER BY fc."SOPInstanceUID" ASC