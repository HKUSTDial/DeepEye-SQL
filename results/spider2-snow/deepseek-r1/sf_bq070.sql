WITH filtered_images AS (
    SELECT
        da."PatientID",
        da."collection_id",
        da."SOPInstanceUID",
        da."gcs_url",
        da."Columns" AS "width",
        da."Rows" AS "height",
        da."PixelSpacing",
        da."TransferSyntaxUID",
        da."CodeValue",
        da."SpecimenDescriptionSequence"
    FROM
        "IDC"."IDC_V17"."DICOM_ALL" AS da
    WHERE
        da."Modality" = 'SM'
        AND da."VolumetricProperties" = 'VOLUME'
        AND da."collection_id" IN ('tcga_luad', 'tcga_lusc')
        AND da."TransferSyntaxUID" IN (
            '1.2.840.10008.1.2.4.50',
            '1.2.840.10008.1.2.4.51',
            '1.2.840.10008.1.2.4.57',
            '1.2.840.10008.1.2.4.70',
            '1.2.840.10008.1.2.4.90',
            '1.2.840.10008.1.2.4.91'
        )
        AND da."CodeValue" IN ('17621005', '86049000')
),
specimen_details AS (
    SELECT
        fi.*,
        f.value:"EmbeddingMedium"::STRING AS "embedding_medium"
    FROM
        filtered_images AS fi,
        LATERAL FLATTEN(INPUT => fi."SpecimenDescriptionSequence") AS f
    WHERE
        f.value:"EmbeddingMedium" IS NOT NULL
)
SELECT DISTINCT
    NULL AS digital_slide_id,
    NULL AS case_id,
    NULL AS physical_slide_id,
    sd."PatientID" AS patient_id,
    sd."collection_id" AS collection_id,
    sd."SOPInstanceUID" AS instance_id,
    sd."gcs_url" AS gcs_url,
    sd."width",
    sd."height",
    sd."PixelSpacing" AS pixel_spacing,
    CASE
        WHEN sd."TransferSyntaxUID" IN ('1.2.840.10008.1.2.4.50', '1.2.840.10008.1.2.4.51', '1.2.840.10008.1.2.4.57', '1.2.840.10008.1.2.4.70') THEN 'jpeg'
        WHEN sd."TransferSyntaxUID" IN ('1.2.840.10008.1.2.4.90', '1.2.840.10008.1.2.4.91') THEN 'jpeg2000'
        ELSE 'other'
    END AS compression_type,
    CASE
        WHEN sd."CodeValue" = '17621005' THEN 'normal'
        WHEN sd."CodeValue" = '86049000' THEN 'tumor'
    END AS tissue_type,
    CASE
        WHEN sd."collection_id" = 'tcga_luad' THEN 'luad'
        WHEN sd."collection_id" = 'tcga_lusc' THEN 'lscc'
    END AS cancer_subtype
FROM
    specimen_details AS sd
WHERE
    sd."embedding_medium" = 'Tissue freezing medium'
ORDER BY
    sd."SOPInstanceUID" ASC