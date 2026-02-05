WITH filtered_instances AS (
    SELECT 
        "SeriesInstanceUID",
        "SeriesNumber",
        "StudyInstanceUID",
        "PatientID",
        "SOPInstanceUID",
        "ImageType",
        "TransferSyntaxUID",
        "ImageOrientationPatient",
        "PixelSpacing",
        "ImagePositionPatient",
        "Rows",
        "Columns",
        "SliceThickness",
        "Exposure",
        "instance_size"
    FROM "IDC"."IDC_V17"."DICOM_ALL"
    WHERE "Modality" = 'CT' AND "collection_id" != 'nlst'
),
series_exclusions AS (
    SELECT 
        "SeriesInstanceUID",
        MAX(CASE WHEN "TransferSyntaxUID" IN ('1.2.840.10008.1.2.4.70', '1.2.840.10008.1.2.4.51') THEN 1 ELSE 0 END) AS bad_transfer,
        MAX(CASE WHEN UPPER("ImageType"::STRING) LIKE '%LOCALIZER%' THEN 1 ELSE 0 END) AS has_localizer
    FROM filtered_instances
    GROUP BY "SeriesInstanceUID"
),
valid_series_uid AS (
    SELECT "SeriesInstanceUID" FROM series_exclusions WHERE bad_transfer = 0 AND has_localizer = 0
),
valid_instances AS (
    SELECT 
        fi.*,
        (fi."ImageOrientationPatient"[0]::FLOAT * fi."ImageOrientationPatient"[4]::FLOAT - fi."ImageOrientationPatient"[1]::FLOAT * fi."ImageOrientationPatient"[3]::FLOAT) AS dot_product,
        fi."ImagePositionPatient"[2]::FLOAT AS z_position,
        fi."ImagePositionPatient"[0]::STRING || ',' || fi."ImagePositionPatient"[1]::STRING AS xy_position,
        fi."ImageOrientationPatient"::STRING AS iop_string,
        fi."PixelSpacing"::STRING AS ps_string,
        fi."ImagePositionPatient"::STRING AS ipp_string,
        TRY_CAST(fi."Exposure" AS FLOAT) AS exposure_numeric
    FROM filtered_instances fi
    INNER JOIN valid_series_uid vs ON fi."SeriesInstanceUID" = vs."SeriesInstanceUID"
),
instance_with_lag AS (
    SELECT 
        *,
        LAG(z_position) OVER (PARTITION BY "SeriesInstanceUID" ORDER BY z_position) AS prev_z
    FROM valid_instances
),
series_stats AS (
    SELECT 
        "SeriesInstanceUID",
        MAX("SeriesNumber") AS "SeriesNumber",
        MAX("StudyInstanceUID") AS "StudyInstanceUID",
        MAX("PatientID") AS "PatientID",
        COUNT(DISTINCT "SOPInstanceUID") AS sop_count,
        COUNT(DISTINCT "SliceThickness") AS distinct_slice_thickness,
        MAX(dot_product) AS max_dot_product,
        MIN(dot_product) AS min_dot_product,
        COUNT(DISTINCT iop_string) AS distinct_iop,
        COUNT(DISTINCT ps_string) AS distinct_pixel_spacing,
        COUNT(DISTINCT "Rows") AS distinct_rows,
        COUNT(DISTINCT "Columns") AS distinct_columns,
        COUNT(DISTINCT xy_position) AS distinct_xy,
        COUNT(DISTINCT ipp_string) AS distinct_ipp,
        COUNT(DISTINCT exposure_numeric) AS distinct_exposure,
        MIN(exposure_numeric) AS min_exposure,
        MAX(exposure_numeric) AS max_exposure,
        (MAX(exposure_numeric) - MIN(exposure_numeric)) AS exposure_diff,
        SUM("instance_size") AS total_bytes,
        MAX(CASE WHEN prev_z IS NOT NULL THEN z_position - prev_z END) AS max_slice_interval,
        MIN(CASE WHEN prev_z IS NOT NULL THEN z_position - prev_z END) AS min_slice_interval
    FROM instance_with_lag
    GROUP BY "SeriesInstanceUID"
),
final_series AS (
    SELECT 
        "SeriesInstanceUID",
        "SeriesNumber",
        "StudyInstanceUID",
        "PatientID",
        max_dot_product,
        sop_count,
        distinct_slice_thickness,
        max_slice_interval,
        min_slice_interval,
        (max_slice_interval - min_slice_interval) AS slice_interval_tolerance,
        distinct_exposure,
        min_exposure,
        max_exposure,
        exposure_diff,
        total_bytes / (1024 * 1024) AS total_mb
    FROM series_stats
    WHERE distinct_iop = 1
        AND distinct_pixel_spacing = 1
        AND distinct_rows = 1
        AND distinct_columns = 1
        AND distinct_xy = 1
        AND sop_count = distinct_ipp
        AND min_dot_product >= 0.99
        AND max_dot_product <= 1.01
)
SELECT 
    "SeriesInstanceUID",
    "SeriesNumber",
    "StudyInstanceUID",
    "PatientID",
    max_dot_product,
    sop_count,
    distinct_slice_thickness,
    max_slice_interval,
    min_slice_interval,
    slice_interval_tolerance,
    distinct_exposure,
    min_exposure,
    max_exposure,
    exposure_diff,
    total_mb
FROM final_series
ORDER BY slice_interval_tolerance DESC, exposure_diff DESC, "SeriesInstanceUID" DESC