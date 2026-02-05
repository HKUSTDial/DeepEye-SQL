WITH CT_Images AS (
    SELECT 
        "SeriesInstanceUID",
        "SeriesNumber",
        "PatientID",
        "instance_size",
        "ImageType",
        "TransferSyntaxUID",
        "Exposure",
        "ImageOrientationPatient",
        "PixelSpacing",
        "Rows",
        "Columns",
        "ImagePositionPatient",
        "collection_id",
        ARRAY_CONTAINS('LOCALIZER'::VARIANT, COALESCE("ImageType", ARRAY_CONSTRUCT())) AS is_localizer
    FROM "IDC"."IDC_V17"."DICOM_ALL"
    WHERE "Modality" = 'CT'
), Distinct_Z AS (
    SELECT 
        "SeriesInstanceUID",
        GET("ImagePositionPatient", 2)::FLOAT AS z_pos
    FROM CT_Images
    GROUP BY "SeriesInstanceUID", z_pos
), Z_Intervals AS (
    SELECT 
        "SeriesInstanceUID",
        STDDEV(diff) AS stddev_diff,
        MIN(diff) AS min_diff,
        MAX(diff) AS max_diff
    FROM (
        SELECT 
            "SeriesInstanceUID",
            z_pos,
            z_pos - LAG(z_pos) OVER (PARTITION BY "SeriesInstanceUID" ORDER BY z_pos) AS diff
        FROM Distinct_Z
    ) t
    WHERE diff IS NOT NULL
    GROUP BY "SeriesInstanceUID"
), Series_Aggregates AS (
    SELECT 
        ct."SeriesInstanceUID",
        ANY_VALUE(ct."SeriesNumber") AS "SeriesNumber",
        ANY_VALUE(ct."PatientID") AS "PatientID",
        SUM(ct."instance_size") AS total_bytes,
        COUNT(*) AS image_count,
        SUM(CASE WHEN ct.is_localizer THEN 1 ELSE 0 END) AS localizer_count,
        SUM(CASE WHEN ct."TransferSyntaxUID" IN ('1.2.840.10008.1.2.4.70', '1.2.840.10008.1.2.4.51') THEN 1 ELSE 0 END) AS excluded_transfer_count,
        SUM(CASE WHEN ct."collection_id" = 'nlst' THEN 1 ELSE 0 END) AS nlst_count,
        COUNT(DISTINCT ct."SeriesNumber") AS distinct_series_num,
        COUNT(DISTINCT ct."PatientID") AS distinct_patient_id,
        COUNT(DISTINCT ct."Exposure") AS distinct_exposure,
        COUNT(DISTINCT ct."ImageOrientationPatient") AS distinct_orientation,
        COUNT(DISTINCT ct."PixelSpacing") AS distinct_pixel_spacing,
        COUNT(DISTINCT ct."Rows") AS distinct_rows,
        COUNT(DISTINCT ct."Columns") AS distinct_columns,
        COUNT(DISTINCT GET(ct."ImagePositionPatient", 0)::FLOAT) AS distinct_x_pos,
        COUNT(DISTINCT GET(ct."ImagePositionPatient", 1)::FLOAT) AS distinct_y_pos,
        COUNT(DISTINCT GET(ct."ImagePositionPatient", 2)::FLOAT) AS distinct_z_pos,
        ANY_VALUE(GET(ct."ImageOrientationPatient", 0)::FLOAT) AS orient_x1,
        ANY_VALUE(GET(ct."ImageOrientationPatient", 1)::FLOAT) AS orient_y1,
        ANY_VALUE(GET(ct."ImageOrientationPatient", 3)::FLOAT) AS orient_x2,
        ANY_VALUE(GET(ct."ImageOrientationPatient", 4)::FLOAT) AS orient_y2
    FROM CT_Images ct
    GROUP BY ct."SeriesInstanceUID"
), Filtered_Series AS (
    SELECT 
        sa."SeriesInstanceUID",
        sa."SeriesNumber",
        sa."PatientID",
        sa.total_bytes / POWER(1024, 2) AS series_size_mib,
        ABS((sa.orient_x1 * sa.orient_y2) - (sa.orient_y1 * sa.orient_x2)) AS cross_z
    FROM Series_Aggregates sa
    LEFT JOIN Z_Intervals zi ON sa."SeriesInstanceUID" = zi."SeriesInstanceUID"
    WHERE sa.localizer_count = 0
        AND sa.excluded_transfer_count = 0
        AND sa.nlst_count = 0
        AND sa.distinct_series_num = 1
        AND sa.distinct_patient_id = 1
        AND sa.distinct_exposure = 1
        AND sa.distinct_orientation = 1
        AND sa.distinct_pixel_spacing = 1
        AND sa.distinct_rows = 1
        AND sa.distinct_columns = 1
        AND sa.distinct_x_pos = 1
        AND sa.distinct_y_pos = 1
        AND sa.distinct_z_pos = sa.image_count
        AND ABS((sa.orient_x1 * sa.orient_y2) - (sa.orient_y1 * sa.orient_x2)) BETWEEN 0.99 AND 1.01
        AND (zi.stddev_diff IS NULL OR ABS(zi.max_diff - zi.min_diff) < 0.001)
)
SELECT 
    "SeriesInstanceUID",
    "SeriesNumber",
    "PatientID",
    series_size_mib AS "series_size"
FROM Filtered_Series
ORDER BY series_size_mib DESC
LIMIT 5