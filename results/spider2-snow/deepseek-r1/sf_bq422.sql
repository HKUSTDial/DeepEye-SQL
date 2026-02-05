WITH base AS (
    SELECT
        "PatientID",
        "SeriesInstanceUID",
        "SOPInstanceUID",
        "instance_size",
        TRY_TO_DOUBLE("SliceThickness") AS slice_thickness_num,
        TRY_TO_DOUBLE("Exposure") AS exposure_num
    FROM "IDC"."IDC_V17"."DICOM_ALL"
    WHERE "collection_id" = 'nlst' AND "Modality" = 'CT'
),
slice_diff AS (
    SELECT
        "PatientID",
        MAX(slice_thickness_num) - MIN(slice_thickness_num) AS slice_interval_diff
    FROM base
    WHERE slice_thickness_num IS NOT NULL
    GROUP BY "PatientID"
    HAVING slice_interval_diff IS NOT NULL
),
exposure_diff AS (
    SELECT
        "PatientID",
        MAX(exposure_num) - MIN(exposure_num) AS exposure_diff
    FROM base
    WHERE exposure_num IS NOT NULL
    GROUP BY "PatientID"
    HAVING exposure_diff IS NOT NULL
),
top_slice_patients AS (
    SELECT "PatientID", slice_interval_diff
    FROM slice_diff
    ORDER BY slice_interval_diff DESC
    LIMIT 3
),
top_exposure_patients AS (
    SELECT "PatientID", exposure_diff
    FROM exposure_diff
    ORDER BY exposure_diff DESC
    LIMIT 3
),
series_sizes AS (
    SELECT
        "PatientID",
        "SeriesInstanceUID",
        SUM("instance_size") AS series_size_bytes
    FROM base
    GROUP BY "PatientID", "SeriesInstanceUID"
)
SELECT
    'Top 3 by Slice Interval' AS group_label,
    AVG(series_size_bytes / 1048576) AS avg_series_size_mib
FROM series_sizes ss
INNER JOIN top_slice_patients tsp ON ss."PatientID" = tsp."PatientID"
UNION ALL
SELECT
    'Top 3 by Max Exposure' AS group_label,
    AVG(series_size_bytes / 1048576) AS avg_series_size_mib
FROM series_sizes ss
INNER JOIN top_exposure_patients tep ON ss."PatientID" = tep."PatientID"