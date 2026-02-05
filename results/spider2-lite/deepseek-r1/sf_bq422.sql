WITH base AS (
    SELECT 
        "PatientID",
        "SeriesInstanceUID",
        "SOPInstanceUID",
        "instance_size",
        TRY_TO_DOUBLE("SpacingBetweenSlices") AS spacing,
        TRY_TO_DOUBLE("Exposure") AS exposure
    FROM "IDC"."IDC_V17"."DICOM_ALL"
    WHERE "collection_id" = 'nlst' AND "Modality" = 'CT'
),
series_sizes AS (
    SELECT 
        "PatientID",
        "SeriesInstanceUID",
        SUM("instance_size") AS series_size_bytes
    FROM base
    GROUP BY "PatientID", "SeriesInstanceUID"
),
patient_spacings AS (
    SELECT 
        "PatientID",
        MIN(spacing) AS min_spacing,
        MAX(spacing) AS max_spacing,
        MAX(spacing) - MIN(spacing) AS spacing_diff
    FROM base
    WHERE spacing IS NOT NULL
    GROUP BY "PatientID"
),
patient_exposures AS (
    SELECT 
        "PatientID",
        MIN(exposure) AS min_exposure,
        MAX(exposure) AS max_exposure,
        MAX(exposure) - MIN(exposure) AS exposure_diff
    FROM base
    WHERE exposure IS NOT NULL
    GROUP BY "PatientID"
),
ranked_spacing AS (
    SELECT 
        "PatientID",
        spacing_diff,
        ROW_NUMBER() OVER (ORDER BY spacing_diff DESC NULLS LAST, "PatientID") AS rn
    FROM patient_spacings
),
ranked_exposure AS (
    SELECT 
        "PatientID",
        exposure_diff,
        ROW_NUMBER() OVER (ORDER BY exposure_diff DESC NULLS LAST, "PatientID") AS rn
    FROM patient_exposures
),
top_spacing_patients AS (
    SELECT "PatientID" FROM ranked_spacing WHERE rn <= 3
),
top_exposure_patients AS (
    SELECT "PatientID" FROM ranked_exposure WHERE rn <= 3
),
avg_series_spacing AS (
    SELECT 
        'Top 3 by Slice Interval' AS metric_group,
        AVG(series_size_bytes / 1048576) AS avg_series_size_mib
    FROM series_sizes
    INNER JOIN top_spacing_patients ON series_sizes."PatientID" = top_spacing_patients."PatientID"
),
avg_series_exposure AS (
    SELECT 
        'Top 3 by Max Exposure' AS metric_group,
        AVG(series_size_bytes / 1048576) AS avg_series_size_mib
    FROM series_sizes
    INNER JOIN top_exposure_patients ON series_sizes."PatientID" = top_exposure_patients."PatientID"
)
SELECT metric_group, avg_series_size_mib FROM avg_series_spacing
UNION ALL
SELECT metric_group, avg_series_size_mib FROM avg_series_exposure