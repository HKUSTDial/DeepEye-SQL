WITH ct_instances AS (
  SELECT
    "SeriesInstanceUID",
    "SeriesNumber",
    "PatientID",
    "instance_size",
    "Exposure",
    "Rows",
    "Columns",
    "PixelSpacing",
    "ImageOrientationPatient",
    "ImagePositionPatient",
    "TransferSyntaxUID",
    "ImageType",
    "ImagePositionPatient"[0]::FLOAT AS x,
    "ImagePositionPatient"[1]::FLOAT AS y,
    "ImagePositionPatient"[2]::FLOAT AS z,
    "ImageOrientationPatient"[0]::FLOAT AS a1,
    "ImageOrientationPatient"[1]::FLOAT AS a2,
    "ImageOrientationPatient"[2]::FLOAT AS a3,
    "ImageOrientationPatient"[3]::FLOAT AS b1,
    "ImageOrientationPatient"[4]::FLOAT AS b2,
    "ImageOrientationPatient"[5]::FLOAT AS b3,
    CASE WHEN "TransferSyntaxUID" IN ('1.2.840.10008.1.2.4.70', '1.2.840.10008.1.2.4.51') THEN 1 ELSE 0 END AS forbidden_transfer_flag,
    CASE WHEN ARRAY_CONTAINS('LOCALIZER'::VARIANT, "ImageType") THEN 1 ELSE 0 END AS localizer_flag
  FROM "IDC"."IDC_V17"."DICOM_ALL"
  WHERE "Modality" = 'CT' AND "collection_id" != 'nlst'
),
instance_ordered AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY "SeriesInstanceUID" ORDER BY z) AS rn,
    LAG(z) OVER (PARTITION BY "SeriesInstanceUID" ORDER BY z) AS prev_z
  FROM ct_instances
),
differences AS (
  SELECT
    "SeriesInstanceUID",
    z - prev_z AS diff
  FROM instance_ordered
  WHERE prev_z IS NOT NULL
),
series_diffs AS (
  SELECT
    "SeriesInstanceUID",
    COUNT(DISTINCT diff) AS distinct_diffs
  FROM differences
  GROUP BY "SeriesInstanceUID"
),
series_aggregates AS (
  SELECT
    "SeriesInstanceUID",
    MIN("SeriesNumber") AS "SeriesNumber",
    MIN("PatientID") AS "PatientID",
    SUM("instance_size") AS total_bytes,
    COUNT(*) AS num_images,
    COUNT(DISTINCT "Exposure") AS distinct_exposure,
    COUNT(DISTINCT "Rows") AS distinct_rows,
    COUNT(DISTINCT "Columns") AS distinct_columns,
    COUNT(DISTINCT "PixelSpacing"::STRING) AS distinct_pixel_spacing,
    COUNT(DISTINCT "ImageOrientationPatient"::STRING) AS distinct_orientation,
    COUNT(DISTINCT OBJECT_CONSTRUCT('x', x, 'y', y)) AS distinct_xy,
    COUNT(DISTINCT z) AS distinct_z,
    MAX(forbidden_transfer_flag) AS has_forbidden_transfer,
    MAX(localizer_flag) AS has_localizer,
    MIN(a1) AS a1,
    MIN(a2) AS a2,
    MIN(b1) AS b1,
    MIN(b2) AS b2
  FROM ct_instances
  GROUP BY "SeriesInstanceUID"
)
SELECT
  sa."SeriesInstanceUID",
  sa."SeriesNumber",
  sa."PatientID",
  sa.total_bytes / (1024 * 1024) AS series_size_mib
FROM series_aggregates sa
LEFT JOIN series_diffs sd ON sa."SeriesInstanceUID" = sd."SeriesInstanceUID"
WHERE sa.has_forbidden_transfer = 0
  AND sa.has_localizer = 0
  AND sa.distinct_exposure = 1
  AND sa.distinct_rows = 1
  AND sa.distinct_columns = 1
  AND sa.distinct_pixel_spacing = 1
  AND sa.distinct_orientation = 1
  AND sa.distinct_xy = 1
  AND sa.distinct_z = sa.num_images
  AND (sd.distinct_diffs = 1 OR sd.distinct_diffs IS NULL)
  AND ABS(sa.a1 * sa.b2 - sa.a2 * sa.b1) BETWEEN 0.99 AND 1.01
ORDER BY series_size_mib DESC
LIMIT 5