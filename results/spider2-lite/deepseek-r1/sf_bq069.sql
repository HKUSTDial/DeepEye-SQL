WITH filtered_instances AS (
  SELECT 
    "SeriesInstanceUID",
    "StudyInstanceUID",
    "PatientID",
    "SeriesNumber",
    "SOPInstanceUID",
    "SliceThickness",
    "Exposure",
    "instance_size",
    "Rows",
    "Columns",
    TO_VARCHAR("ImageOrientationPatient") AS iop_str,
    TO_VARCHAR("PixelSpacing") AS pixel_spacing_str,
    TO_VARCHAR("ImagePositionPatient") AS ipp_str,
    TRY_TO_DOUBLE(TO_VARCHAR("ImageOrientationPatient"[0])) AS iop_x1,
    TRY_TO_DOUBLE(TO_VARCHAR("ImageOrientationPatient"[1])) AS iop_y1,
    TRY_TO_DOUBLE(TO_VARCHAR("ImageOrientationPatient"[2])) AS iop_z1,
    TRY_TO_DOUBLE(TO_VARCHAR("ImageOrientationPatient"[3])) AS iop_x2,
    TRY_TO_DOUBLE(TO_VARCHAR("ImageOrientationPatient"[4])) AS iop_y2,
    TRY_TO_DOUBLE(TO_VARCHAR("ImageOrientationPatient"[5])) AS iop_z2,
    TRY_TO_DOUBLE(TO_VARCHAR("ImagePositionPatient"[0])) AS ipp_x,
    TRY_TO_DOUBLE(TO_VARCHAR("ImagePositionPatient"[1])) AS ipp_y,
    TRY_TO_DOUBLE(TO_VARCHAR("ImagePositionPatient"[2])) AS ipp_z
  FROM "IDC"."IDC_V17"."DICOM_ALL"
  WHERE "Modality" = 'CT'
    AND "collection_id" != 'nlst'
    AND NOT ARRAY_CONTAINS('LOCALIZER'::VARIANT, "ImageType")
    AND "TransferSyntaxUID" NOT IN ('1.2.840.10008.1.2.4.70', '1.2.840.10008.1.2.4.51')
),
series_metrics AS (
  SELECT 
    "SeriesInstanceUID",
    ANY_VALUE("StudyInstanceUID") AS "StudyInstanceUID",
    ANY_VALUE("PatientID") AS "PatientID",
    ANY_VALUE("SeriesNumber") AS "SeriesNumber",
    MAX(iop_x1 * iop_y2 - iop_y1 * iop_x2) AS max_dot_product,
    COUNT(DISTINCT "SOPInstanceUID") AS sop_instance_count,
    COUNT(DISTINCT "SliceThickness") AS distinct_slice_thickness,
    COUNT(DISTINCT "Exposure") AS distinct_exposure,
    MAX(TRY_TO_DOUBLE("Exposure")) AS max_exposure,
    MIN(TRY_TO_DOUBLE("Exposure")) AS min_exposure,
    SUM("instance_size") AS total_size_bytes
  FROM filtered_instances
  GROUP BY "SeriesInstanceUID"
  HAVING 
    COUNT(DISTINCT iop_str) = 1 AND
    COUNT(DISTINCT pixel_spacing_str) = 1 AND
    MAX(ABS((iop_x1 * iop_y2 - iop_y1 * iop_x2) - 1)) <= 0.01 AND
    COUNT(DISTINCT "SOPInstanceUID") = COUNT(DISTINCT ipp_str) AND
    COUNT(DISTINCT ipp_x || ',' || ipp_y) = 1 AND
    COUNT(DISTINCT "Rows") = 1 AND
    COUNT(DISTINCT "Columns") = 1
),
slice_intervals AS (
  SELECT 
    f."SeriesInstanceUID",
    f.ipp_z,
    LAG(f.ipp_z) OVER (PARTITION BY f."SeriesInstanceUID" ORDER BY f.ipp_z) AS prev_ipp_z
  FROM filtered_instances f
  INNER JOIN series_metrics s ON f."SeriesInstanceUID" = s."SeriesInstanceUID"
),
interval_metrics AS (
  SELECT
    "SeriesInstanceUID",
    MAX(ipp_z - prev_ipp_z) AS max_slice_interval,
    MIN(ipp_z - prev_ipp_z) AS min_slice_interval
  FROM slice_intervals
  WHERE prev_ipp_z IS NOT NULL
  GROUP BY "SeriesInstanceUID"
)
SELECT
  s."SeriesInstanceUID",
  s."SeriesNumber",
  s."StudyInstanceUID",
  s."PatientID",
  s.max_dot_product,
  s.sop_instance_count,
  s.distinct_slice_thickness,
  i.max_slice_interval,
  i.min_slice_interval,
  i.max_slice_interval - i.min_slice_interval AS slice_interval_tolerance,
  s.distinct_exposure,
  s.max_exposure,
  s.min_exposure,
  s.max_exposure - s.min_exposure AS exposure_range,
  s.total_size_bytes / (1024 * 1024) AS total_size_mb
FROM series_metrics s
JOIN interval_metrics i ON s."SeriesInstanceUID" = i."SeriesInstanceUID"
ORDER BY 
  slice_interval_tolerance DESC,
  exposure_range DESC,
  s."SeriesInstanceUID" DESC