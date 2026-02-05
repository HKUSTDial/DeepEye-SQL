WITH series_data AS (
  SELECT 
    "SeriesInstanceUID",
    MIN(TRY_TO_DOUBLE("RepetitionTime")) AS RepetitionTime_numeric,
    MIN(TRY_TO_DOUBLE("EchoTime")) AS EchoTime_numeric,
    MIN(TRY_TO_DOUBLE("SliceThickness")) AS SliceThickness_numeric
  FROM "IDC"."IDC_V17"."DICOM_ALL"
  WHERE "collection_id" = 'prostatex'
    AND "Modality" = 'MR'
    AND (
      CONTAINS("SeriesDescription", 't2_tse_tra')
      OR CONTAINS("SeriesDescription", 'ADC')
    )
  GROUP BY "SeriesInstanceUID"
),
averages AS (
  SELECT 
    AVG(RepetitionTime_numeric) AS avg_RepetitionTime,
    AVG(EchoTime_numeric) AS avg_EchoTime,
    AVG(SliceThickness_numeric) AS avg_SliceThickness
  FROM series_data
)
SELECT 
  avg_RepetitionTime,
  avg_EchoTime,
  avg_SliceThickness,
  avg_RepetitionTime + avg_EchoTime + avg_SliceThickness AS combined_overall_average
FROM averages