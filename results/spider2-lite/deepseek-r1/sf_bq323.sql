WITH distinct_series AS (
    SELECT DISTINCT
        "SeriesInstanceUID",
        TRY_TO_DOUBLE("RepetitionTime") AS "RepetitionTime",
        TRY_TO_DOUBLE("EchoTime") AS "EchoTime",
        TRY_TO_DOUBLE("SliceThickness") AS "SliceThickness"
    FROM "IDC"."IDC_V17"."DICOM_ALL"
    WHERE "collection_id" = 'prostatex'
        AND "Modality" = 'MR'
        AND (CONTAINS("SeriesDescription", 't2_tse_tra') OR CONTAINS("SeriesDescription", 'ADC'))
)
SELECT
    AVG("RepetitionTime") AS "avg_rep_time",
    AVG("EchoTime") AS "avg_echo_time",
    AVG("SliceThickness") AS "avg_slice_thickness",
    AVG("RepetitionTime") + AVG("EchoTime") + AVG("SliceThickness") AS "combined_overall_avg"
FROM distinct_series