SELECT
    seg."SegmentedPropertyCategory"['CodeMeaning']::STRING AS "CodeMeaning",
    COUNT(*) AS "frequency"
FROM
    "IDC"."IDC_V17"."DICOM_ALL" AS da
INNER JOIN
    "IDC"."IDC_V17"."SEGMENTATIONS" AS seg
    ON da."SOPInstanceUID" = seg."SOPInstanceUID"
WHERE
    da."access" = 'Public'
    AND da."Modality" = 'SEG'
    AND da."SOPClassUID" = '1.2.840.10008.5.1.4.1.1.66.4'
GROUP BY
    seg."SegmentedPropertyCategory"['CodeMeaning']::STRING
ORDER BY
    "frequency" DESC
LIMIT 5