SELECT "SegmentedPropertyCategory":"CodeMeaning"::STRING AS "category", COUNT(*) AS "frequency"
FROM "IDC"."IDC_V17"."DICOM_ALL" AS "d"
INNER JOIN "IDC"."IDC_V17"."SEGMENTATIONS" AS "s"
ON "d"."SOPInstanceUID" = "s"."SOPInstanceUID"
WHERE "d"."access" = 'Public'
AND "d"."Modality" = 'SEG'
AND "d"."SOPClassUID" = '1.2.840.10008.5.1.4.1.1.66.4'
GROUP BY "SegmentedPropertyCategory":"CodeMeaning"::STRING
ORDER BY "frequency" DESC
LIMIT 5