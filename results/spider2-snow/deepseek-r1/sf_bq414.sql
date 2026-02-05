SELECT DISTINCT 
    "OBJECTS"."object_id",
    "OBJECTS"."title",
    TO_CHAR(TO_TIMESTAMP("OBJECTS"."metadata_date" / 1000000), 'YYYY-MM-DD') AS formatted_metadata_date
FROM "THE_MET"."THE_MET"."OBJECTS"
INNER JOIN (
    SELECT DISTINCT "object_id"
    FROM "THE_MET"."THE_MET"."VISION_API_DATA",
    LATERAL FLATTEN(INPUT => "cropHintsAnnotation":cropHints) AS crop_hints
    WHERE crop_hints.value:confidence > 0.5
) AS "VISION_FILTERED"
ON "OBJECTS"."object_id" = "VISION_FILTERED"."object_id"
WHERE "OBJECTS"."department" = 'The Libraries'
AND LOWER("OBJECTS"."title") LIKE '%book%'