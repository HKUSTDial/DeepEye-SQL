SELECT o."title", o."artist_display_name", o."medium", i."original_image_url"
FROM "THE_MET"."THE_MET"."OBJECTS" o
INNER JOIN "THE_MET"."THE_MET"."IMAGES" i ON o."object_id" = i."object_id"
WHERE o."object_name" LIKE '%Photograph%'
  AND o."department" = 'Photographs'
  AND (o."artist_display_name" IS NOT NULL AND TRIM(o."artist_display_name") != '' AND LOWER(TRIM(o."artist_display_name")) != 'unknown')
  AND o."object_end_date" <= 1839