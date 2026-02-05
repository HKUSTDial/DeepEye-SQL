WITH CAT_IMAGE_IDS AS (
  SELECT DISTINCT "image_id" 
  FROM "OPEN_IMAGES"."OPEN_IMAGES"."LABELS" 
  WHERE "label_name" = '/m/01yrx' AND "confidence" = 1
),
ALL_CAT_IMAGE_IDS AS (
  SELECT DISTINCT "image_id" 
  FROM "OPEN_IMAGES"."OPEN_IMAGES"."LABELS" 
  WHERE "label_name" = '/m/01yrx'
)
SELECT 
  (SELECT COUNT(DISTINCT "original_url") FROM "OPEN_IMAGES"."OPEN_IMAGES"."IMAGES" WHERE "image_id" IN (SELECT "image_id" FROM CAT_IMAGE_IDS)) AS cat_url_count,
  (SELECT COUNT(DISTINCT "original_url") FROM "OPEN_IMAGES"."OPEN_IMAGES"."IMAGES" WHERE "image_id" NOT IN (SELECT "image_id" FROM ALL_CAT_IMAGE_IDS)) AS other_url_count