WITH page_views AS (
  SELECT 
    param.value:string_value::STRING AS page_location
  FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210102",
  LATERAL FLATTEN(INPUT => "EVENT_PARAMS") AS param
  WHERE "EVENT_NAME" = 'page_view'
    AND "EVENT_DATE" = '20210102'
    AND param.key = 'page_location'
    AND param.value:string_value IS NOT NULL
),
total AS (
  SELECT COUNT(*) AS total_count FROM page_views
),
pdp AS (
  SELECT COUNT(*) AS pdp_count
  FROM page_views
  WHERE ARRAY_SIZE(SPLIT(page_location, '/')) >= 5
    AND SPLIT(page_location, '/')[ARRAY_SIZE(SPLIT(page_location, '/'))] LIKE '%+%'
    AND (
      LOWER(SPLIT(page_location, '/')[4]) LIKE '%accessories%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%apparel%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%brands%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%campus+collection%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%drinkware%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%electronics%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%google+redesign%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%lifestyle%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%nest%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%new+2015+logo%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%notebooks+journals%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%office%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%shop+by+brand%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%small+goods%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%stationery%'
      OR LOWER(SPLIT(page_location, '/')[4]) LIKE '%wearables%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%accessories%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%apparel%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%brands%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%campus+collection%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%drinkware%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%electronics%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%google+redesign%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%lifestyle%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%nest%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%new+2015+logo%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%notebooks+journals%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%office%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%shop+by+brand%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%small+goods%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%stationery%'
      OR LOWER(SPLIT(page_location, '/')[5]) LIKE '%wearables%'
    )
)
SELECT 
  COALESCE((pdp.pdp_count * 100.0 / NULLIF(total.total_count, 0)), 0) AS pdp_percentage
FROM total CROSS JOIN pdp