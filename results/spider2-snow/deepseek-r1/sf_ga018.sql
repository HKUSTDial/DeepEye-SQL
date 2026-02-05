WITH page_views AS (
  SELECT 
    "USER_PSEUDO_ID",
    "EVENT_TIMESTAMP",
    (SELECT "value"."int_value" FROM LATERAL FLATTEN(INPUT => "EVENT_PARAMS") WHERE "key" = 'ga_session_id') as session_id,
    (SELECT "value"."string_value" FROM LATERAL FLATTEN(INPUT => "EVENT_PARAMS") WHERE "key" = 'page_location') as page_location
  FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210102"
  WHERE "EVENT_NAME" = 'page_view'
    AND "page_location" IS NOT NULL
),
classified_pages AS (
  SELECT 
    "USER_PSEUDO_ID",
    "EVENT_TIMESTAMP",
    session_id,
    page_location,
    CASE 
      WHEN ARRAY_SIZE(SPLIT(page_location, '/')) >= 5 
           AND (LOWER(SPLIT(page_location, '/')[3]) LIKE '%accessories%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%apparel%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%brands%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%campus collection%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%drinkware%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%electronics%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%google redesign%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%lifestyle%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%nest%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%new 2015 logo%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%notebooks journals%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%office%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%shop by brand%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%small goods%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%stationery%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%wearables%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%accessories%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%apparel%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%brands%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%campus collection%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%drinkware%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%electronics%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%google redesign%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%lifestyle%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%nest%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%new 2015 logo%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%notebooks journals%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%office%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%shop by brand%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%small goods%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%stationery%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%wearables%')
           AND NOT (SPLIT(page_location, '/')[3] LIKE '%+%' OR SPLIT(page_location, '/')[4] LIKE '%+%')
      THEN 'PLP'
      WHEN ARRAY_SIZE(SPLIT(page_location, '/')) >= 5 
           AND (LOWER(SPLIT(page_location, '/')[3]) LIKE '%accessories%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%apparel%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%brands%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%campus collection%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%drinkware%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%electronics%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%google redesign%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%lifestyle%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%nest%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%new 2015 logo%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%notebooks journals%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%office%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%shop by brand%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%small goods%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%stationery%' OR
                LOWER(SPLIT(page_location, '/')[3]) LIKE '%wearables%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%accessories%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%apparel%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%brands%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%campus collection%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%drinkware%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%electronics%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%google redesign%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%lifestyle%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%nest%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%new 2015 logo%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%notebooks journals%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%office%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%shop by brand%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%small goods%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%stationery%' OR
                LOWER(SPLIT(page_location, '/')[4]) LIKE '%wearables%')
           AND SPLIT(page_location, '/')[ARRAY_SIZE(SPLIT(page_location, '/')) - 1] LIKE '%+%'
      THEN 'PDP'
      ELSE 'other'
    END as page_type
  FROM page_views
  WHERE session_id IS NOT NULL
),
session_plp_pdp AS (
  SELECT 
    "USER_PSEUDO_ID",
    session_id,
    MAX(CASE WHEN page_type = 'PLP' THEN 1 ELSE 0 END) as has_plp,
    MAX(CASE WHEN page_type = 'PDP' THEN 1 ELSE 0 END) as has_pdp,
    MIN(CASE WHEN page_type = 'PLP' THEN "EVENT_TIMESTAMP" END) as first_plp_time,
    MIN(CASE WHEN page_type = 'PDP' THEN "EVENT_TIMESTAMP" END) as first_pdp_time
  FROM classified_pages
  GROUP BY "USER_PSEUDO_ID", session_id
),
session_transitions AS (
  SELECT 
    "USER_PSEUDO_ID",
    session_id,
    has_plp,
    has_pdp,
    CASE 
      WHEN has_plp = 1 AND has_pdp = 1 AND first_pdp_time > first_plp_time 
      THEN 1 
      ELSE 0 
    END as has_plp_to_pdp
  FROM session_plp_pdp
)
SELECT 
  COUNT(DISTINCT CASE WHEN has_plp = 1 THEN CONCAT("USER_PSEUDO_ID", '|', session_id) END) as sessions_with_plp,
  COUNT(DISTINCT CASE WHEN has_plp_to_pdp = 1 THEN CONCAT("USER_PSEUDO_ID", '|', session_id) END) as sessions_with_plp_to_pdp,
  ROUND(
    COUNT(DISTINCT CASE WHEN has_plp_to_pdp = 1 THEN CONCAT("USER_PSEUDO_ID", '|', session_id) END) * 100.0 / 
    NULLIF(COUNT(DISTINCT CASE WHEN has_plp = 1 THEN CONCAT("USER_PSEUDO_ID", '|', session_id) END), 0),
    2
  ) as percentage_plp_to_pdp
FROM session_transitions