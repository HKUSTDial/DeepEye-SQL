WITH filtered_events AS (
  SELECT
    "USER_PSEUDO_ID",
    "EVENT_TIMESTAMP",
    "EVENT_NAME",
    "EVENT_PARAMS"
  FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210102"
  WHERE "USER_PSEUDO_ID" = '1402138.5184246691'
    AND "EVENT_NAME" = 'page_view'
    AND "EVENT_DATE" = '20210102'
),
event_params_flat AS (
  SELECT
    fe."USER_PSEUDO_ID",
    fe."EVENT_TIMESTAMP",
    fe."EVENT_NAME",
    f.value:key::STRING AS param_key,
    f.value:value.string_value::STRING AS param_value
  FROM filtered_events fe,
  LATERAL FLATTEN(INPUT => fe."EVENT_PARAMS") f
),
event_params_pivot AS (
  SELECT
    "USER_PSEUDO_ID",
    "EVENT_TIMESTAMP",
    MAX(CASE WHEN param_key = 'page_location' THEN param_value END) AS page_location,
    MAX(CASE WHEN param_key = 'page_title' THEN param_value END) AS page_title
  FROM event_params_flat
  GROUP BY "USER_PSEUDO_ID", "EVENT_TIMESTAMP"
),
classified AS (
  SELECT
    *,
    SPLIT_PART(page_location, '/', 7) AS seg4,
    SPLIT_PART(page_location, '/', 8) AS seg5,
    SPLIT_PART(page_location, '/', -1) AS last_seg,
    CASE WHEN page_location IS NOT NULL AND SPLIT_PART(page_location, '/', 8) != '' THEN TRUE ELSE FALSE END AS has_five_segments,
    CASE
      WHEN (LOWER(seg4) LIKE '%accessories%' OR LOWER(seg4) LIKE '%apparel%' OR LOWER(seg4) LIKE '%brands%' OR 
            LOWER(seg4) LIKE '%campus collection%' OR LOWER(seg4) LIKE '%drinkware%' OR LOWER(seg4) LIKE '%electronics%' OR
            LOWER(seg4) LIKE '%google redesign%' OR LOWER(seg4) LIKE '%lifestyle%' OR LOWER(seg4) LIKE '%nest%' OR
            LOWER(seg4) LIKE '%new 2015 logo%' OR LOWER(seg4) LIKE '%notebooks journals%' OR LOWER(seg4) LIKE '%office%' OR
            LOWER(seg4) LIKE '%shop by brand%' OR LOWER(seg4) LIKE '%small goods%' OR LOWER(seg4) LIKE '%stationery%' OR
            LOWER(seg4) LIKE '%wearables%' OR
            LOWER(seg5) LIKE '%accessories%' OR LOWER(seg5) LIKE '%apparel%' OR LOWER(seg5) LIKE '%brands%' OR 
            LOWER(seg5) LIKE '%campus collection%' OR LOWER(seg5) LIKE '%drinkware%' OR LOWER(seg5) LIKE '%electronics%' OR
            LOWER(seg5) LIKE '%google redesign%' OR LOWER(seg5) LIKE '%lifestyle%' OR LOWER(seg5) LIKE '%nest%' OR
            LOWER(seg5) LIKE '%new 2015 logo%' OR LOWER(seg5) LIKE '%notebooks journals%' OR LOWER(seg5) LIKE '%office%' OR
            LOWER(seg5) LIKE '%shop by brand%' OR LOWER(seg5) LIKE '%small goods%' OR LOWER(seg5) LIKE '%stationery%' OR
            LOWER(seg5) LIKE '%wearables%')
      THEN TRUE ELSE FALSE
    END AS has_category,
    CASE WHEN seg4 LIKE '%+%' OR seg5 LIKE '%+%' THEN TRUE ELSE FALSE END AS seg4_or_seg5_has_plus,
    CASE WHEN last_seg LIKE '%+%' THEN TRUE ELSE FALSE END AS last_seg_has_plus
  FROM event_params_pivot
),
determined AS (
  SELECT
    *,
    CASE
      WHEN has_five_segments AND has_category AND last_seg_has_plus THEN 'PDP'
      WHEN has_five_segments AND has_category AND NOT seg4_or_seg5_has_plus THEN 'PLP'
      ELSE NULL
    END AS page_type
  FROM classified
)
SELECT
  CASE
    WHEN page_type = 'PDP' THEN 'PDP'
    WHEN page_type = 'PLP' THEN 'PLP'
    ELSE page_title
  END AS adjusted_page_name
FROM determined
ORDER BY "EVENT_TIMESTAMP"