WITH events AS (
  SELECT 
    event_timestamp,
    (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = 'page_title') AS page_title,
    (SELECT ep.value.string_value FROM UNNEST(event_params) AS ep WHERE ep.key = 'page_location') AS page_location
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210128`
  WHERE event_name = 'page_view'
    AND user_pseudo_id = '1362228.4966015575'
    AND event_date = '20210128'
),
classified_events AS (
  SELECT 
    event_timestamp,
    page_title,
    page_location,
    SPLIT(page_location, '/') AS segments,
    ARRAY_LENGTH(SPLIT(page_location, '/')) AS num_segments,
    SPLIT(page_location, '/')[OFFSET(3)] AS segment4,
    SPLIT(page_location, '/')[SAFE_OFFSET(4)] AS segment5,
    SPLIT(page_location, '/')[OFFSET(ARRAY_LENGTH(SPLIT(page_location, '/')) - 1)] AS last_segment
  FROM events
  WHERE page_location IS NOT NULL
),
with_category_match AS (
  SELECT 
    *,
    ['accessories', 'apparel', 'brands', 'campus collection', 'drinkware', 'electronics', 'google redesign', 'lifestyle', 'nest', 'new 2015 logo', 'notebooks journals', 'office', 'shop by brand', 'small goods', 'stationery', 'wearables'] AS category_list,
    REPLACE(LOWER(segment4), '+', ' ') AS norm_seg4,
    REPLACE(LOWER(segment5), '+', ' ') AS norm_seg5
  FROM classified_events
),
with_type AS (
  SELECT 
    event_timestamp,
    page_title,
    EXISTS (SELECT 1 FROM UNNEST(category_list) AS cat 
            WHERE norm_seg4 LIKE CONCAT('%', cat, '%') 
               OR (segment5 IS NOT NULL AND norm_seg5 LIKE CONCAT('%', cat, '%'))
           ) AS has_category_match,
    (num_segments >= 5) AND (last_segment LIKE '%+%') AS has_pdp_structure,
    (num_segments >= 5) AND (segment4 NOT LIKE '%+%') AND (segment5 IS NULL OR segment5 NOT LIKE '%+%') AS has_plp_structure
  FROM with_category_match
),
with_label AS (
  SELECT 
    event_timestamp,
    CASE 
      WHEN has_category_match AND has_pdp_structure THEN 'PDP'
      WHEN has_category_match AND has_plp_structure THEN 'PLP'
      ELSE page_title
    END AS step_label
  FROM with_type
),
deduplicated AS (
  SELECT 
    event_timestamp,
    step_label,
    LAG(step_label) OVER (ORDER BY event_timestamp) AS prev_step_label
  FROM with_label
)
SELECT STRING_AGG(step_label, ' >> ' ORDER BY event_timestamp) AS navigation_flow
FROM deduplicated
WHERE prev_step_label IS NULL OR step_label != prev_step_label