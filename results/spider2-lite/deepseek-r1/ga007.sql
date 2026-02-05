WITH page_views AS (
  SELECT 
    event_name,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') as page_location
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_20210102`
  WHERE event_name = 'page_view'
),
classified_views AS (
  SELECT 
    page_location,
    CASE 
      WHEN 
        ARRAY_LENGTH(SPLIT(page_location, '/')) >= 5 AND
        CONTAINS_SUBSTR(SPLIT(page_location, '/')[SAFE_OFFSET(ARRAY_LENGTH(SPLIT(page_location, '/')) - 1)], '+') AND
        (
          LOWER(SPLIT(page_location, '/')[SAFE_OFFSET(3)]) IN ('accessories', 'apparel', 'brands', 'campus+collection', 'drinkware', 'electronics', 'google+redesign', 'lifestyle', 'nest', 'new+2015+logo', 'notebooks+journals', 'office', 'shop+by+brand', 'small+goods', 'stationery', 'wearables') OR
          LOWER(SPLIT(page_location, '/')[SAFE_OFFSET(4)]) IN ('accessories', 'apparel', 'brands', 'campus+collection', 'drinkware', 'electronics', 'google+redesign', 'lifestyle', 'nest', 'new+2015+logo', 'notebooks+journals', 'office', 'shop+by+brand', 'small+goods', 'stationery', 'wearables')
        )
      THEN 1
      ELSE 0
    END as is_pdp
  FROM page_views
  WHERE page_location IS NOT NULL
)
SELECT 
  COUNT(*) as total_page_views,
  SUM(is_pdp) as pdp_page_views,
  ROUND(100.0 * SUM(is_pdp) / COUNT(*), 2) as pdp_percentage
FROM classified_views