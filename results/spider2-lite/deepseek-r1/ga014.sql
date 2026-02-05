WITH session_data AS (
  SELECT DISTINCT
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as session_id,
    traffic_source.source,
    traffic_source.medium,
    traffic_source.name as campaign_name
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20201201' AND '20201231'
    AND (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') IS NOT NULL
),
channels AS (
  SELECT 
    user_pseudo_id,
    session_id,
    CASE
      WHEN source = '(direct)' AND medium IN ('(not set)', '(none)') THEN 'Direct'
      WHEN campaign_name LIKE '%cross-network%' THEN 'Cross-network'
      WHEN (REGEXP_CONTAINS(LOWER(source), r'(alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart)') 
            OR REGEXP_CONTAINS(LOWER(campaign_name), r'^(.*(([^a-df-z]|^)shop|shopping).*)$'))
           AND REGEXP_CONTAINS(LOWER(medium), r'^(.*cp.*|ppc|retargeting|paid.*)$') THEN 'Paid Shopping'
      WHEN REGEXP_CONTAINS(LOWER(source), r'(baidu|bing|duckduckgo|ecosia|google|yahoo|yandex)')
           AND REGEXP_CONTAINS(LOWER(medium), r'^(.*cp.*|ppc|paid.*)$') THEN 'Paid Search'
      WHEN REGEXP_CONTAINS(LOWER(source), r'(badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp)')
           AND REGEXP_CONTAINS(LOWER(medium), r'^(.*cp.*|ppc|retargeting|paid.*)$') THEN 'Paid Social'
      WHEN REGEXP_CONTAINS(LOWER(source), r'(dailymotion|disneyplus|netflix|youtube|vimeo|twitch)')
           AND REGEXP_CONTAINS(LOWER(medium), r'^(.*cp.*|ppc|retargeting|paid.*)$') THEN 'Paid Video'
      WHEN medium IN ('display', 'banner', 'expandable', 'interstitial', 'cpm') THEN 'Display'
      WHEN REGEXP_CONTAINS(LOWER(source), r'(alibaba|amazon|google shopping|shopify|etsy|ebay|stripe|walmart)')
           OR REGEXP_CONTAINS(LOWER(campaign_name), r'^(.*(([^a-df-z]|^)shop|shopping).*)$') THEN 'Organic Shopping'
      WHEN REGEXP_CONTAINS(LOWER(source), r'(badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp)')
           OR medium IN ('social', 'social-network', 'social-media', 'sm', 'social network', 'social media') THEN 'Organic Social'
      WHEN REGEXP_CONTAINS(LOWER(source), r'(dailymotion|disneyplus|netflix|youtube|vimeo|twitch)')
           OR REGEXP_CONTAINS(LOWER(medium), r'^(.*video.*)$') THEN 'Organic Video'
      WHEN REGEXP_CONTAINS(LOWER(source), r'(baidu|bing|duckduckgo|ecosia|google|yahoo|yandex)')
           OR medium = 'organic' THEN 'Organic Search'
      WHEN medium = 'referral' THEN 'Referral'
      WHEN LOWER(source) IN ('email', 'e-mail', 'e_mail', 'e mail') OR LOWER(medium) IN ('email', 'e-mail', 'e_mail', 'e mail') THEN 'Email'
      WHEN medium = 'affiliate' THEN 'Affiliates'
      WHEN medium = 'audio' THEN 'Audio'
      WHEN source = 'sms' OR medium = 'sms' THEN 'SMS'
      WHEN ENDS_WITH(medium, 'push') OR CONTAINS_SUBSTR(medium, 'mobile') OR CONTAINS_SUBSTR(medium, 'notification') THEN 'Mobile Push Notifications'
      ELSE 'Unassigned'
    END as channel
  FROM session_data
)
SELECT channel, COUNT(DISTINCT CONCAT(user_pseudo_id, CAST(session_id AS STRING))) as total_sessions
FROM channels
GROUP BY channel
ORDER BY total_sessions DESC