WITH raw_sessions AS (
  SELECT 
    user_pseudo_id,
    event_timestamp,
    traffic_source.source AS source,
    traffic_source.medium AS medium,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign') AS campaign
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_202012*`
  WHERE _TABLE_SUFFIX BETWEEN '01' AND '31'
    AND event_name = 'session_start'
),
sessions_with_channel AS (
  SELECT 
    user_pseudo_id,
    event_timestamp,
    source,
    medium,
    campaign,
    CASE 
      WHEN source = '(direct)' AND medium IN ('(not set)', '(none)') THEN 'Direct'
      WHEN campaign LIKE '%cross-network%' THEN 'Cross-network'
      WHEN (
        (source IN ('alibaba', 'amazon', 'google shopping', 'shopify', 'etsy', 'ebay', 'stripe', 'walmart')
         OR REGEXP_CONTAINS(campaign, r'^(.*(([^a-df-z]|^)shop|shopping).*)$'))
        AND REGEXP_CONTAINS(medium, r'^(.*cp.*|ppc|retargeting|paid.*)$')
      ) THEN 'Paid Shopping'
      WHEN (
        source IN ('baidu', 'bing', 'duckduckgo', 'ecosia', 'google', 'yahoo', 'yandex')
        AND REGEXP_CONTAINS(medium, r'^(.*cp.*|ppc|paid.*)$')
      ) THEN 'Paid Search'
      WHEN (
        REGEXP_CONTAINS(LOWER(source), r'^(badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp)$')
        AND REGEXP_CONTAINS(medium, r'^(.*cp.*|ppc|retargeting|paid.*)$')
      ) THEN 'Paid Social'
      WHEN (
        source IN ('dailymotion', 'disneyplus', 'netflix', 'youtube', 'vimeo', 'twitch', 'vimeo', 'youtube')
        AND REGEXP_CONTAINS(medium, r'^(.*cp.*|ppc|retargeting|paid.*)$')
      ) THEN 'Paid Video'
      WHEN medium IN ('display', 'banner', 'expandable', 'interstitial', 'cpm') THEN 'Display'
      WHEN (
        source IN ('alibaba', 'amazon', 'google shopping', 'shopify', 'etsy', 'ebay', 'stripe', 'walmart')
        OR REGEXP_CONTAINS(campaign, r'^(.*(([^a-df-z]|^)shop|shopping).*)$')
      ) THEN 'Organic Shopping'
      WHEN (
        REGEXP_CONTAINS(LOWER(source), r'^(badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp)$')
        OR medium IN ('social', 'social-network', 'social-media', 'sm', 'social network', 'social media')
      ) THEN 'Organic Social'
      WHEN (
        source IN ('dailymotion', 'disneyplus', 'netflix', 'youtube', 'vimeo', 'twitch', 'vimeo', 'youtube')
        OR REGEXP_CONTAINS(medium, r'^(.*video.*)$')
      ) THEN 'Organic Video'
      WHEN (
        source IN ('baidu', 'bing', 'duckduckgo', 'ecosia', 'google', 'yahoo', 'yandex')
        OR medium = 'organic'
      ) THEN 'Organic Search'
      WHEN medium = 'referral' THEN 'Referral'
      WHEN (
        LOWER(source) IN ('email', 'e-mail', 'e_mail', 'e mail')
        OR LOWER(medium) IN ('email', 'e-mail', 'e_mail', 'e mail')
      ) THEN 'Email'
      WHEN medium = 'affiliate' THEN 'Affiliates'
      WHEN medium = 'audio' THEN 'Audio'
      WHEN source = 'sms' OR medium = 'sms' THEN 'SMS'
      WHEN medium LIKE '%push' OR medium LIKE '%mobile%' OR medium LIKE '%notification%' THEN 'Mobile Push Notifications'
      ELSE 'Unassigned'
    END AS channel
  FROM raw_sessions
),
channel_counts AS (
  SELECT 
    channel,
    COUNT(*) AS session_count
  FROM sessions_with_channel
  GROUP BY channel
),
ranked_channels AS (
  SELECT 
    channel,
    session_count,
    ROW_NUMBER() OVER (ORDER BY session_count DESC, channel) AS rank
  FROM channel_counts
)
SELECT channel, session_count
FROM ranked_channels
WHERE rank = 4