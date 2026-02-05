WITH december_sessions AS (
  SELECT 
    e."USER_PSEUDO_ID",
    e."EVENT_TIMESTAMP",
    MAX(CASE WHEN ep.value:key::STRING = 'ga_session_id' THEN ep.value:value.int_value END) AS ga_session_id,
    MAX(CASE WHEN ep.value:key::STRING = 'source' THEN ep.value:value.string_value::STRING END) AS source,
    MAX(CASE WHEN ep.value:key::STRING = 'medium' THEN ep.value:value.string_value::STRING END) AS medium,
    MAX(CASE WHEN ep.value:key::STRING = 'campaign' THEN ep.value:value.string_value::STRING END) AS campaign_name
  FROM (
    SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201201"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201202"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201203"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201204"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201205"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201206"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201207"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201208"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201209"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201210"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201211"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201212"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201213"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201214"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201215"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201216"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201217"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201218"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201219"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201220"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201221"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201222"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201223"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201224"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201225"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201226"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201227"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201228"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201229"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201230"
    UNION ALL SELECT * FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20201231"
  ) e,
  LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") ep
  WHERE e."EVENT_NAME" = 'session_start'
  GROUP BY e."USER_PSEUDO_ID", e."EVENT_TIMESTAMP"
),
channel_groups AS (
  SELECT 
    ds."USER_PSEUDO_ID",
    ds.ga_session_id,
    ds.source,
    ds.medium,
    ds.campaign_name,
    CASE 
      WHEN ds.source = '(direct)' AND ds.medium IN ('(not set)', '(none)') THEN 'Direct'
      WHEN ds.campaign_name LIKE '%cross-network%' THEN 'Cross-network'
      WHEN (LOWER(ds.source) IN ('alibaba','amazon','google shopping','shopify','etsy','ebay','stripe','walmart') 
            OR REGEXP_LIKE(ds.campaign_name, '^(.*(([^a-df-z]|^)shop|shopping).*)$', 'i'))
           AND REGEXP_LIKE(ds.medium, '^(.*cp.*|ppc|retargeting|paid.*)$', 'i') THEN 'Paid Shopping'
      WHEN LOWER(ds.source) IN ('baidu','bing','duckduckgo','ecosia','google','yahoo','yandex')
           AND REGEXP_LIKE(ds.medium, '^(.*cp.*|ppc|paid.*)$', 'i') THEN 'Paid Search'
      WHEN REGEXP_LIKE(LOWER(ds.source), '(badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp)')
           AND REGEXP_LIKE(ds.medium, '^(.*cp.*|ppc|retargeting|paid.*)$', 'i') THEN 'Paid Social'
      WHEN LOWER(ds.source) IN ('dailymotion','disneyplus','netflix','youtube','vimeo','twitch','vimeo','youtube')
           AND REGEXP_LIKE(ds.medium, '^(.*cp.*|ppc|retargeting|paid.*)$', 'i') THEN 'Paid Video'
      WHEN ds.medium IN ('display', 'banner', 'expandable', 'interstitial', 'cpm') THEN 'Display'
      WHEN LOWER(ds.source) IN ('alibaba','amazon','google shopping','shopify','etsy','ebay','stripe','walmart')
           OR REGEXP_LIKE(ds.campaign_name, '^(.*(([^a-df-z]|^)shop|shopping).*)$', 'i') THEN 'Organic Shopping'
      WHEN REGEXP_LIKE(LOWER(ds.source), '(badoo|facebook|fb|instagram|linkedin|pinterest|tiktok|twitter|whatsapp)')
           OR ds.medium IN ('social', 'social-network', 'social-media', 'sm', 'social network', 'social media') THEN 'Organic Social'
      WHEN LOWER(ds.source) IN ('dailymotion','disneyplus','netflix','youtube','vimeo','twitch','vimeo','youtube')
           OR REGEXP_LIKE(ds.medium, '^(.*video.*)$', 'i') THEN 'Organic Video'
      WHEN LOWER(ds.source) IN ('baidu','bing','duckduckgo','ecosia','google','yahoo','yandex')
           OR ds.medium = 'organic' THEN 'Organic Search'
      WHEN ds.medium = 'referral' THEN 'Referral'
      WHEN REGEXP_LIKE(LOWER(ds.source), 'email|e-mail|e_mail|e[ _-]?mail') OR REGEXP_LIKE(LOWER(ds.medium), 'email|e-mail|e_mail|e[ _-]?mail') THEN 'Email'
      WHEN ds.medium = 'affiliate' THEN 'Affiliates'
      WHEN ds.medium = 'audio' THEN 'Audio'
      WHEN LOWER(ds.source) = 'sms' OR LOWER(ds.medium) = 'sms' THEN 'SMS'
      WHEN ds.medium LIKE '%push' OR ds.medium LIKE '%mobile%' OR ds.medium LIKE '%notification%' THEN 'Mobile Push Notifications'
      ELSE 'Unassigned'
    END AS channel_group
  FROM december_sessions ds
  WHERE ds.ga_session_id IS NOT NULL
)
SELECT 
  cg.channel_group,
  COUNT(DISTINCT CONCAT(cg."USER_PSEUDO_ID", '_', cg.ga_session_id)) AS session_count
FROM channel_groups cg
GROUP BY cg.channel_group
ORDER BY session_count DESC;