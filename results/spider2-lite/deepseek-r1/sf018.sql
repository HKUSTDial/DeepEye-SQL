WITH send_events AS (
  SELECT 
    "APP_GROUP_ID",
    "CAMPAIGN_ID", 
    "USER_ID",
    "MESSAGE_VARIATION_ID",
    "PLATFORM",
    "AD_TRACKING_ENABLED",
    NULL AS "CARRIER",
    NULL AS "BROWSER",
    NULL AS "DEVICE_MODEL",
    COUNT(*) AS "push_notification_sends",
    COUNT(DISTINCT "USER_ID") AS "unique_push_notification_sends",
    0 AS "push_notification_bounced",
    0 AS "unique_push_notification_bounced",
    0 AS "push_notification_open",
    0 AS "unique_push_notification_opened",
    0 AS "push_notification_influenced_open",
    0 AS "unique_push_notification_influenced_open"
  FROM "BRAZE_USER_EVENT_DEMO_DATASET"."PUBLIC"."USERS_MESSAGES_PUSHNOTIFICATION_SEND_VIEW"
  WHERE "TIME" >= 1685606400 AND "TIME" < 1685610000
  GROUP BY 1,2,3,4,5,6
),
bounce_events AS (
  SELECT 
    "APP_GROUP_ID",
    "CAMPAIGN_ID",
    "USER_ID",
    "MESSAGE_VARIATION_ID",
    "PLATFORM",
    "AD_TRACKING_ENABLED",
    NULL AS "CARRIER",
    NULL AS "BROWSER",
    NULL AS "DEVICE_MODEL",
    0 AS "push_notification_sends",
    0 AS "unique_push_notification_sends",
    COUNT(*) AS "push_notification_bounced",
    COUNT(DISTINCT "USER_ID") AS "unique_push_notification_bounced",
    0 AS "push_notification_open",
    0 AS "unique_push_notification_opened",
    0 AS "push_notification_influenced_open",
    0 AS "unique_push_notification_influenced_open"
  FROM "BRAZE_USER_EVENT_DEMO_DATASET"."PUBLIC"."USERS_MESSAGES_PUSHNOTIFICATION_BOUNCE_VIEW"
  WHERE "TIME" >= 1685606400 AND "TIME" < 1685610000
  GROUP BY 1,2,3,4,5,6
),
open_events AS (
  SELECT 
    "APP_GROUP_ID",
    "CAMPAIGN_ID",
    "USER_ID",
    "MESSAGE_VARIATION_ID",
    "PLATFORM",
    "AD_TRACKING_ENABLED",
    "CARRIER",
    "BROWSER",
    "DEVICE_MODEL",
    0 AS "push_notification_sends",
    0 AS "unique_push_notification_sends",
    0 AS "push_notification_bounced",
    0 AS "unique_push_notification_bounced",
    COUNT(*) AS "push_notification_open",
    COUNT(DISTINCT "USER_ID") AS "unique_push_notification_opened",
    0 AS "push_notification_influenced_open",
    0 AS "unique_push_notification_influenced_open"
  FROM "BRAZE_USER_EVENT_DEMO_DATASET"."PUBLIC"."USERS_MESSAGES_PUSHNOTIFICATION_OPEN_VIEW"
  WHERE "TIME" >= 1685606400 AND "TIME" < 1685610000
  GROUP BY 1,2,3,4,5,6,7,8,9
),
influenced_open_events AS (
  SELECT 
    "APP_GROUP_ID",
    "CAMPAIGN_ID",
    "USER_ID",
    "MESSAGE_VARIATION_ID",
    "PLATFORM",
    NULL AS "AD_TRACKING_ENABLED",
    "CARRIER",
    "BROWSER",
    "DEVICE_MODEL",
    0 AS "push_notification_sends",
    0 AS "unique_push_notification_sends",
    0 AS "push_notification_bounced",
    0 AS "unique_push_notification_bounced",
    0 AS "push_notification_open",
    0 AS "unique_push_notification_opened",
    COUNT(*) AS "push_notification_influenced_open",
    COUNT(DISTINCT "USER_ID") AS "unique_push_notification_influenced_open"
  FROM "BRAZE_USER_EVENT_DEMO_DATASET"."PUBLIC"."USERS_MESSAGES_PUSHNOTIFICATION_INFLUENCEDOPEN_VIEW"
  WHERE "TIME" >= 1685606400 AND "TIME" < 1685610000
  GROUP BY 1,2,3,4,5,7,8,9
),
all_events AS (
  SELECT * FROM send_events
  UNION ALL
  SELECT * FROM bounce_events
  UNION ALL
  SELECT * FROM open_events
  UNION ALL
  SELECT * FROM influenced_open_events
)
SELECT 
  "APP_GROUP_ID",
  "CAMPAIGN_ID",
  "USER_ID",
  "MESSAGE_VARIATION_ID",
  "PLATFORM",
  "AD_TRACKING_ENABLED",
  "CARRIER",
  "BROWSER",
  "DEVICE_MODEL",
  SUM("push_notification_sends") AS "push_notification_sends",
  SUM("unique_push_notification_sends") AS "unique_push_notification_sends",
  SUM("push_notification_bounced") AS "push_notification_bounced",
  SUM("unique_push_notification_bounced") AS "unique_push_notification_bounced",
  SUM("push_notification_open") AS "push_notification_open",
  SUM("unique_push_notification_opened") AS "unique_push_notification_opened",
  SUM("push_notification_influenced_open") AS "push_notification_influenced_open",
  SUM("unique_push_notification_influenced_open") AS "unique_push_notification_influenced_open"
FROM all_events
GROUP BY 1,2,3,4,5,6,7,8,9
ORDER BY 1,2,3,4,5,6,7,8,9