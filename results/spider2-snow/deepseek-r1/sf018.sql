WITH all_events AS (
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
        'send' AS event_type
    FROM "BRAZE_USER_EVENT_DEMO_DATASET"."PUBLIC"."USERS_MESSAGES_PUSHNOTIFICATION_SEND_VIEW"
    WHERE "TIME" >= 1685606400 AND "TIME" < 1685610000
    UNION ALL
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
        'bounce' AS event_type
    FROM "BRAZE_USER_EVENT_DEMO_DATASET"."PUBLIC"."USERS_MESSAGES_PUSHNOTIFICATION_BOUNCE_VIEW"
    WHERE "TIME" >= 1685606400 AND "TIME" < 1685610000
    UNION ALL
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
        'open' AS event_type
    FROM "BRAZE_USER_EVENT_DEMO_DATASET"."PUBLIC"."USERS_MESSAGES_PUSHNOTIFICATION_OPEN_VIEW"
    WHERE "TIME" >= 1685606400 AND "TIME" < 1685610000
    UNION ALL
    SELECT 
        "APP_GROUP_ID",
        "CAMPAIGN_ID",
        "USER_ID",
        "MESSAGE_VARIATION_ID",
        "PLATFORM",
        CAST(NULL AS BOOLEAN) AS "AD_TRACKING_ENABLED",
        "CARRIER",
        "BROWSER",
        "DEVICE_MODEL",
        'influenced_open' AS event_type
    FROM "BRAZE_USER_EVENT_DEMO_DATASET"."PUBLIC"."USERS_MESSAGES_PUSHNOTIFICATION_INFLUENCEDOPEN_VIEW"
    WHERE "TIME" >= 1685606400 AND "TIME" < 1685610000
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
    COUNT(CASE WHEN event_type = 'send' THEN 1 END) AS push_notification_sends,
    COUNT(DISTINCT CASE WHEN event_type = 'send' THEN "USER_ID" END) AS unique_push_notification_sends,
    COUNT(CASE WHEN event_type = 'bounce' THEN 1 END) AS push_notification_bounced,
    COUNT(DISTINCT CASE WHEN event_type = 'bounce' THEN "USER_ID" END) AS unique_push_notification_bounced,
    COUNT(CASE WHEN event_type = 'open' THEN 1 END) AS push_notification_open,
    COUNT(DISTINCT CASE WHEN event_type = 'open' THEN "USER_ID" END) AS unique_push_notification_opened,
    COUNT(CASE WHEN event_type = 'influenced_open' THEN 1 END) AS push_notification_influenced_open,
    COUNT(DISTINCT CASE WHEN event_type = 'influenced_open' THEN "USER_ID" END) AS unique_push_notification_influenced_open
FROM all_events
GROUP BY 1,2,3,4,5,6,7,8,9