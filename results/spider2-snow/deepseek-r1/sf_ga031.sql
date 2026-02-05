WITH page_views AS (
    SELECT 
        e."USER_PSEUDO_ID",
        MAX(CASE WHEN params."VALUE":"key" = 'ga_session_id' THEN params."VALUE":"value":"string_value"::STRING END) AS session_id,
        MAX(CASE WHEN params."VALUE":"key" = 'page_title' THEN params."VALUE":"value":"string_value"::STRING END) AS page_title
    FROM "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210102" e,
    LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") params
    WHERE e."EVENT_NAME" = 'page_view'
      AND e."EVENT_DATE" = '20210102'
    GROUP BY e."USER_PSEUDO_ID", e."EVENT_TIMESTAMP"
),
home_sessions AS (
    SELECT DISTINCT "USER_PSEUDO_ID", session_id
    FROM page_views
    WHERE page_title = 'Home'
),
checkout_sessions AS (
    SELECT DISTINCT "USER_PSEUDO_ID", session_id
    FROM page_views
    WHERE page_title = 'Checkout Confirmation'
),
both_sessions AS (
    SELECT h."USER_PSEUDO_ID", h.session_id
    FROM home_sessions h
    INNER JOIN checkout_sessions c 
        ON h."USER_PSEUDO_ID" = c."USER_PSEUDO_ID" 
        AND h.session_id = c.session_id
),
counts AS (
    SELECT 
        (SELECT COUNT(*) FROM home_sessions) AS home_count,
        (SELECT COUNT(*) FROM both_sessions) AS both_count
)
SELECT 
    ROUND((both_count * 100.0) / NULLIF(home_count, 0), 2) AS conversion_rate
FROM counts;