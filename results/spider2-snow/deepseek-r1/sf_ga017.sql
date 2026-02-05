WITH page_views AS (
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210101" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210102" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210103" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210104" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210105" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210106" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210107" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210108" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210109" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210110" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210111" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210112" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210113" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210114" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210115" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210116" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210117" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210118" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210119" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210120" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210121" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210122" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210123" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210124" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210125" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210126" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210127" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210128" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210129" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210130" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
    UNION ALL
    SELECT 
        e."USER_PSEUDO_ID",
        param.value:"string_value"::STRING AS page_location
    FROM 
        "GA4"."GA4_OBFUSCATED_SAMPLE_ECOMMERCE"."EVENTS_20210131" e,
        LATERAL FLATTEN(INPUT => e."EVENT_PARAMS") param
    WHERE 
        e."EVENT_NAME" = 'page_view'
        AND param.key = 'page_location'
        AND e."EVENT_DATE" BETWEEN '20210101' AND '20210131'
),
page_visits AS (
    SELECT 
        page_location,
        COUNT(*) AS visit_count
    FROM page_views
    GROUP BY page_location
),
top_page AS (
    SELECT 
        page_location
    FROM page_visits
    ORDER BY visit_count DESC
    LIMIT 1
)
SELECT 
    COUNT(DISTINCT "USER_PSEUDO_ID") AS distinct_users
FROM page_views
WHERE page_location IN (SELECT page_location FROM top_page)