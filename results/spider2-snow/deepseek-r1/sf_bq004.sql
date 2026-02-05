WITH july_sessions AS (
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170701"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170702"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170703"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170704"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170705"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170706"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170707"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170708"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170709"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170710"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170711"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170712"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170713"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170714"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170715"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170716"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170717"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170718"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170719"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170720"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170721"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170722"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170723"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170724"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170725"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170726"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170727"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170728"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170729"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170730"
    UNION ALL
    SELECT "fullVisitorId", "date", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170731"
), flattened_sessions AS (
    SELECT
        js."fullVisitorId",
        hit.value AS hit,
        product.value AS product
    FROM july_sessions js
    LEFT JOIN LATERAL FLATTEN(INPUT => js."hits") hit
    LEFT JOIN LATERAL FLATTEN(INPUT => hit.value:"product") product
), youtube_buyers AS (
    SELECT DISTINCT
        fs."fullVisitorId"
    FROM flattened_sessions fs
    WHERE
        fs.hit:"eCommerceAction"."action_type" = '6'
        AND fs.product:"v2ProductName" ILIKE '%YouTube%'
), product_quantities AS (
    SELECT
        fs.product:"v2ProductName" AS product_name,
        SUM(fs.product:"productQuantity") AS total_quantity
    FROM flattened_sessions fs
    INNER JOIN youtube_buyers yb ON fs."fullVisitorId" = yb."fullVisitorId"
    WHERE
        fs.hit:"eCommerceAction"."action_type" = '6'
        AND fs.product:"v2ProductName" IS NOT NULL
        AND fs.product:"v2ProductName" NOT ILIKE '%YouTube%'
    GROUP BY fs.product:"v2ProductName"
)
SELECT
    product_name,
    total_quantity
FROM product_quantities
ORDER BY total_quantity DESC
LIMIT 1