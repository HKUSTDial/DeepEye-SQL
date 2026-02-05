WITH sessions AS (
    SELECT "date", "fullVisitorId", "totals", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170429"
    UNION ALL
    SELECT "date", "fullVisitorId", "totals", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170430"
    UNION ALL
    SELECT "date", "fullVisitorId", "totals", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170501"
    UNION ALL
    SELECT "date", "fullVisitorId", "totals", "hits"
    FROM "GA360"."GOOGLE_ANALYTICS_SAMPLE"."GA_SESSIONS_20170502"
    -- ... include all tables for dates from 2017-04-01 to 2017-07-31
),
session_hits_revenue AS (
    SELECT 
        s."date",
        s."fullVisitorId",
        s."totals",
        MAX(CASE WHEN h.value:"product"."productRevenue" IS NOT NULL THEN 1 ELSE 0 END) AS has_product_revenue
    FROM sessions s
    LEFT JOIN LATERAL FLATTEN(INPUT => s."hits") h
    GROUP BY s."date", s."fullVisitorId", s."totals"
),
classified_sessions AS (
    SELECT 
        TO_DATE("date", 'YYYYMMDD') AS session_date,
        EXTRACT(MONTH FROM TO_DATE("date", 'YYYYMMDD')) AS month,
        "fullVisitorId",
        COALESCE("totals":"pageviews", 0) AS pageviews,
        CASE 
            WHEN "totals":"transactions" >= 1 AND has_product_revenue = 1 THEN 'purchase'
            WHEN "totals":"transactions" IS NULL AND has_product_revenue = 0 THEN 'non-purchase'
            ELSE 'other'
        END AS session_type
    FROM session_hits_revenue
    WHERE session_date BETWEEN '2017-04-01' AND '2017-07-31'
)
SELECT 
    month,
    session_type,
    SUM(pageviews) / COUNT(DISTINCT "fullVisitorId") AS avg_pageviews_per_visitor
FROM classified_sessions
WHERE session_type IN ('purchase', 'non-purchase')
GROUP BY month, session_type
ORDER BY month, session_type