WITH date_series AS (
    SELECT DATE '2018-08-30' AS "date"
    UNION ALL
    SELECT DATEADD(day, 1, "date")
    FROM date_series
    WHERE "date" < DATE '2018-09-30'
), creates AS (
    SELECT 
        DATEADD('second', "block_timestamp"::BIGINT / 1000000, '1970-01-01')::DATE AS "create_date",
        CASE WHEN "trace_address" IS NULL THEN 'external' ELSE 'internal' END AS "category"
    FROM "CRYPTO"."CRYPTO_ETHEREUM"."TRACES"
    WHERE "trace_type" = 'create'
), daily_counts AS (
    SELECT 
        "create_date",
        COUNT(CASE WHEN "category" = 'external' THEN 1 END) AS "external_daily",
        COUNT(CASE WHEN "category" = 'internal' THEN 1 END) AS "internal_daily"
    FROM creates
    WHERE "create_date" BETWEEN DATE '2018-08-30' AND DATE '2018-09-30'
    GROUP BY "create_date"
)
SELECT 
    ds."date",
    SUM(COALESCE(dc."external_daily", 0)) OVER (ORDER BY ds."date") AS "cumulative_external",
    SUM(COALESCE(dc."internal_daily", 0)) OVER (ORDER BY ds."date") AS "cumulative_internal"
FROM date_series ds
LEFT JOIN daily_counts dc ON ds."date" = dc."create_date"
ORDER BY ds."date"