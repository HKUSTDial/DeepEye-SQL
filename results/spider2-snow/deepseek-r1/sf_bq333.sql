WITH session_durations AS (
    SELECT 
        "session_id",
        MIN("browser") AS "browser",
        MIN("created_at") AS min_ts,
        MAX("created_at") AS max_ts
    FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."EVENTS"
    WHERE "browser" IS NOT NULL
    GROUP BY "session_id"
)
SELECT 
    "browser",
    AVG((max_ts - min_ts) / 1000000) AS avg_session_duration_seconds,
    COUNT(*) AS total_sessions
FROM session_durations
GROUP BY "browser"
HAVING COUNT(*) > 10
ORDER BY avg_session_duration_seconds ASC
LIMIT 3