WITH ordered_logs AS (
    SELECT 
        "session",
        "path",
        TO_TIMESTAMP("stamp", 'YYYY-MM-DD HH24:MI:SS') AS stamp_time,
        LAG("path", 1) OVER (PARTITION BY "session" ORDER BY TO_TIMESTAMP("stamp", 'YYYY-MM-DD HH24:MI:SS')) AS prev_path,
        LEAD("path", 1) OVER (PARTITION BY "session" ORDER BY TO_TIMESTAMP("stamp", 'YYYY-MM-DD HH24:MI:SS')) AS next_path
    FROM "LOG"."LOG"."ACTIVITY_LOG"
)
SELECT 
    next_path AS third_page_visit,
    COUNT(*) AS frequency
FROM ordered_logs
WHERE "path" = '/detail' AND prev_path = '/detail' AND next_path IS NOT NULL
GROUP BY next_path
ORDER BY frequency DESC
LIMIT 3