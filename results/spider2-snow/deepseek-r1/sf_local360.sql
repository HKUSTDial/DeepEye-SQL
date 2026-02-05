WITH first_click_event AS (
    SELECT 
        "session",
        "path",
        "search_type",
        TO_TIMESTAMP("stamp", 'YYYY-MM-DD HH24:MI:SS') AS click_stamp,
        ROW_NUMBER() OVER (PARTITION BY "session" ORDER BY TO_TIMESTAMP("stamp", 'YYYY-MM-DD HH24:MI:SS')) AS rn
    FROM "LOG"."LOG"."ACTIVITY_LOG"
    WHERE ("path" LIKE '%/detail%' OR "path" LIKE '%/complete%')
),
session_pre_counts AS (
    SELECT 
        f."session",
        COUNT(a."session") AS pre_count
    FROM first_click_event f
    LEFT JOIN "LOG"."LOG"."ACTIVITY_LOG" a 
        ON f."session" = a."session" 
        AND TO_TIMESTAMP(a."stamp", 'YYYY-MM-DD HH24:MI:SS') < f.click_stamp
        AND a."search_type" != ''
    WHERE f.rn = 1
    GROUP BY f."session"
),
min_count AS (
    SELECT MIN(pre_count) AS min_pre_count
    FROM session_pre_counts
),
sessions_with_min AS (
    SELECT spc."session"
    FROM session_pre_counts spc
    CROSS JOIN min_count mc
    WHERE spc.pre_count = mc.min_pre_count
)
SELECT 
    sm."session",
    fce."path",
    fce."search_type"
FROM sessions_with_min sm
JOIN first_click_event fce ON sm."session" = fce."session" AND fce.rn = 1
ORDER BY sm."session"