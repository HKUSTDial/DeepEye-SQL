WITH landing AS (
    SELECT "session", "path" AS landing_page
    FROM (
        SELECT "session", "path",
               ROW_NUMBER() OVER (PARTITION BY "session" ORDER BY TO_TIMESTAMP("stamp") ASC) AS rn_asc
        FROM "LOG"."LOG"."ACTIVITY_LOG"
    ) WHERE rn_asc = 1
),
exit AS (
    SELECT "session", "path" AS exit_page
    FROM (
        SELECT "session", "path",
               ROW_NUMBER() OVER (PARTITION BY "session" ORDER BY TO_TIMESTAMP("stamp") DESC) AS rn_desc
        FROM "LOG"."LOG"."ACTIVITY_LOG"
    ) WHERE rn_desc = 1
),
combined_pages AS (
    SELECT "session", landing_page AS page FROM landing
    UNION
    SELECT "session", exit_page AS page FROM exit
)
SELECT page, COUNT(DISTINCT "session") AS total_unique_sessions
FROM combined_pages
GROUP BY page
ORDER BY page