WITH latest_refresh AS (
    SELECT MAX("refresh_date") AS max_refresh
    FROM "GOOGLE_TRENDS"."GOOGLE_TRENDS"."INTERNATIONAL_TOP_RISING_TERMS"
),
latest_week AS (
    SELECT MAX("week") AS max_week
    FROM "GOOGLE_TRENDS"."GOOGLE_TRENDS"."INTERNATIONAL_TOP_RISING_TERMS"
    WHERE "refresh_date" = (SELECT max_refresh FROM latest_refresh)
),
target_week AS (
    SELECT DATEADD(week, -52, max_week) AS target_week_date
    FROM latest_week
)
SELECT "term"
FROM "GOOGLE_TRENDS"."GOOGLE_TRENDS"."INTERNATIONAL_TOP_RISING_TERMS"
WHERE "refresh_date" = (SELECT max_refresh FROM latest_refresh)
    AND "week" = (SELECT target_week_date FROM target_week)
    AND "rank" = 1
ORDER BY "percent_gain" DESC, "term"
LIMIT 1