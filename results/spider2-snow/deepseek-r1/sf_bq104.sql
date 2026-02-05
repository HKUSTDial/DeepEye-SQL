WITH latest_refresh AS (
    SELECT MAX("refresh_date") AS max_refresh
    FROM "GOOGLE_TRENDS"."GOOGLE_TRENDS"."INTERNATIONAL_TOP_RISING_TERMS"
), latest_week_in_refresh AS (
    SELECT MAX("week") AS max_week
    FROM "GOOGLE_TRENDS"."GOOGLE_TRENDS"."INTERNATIONAL_TOP_RISING_TERMS"
    WHERE "refresh_date" = (SELECT max_refresh FROM latest_refresh)
)
SELECT "term"
FROM "GOOGLE_TRENDS"."GOOGLE_TRENDS"."INTERNATIONAL_TOP_RISING_TERMS"
WHERE "refresh_date" = (SELECT max_refresh FROM latest_refresh)
  AND "week" = DATEADD(week, -52, (SELECT max_week FROM latest_week_in_refresh))
ORDER BY "rank" ASC, "percent_gain" DESC
LIMIT 1