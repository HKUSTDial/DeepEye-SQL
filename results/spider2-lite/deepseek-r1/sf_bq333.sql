WITH session_durations AS (
  SELECT 
    "session_id",
    MIN("browser") AS "browser",
    (MAX("created_at") - MIN("created_at")) / 1000000 AS "duration_seconds"
  FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."EVENTS"
  GROUP BY "session_id"
)
SELECT 
  "browser",
  AVG("duration_seconds") AS "avg_session_duration_seconds"
FROM session_durations
GROUP BY "browser"
HAVING COUNT(*) > 10
ORDER BY "avg_session_duration_seconds" ASC
LIMIT 3