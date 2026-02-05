WITH session_users AS (
  SELECT DISTINCT "user_pseudo_id"
  FROM (
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180702"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180703"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180704"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180705"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180706"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180707"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180708"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180709"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180710"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180711"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180712"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180713"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180714"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180715"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180716"
  )
  WHERE "event_name" = 'session_start'
    AND "event_date" BETWEEN '20180702' AND '20180716'
),
first_quickplay AS (
  SELECT 
    "user_pseudo_id",
    "event_name" as "quickplay_type",
    TO_DATE("event_date", 'YYYYMMDD') as "first_quickplay_date",
    ROW_NUMBER() OVER (PARTITION BY "user_pseudo_id" ORDER BY "event_timestamp") as rn
  FROM (
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180702"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180703"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180704"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180705"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180706"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180707"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180708"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180709"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180710"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180711"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180712"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180713"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180714"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180715"
    UNION ALL
    SELECT "user_pseudo_id", "event_date", "event_name", "event_timestamp"
    FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180716"
  )
  WHERE "user_pseudo_id" IN (SELECT "user_pseudo_id" FROM session_users)
    AND "event_name" LIKE '%quickplay%'
),
user_cohort AS (
  SELECT 
    "user_pseudo_id",
    "quickplay_type",
    "first_quickplay_date"
  FROM first_quickplay
  WHERE rn = 1
),
retention_check AS (
  SELECT 
    uc."user_pseudo_id",
    uc."quickplay_type",
    uc."first_quickplay_date",
    CASE 
      WHEN EXISTS (
        SELECT 1
        FROM (
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180702"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180703"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180704"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180705"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180706"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180707"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180708"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180709"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180710"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180711"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180712"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180713"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180714"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180715"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180716"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180717"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180718"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180719"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180720"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180721"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180722"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180723"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180724"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180725"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180726"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180727"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180728"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180729"
          UNION ALL
          SELECT "user_pseudo_id", "event_date"
          FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180730"
        ) events
        WHERE events."user_pseudo_id" = uc."user_pseudo_id"
          AND TO_DATE(events."event_date", 'YYYYMMDD') = DATEADD(day, 14, uc."first_quickplay_date")
      ) THEN 1
      ELSE 0
    END as "retained"
  FROM user_cohort uc
)
SELECT 
  "quickplay_type",
  COUNT(*) as "total_users",
  SUM("retained") as "retained_users",
  ROUND(SUM("retained") * 100.0 / COUNT(*), 2) as "retention_rate_percent"
FROM retention_check
GROUP BY "quickplay_type"
ORDER BY "quickplay_type"