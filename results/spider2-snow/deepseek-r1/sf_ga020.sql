WITH "august_events" AS (
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180801"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180802"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180803"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180804"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180805"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180806"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180807"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180808"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180809"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180810"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180811"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180812"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180813"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180814"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180815"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180816"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180817"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180818"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180819"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180820"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180821"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180822"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180823"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180824"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180825"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180826"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180827"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180828"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180829"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180830"
  UNION ALL
  SELECT "user_pseudo_id", "event_name", "event_date", "event_timestamp", "user_first_touch_timestamp"
  FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180831"
),
"cohort" AS (
  SELECT
    "user_pseudo_id",
    DATE(TO_TIMESTAMP(MIN("user_first_touch_timestamp") / 1000000)) AS "first_engagement_date"
  FROM "august_events"
  WHERE DATE(TO_TIMESTAMP("user_first_touch_timestamp" / 1000000)) BETWEEN '2018-08-01' AND '2018-08-15'
  GROUP BY "user_pseudo_id"
),
"quickplay_events" AS (
  SELECT DISTINCT
    c."user_pseudo_id",
    c."first_engagement_date",
    e."event_name"
  FROM "cohort" c
  JOIN "august_events" e
    ON c."user_pseudo_id" = e."user_pseudo_id"
    AND e."event_date" = TO_CHAR(c."first_engagement_date", 'YYYYMMDD')
  WHERE e."event_name" LIKE '%quickplay%'
),
"retention_flag" AS (
  SELECT
    c."user_pseudo_id",
    MAX(CASE
      WHEN se."event_name" = 'session_start'
        AND DATE(TO_TIMESTAMP(se."event_timestamp" / 1000000)) BETWEEN DATEADD(day, 8, c."first_engagement_date") AND DATEADD(day, 14, c."first_engagement_date")
      THEN 1
      ELSE 0
    END) AS "retained_week2"
  FROM "cohort" c
  LEFT JOIN "august_events" se
    ON c."user_pseudo_id" = se."user_pseudo_id"
  GROUP BY c."user_pseudo_id"
),
"user_event_retention" AS (
  SELECT
    qe."user_pseudo_id",
    qe."event_name",
    rf."retained_week2"
  FROM "quickplay_events" qe
  JOIN "retention_flag" rf
    ON qe."user_pseudo_id" = rf."user_pseudo_id"
),
"retention_rates" AS (
  SELECT
    "event_name",
    COUNT(DISTINCT "user_pseudo_id") AS "total_users",
    SUM("retained_week2") AS "retained_users",
    "retained_users" / "total_users" AS "retention_rate"
  FROM "user_event_retention"
  GROUP BY "event_name"
)
SELECT "event_name"
FROM "retention_rates"
ORDER BY "retention_rate" ASC
LIMIT 1