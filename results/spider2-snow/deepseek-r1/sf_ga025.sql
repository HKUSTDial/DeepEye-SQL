WITH all_events AS (
  SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180901"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180902"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180903"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180904"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180905"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180906"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180907"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180908"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180909"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180910"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180911"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180912"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180913"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180914"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180915"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180916"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180917"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180918"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180919"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180920"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180921"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180922"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180923"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180924"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180925"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180926"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180927"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180928"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180929"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20180930"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20181001"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20181002"
  UNION ALL SELECT "user_pseudo_id", "user_first_touch_timestamp", "event_name", "event_timestamp" FROM "FIREBASE"."ANALYTICS_153293282"."EVENTS_20181003"
),
user_first_open AS (
  SELECT 
    "user_pseudo_id",
    MIN(DATE(TO_TIMESTAMP("user_first_touch_timestamp" / 1000000))) AS first_open_date
  FROM all_events
  GROUP BY "user_pseudo_id"
  HAVING first_open_date BETWEEN '2018-09-01' AND '2018-09-30'
),
uninstall_events AS (
  SELECT 
    "user_pseudo_id",
    MIN(DATE(TO_TIMESTAMP("event_timestamp" / 1000000))) AS uninstall_date
  FROM all_events
  WHERE "event_name" = 'app_remove'
  GROUP BY "user_pseudo_id"
),
users_within_7 AS (
  SELECT 
    ufo."user_pseudo_id",
    ufo.first_open_date,
    ue.uninstall_date
  FROM user_first_open ufo
  INNER JOIN uninstall_events ue ON ufo."user_pseudo_id" = ue."user_pseudo_id"
  WHERE DATEDIFF(day, ufo.first_open_date, ue.uninstall_date) <= 7
    AND DATEDIFF(day, ufo.first_open_date, ue.uninstall_date) >= 0
),
crash_users AS (
  SELECT DISTINCT "user_pseudo_id"
  FROM all_events
  WHERE "event_name" = 'app_exception'
)
SELECT 
  COUNT(DISTINCT u."user_pseudo_id") AS total_users,
  COUNT(DISTINCT CASE WHEN c."user_pseudo_id" IS NOT NULL THEN u."user_pseudo_id" END) AS crashed_users,
  (crashed_users * 100.0) / total_users AS percentage
FROM users_within_7 u
LEFT JOIN crash_users c ON u."user_pseudo_id" = c."user_pseudo_id"