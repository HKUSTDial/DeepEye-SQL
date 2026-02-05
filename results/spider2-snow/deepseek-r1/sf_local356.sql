WITH "lap_positions_filtered" AS (
  SELECT 
    lp."race_id",
    lp."driver_id",
    lp."lap",
    lp."position",
    CASE WHEN ps."lap" IS NOT NULL THEN 1 ELSE 0 END AS "pitted",
    CASE WHEN r."lap" IS NOT NULL AND r."lap" <= lp."lap" THEN 1 ELSE 0 END AS "retired"
  FROM "F1"."F1"."LAP_POSITIONS" lp
  LEFT JOIN "F1"."F1"."PIT_STOPS" ps 
    ON lp."race_id" = ps."race_id" AND lp."driver_id" = ps."driver_id" AND lp."lap" = ps."lap"
  LEFT JOIN "F1"."F1"."RETIREMENTS" r 
    ON lp."race_id" = r."race_id" AND lp."driver_id" = r."driver_id"
  WHERE lp."lap_type" = 'Race'
),
"consecutive_laps" AS (
  SELECT 
    a."race_id" AS "race_id",
    a."lap" AS "lap",
    a."driver_id" AS "driver_id",
    a."position" AS "pos_curr",
    b."position" AS "pos_prev",
    a."pitted" AS "pitted_curr",
    b."pitted" AS "pitted_prev",
    a."retired" AS "retired_curr",
    b."retired" AS "retired_prev"
  FROM "lap_positions_filtered" a
  INNER JOIN "lap_positions_filtered" b 
    ON a."race_id" = b."race_id" 
    AND a."driver_id" = b."driver_id" 
    AND a."lap" = b."lap" + 1
  WHERE a."lap" > 2
),
"overtake_events" AS (
  SELECT 
    c1."race_id" AS "race_id",
    c1."lap" AS "lap",
    c1."driver_id" AS "overtaker_id",
    c2."driver_id" AS "overtaken_id"
  FROM "consecutive_laps" c1
  INNER JOIN "consecutive_laps" c2 
    ON c1."race_id" = c2."race_id" 
    AND c1."lap" = c2."lap"
  WHERE c1."driver_id" != c2."driver_id"
    AND c1."pos_prev" > c2."pos_prev"
    AND c1."pos_curr" < c2."pos_curr"
    AND c1."pitted_prev" = 0 AND c1."pitted_curr" = 0
    AND c2."pitted_prev" = 0 AND c2."pitted_curr" = 0
    AND c1."retired_prev" = 0 AND c1."retired_curr" = 0
    AND c2."retired_prev" = 0 AND c2."retired_curr" = 0
),
"overtakes_made" AS (
  SELECT "overtaker_id" AS "driver_id", COUNT(*) AS "overtakes_count"
  FROM "overtake_events"
  GROUP BY "overtaker_id"
),
"overtaken_times" AS (
  SELECT "overtaken_id" AS "driver_id", COUNT(*) AS "overtaken_count"
  FROM "overtake_events"
  GROUP BY "overtaken_id"
),
"driver_stats" AS (
  SELECT 
    COALESCE(m."driver_id", t."driver_id") AS "driver_id",
    COALESCE(m."overtakes_count", 0) AS "overtakes_made",
    COALESCE(t."overtaken_count", 0) AS "overtaken_times"
  FROM "overtakes_made" m
  FULL OUTER JOIN "overtaken_times" t ON m."driver_id" = t."driver_id"
)
SELECT d."full_name"
FROM "driver_stats" ds
INNER JOIN "F1"."F1"."DRIVERS_EXT" d ON ds."driver_id" = d."driver_id"
WHERE ds."overtaken_times" > ds."overtakes_made"
ORDER BY d."full_name"