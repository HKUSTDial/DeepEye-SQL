WITH driver_laps AS (
  SELECT 
    "race_id",
    "driver_id",
    0 AS "lap",
    "grid" AS "position"
  FROM "F1"."F1"."RESULTS"
  UNION ALL
  SELECT 
    "race_id",
    "driver_id",
    "lap",
    "position"
  FROM "F1"."F1"."LAP_POSITIONS"
  WHERE "lap_type" = 'Race' 
    AND "lap" BETWEEN 1 AND 5
),
driver_prev_curr AS (
  SELECT 
    curr."race_id",
    curr."lap",
    curr."driver_id",
    curr."position" AS "pos_curr",
    prev."position" AS "pos_prev"
  FROM driver_laps curr
  INNER JOIN driver_laps prev 
    ON curr."race_id" = prev."race_id" 
    AND curr."driver_id" = prev."driver_id" 
    AND curr."lap" = prev."lap" + 1
  WHERE curr."lap" BETWEEN 1 AND 5
),
overtakes AS (
  SELECT 
    A."race_id",
    A."lap",
    A."driver_id" AS "overtaking_driver_id",
    B."driver_id" AS "overtaken_driver_id",
    A."pos_prev" AS "a_pos_prev",
    A."pos_curr" AS "a_pos_curr",
    B."pos_prev" AS "b_pos_prev",
    B."pos_curr" AS "b_pos_curr"
  FROM driver_prev_curr A
  JOIN driver_prev_curr B 
    ON A."race_id" = B."race_id" 
    AND A."lap" = B."lap"
    AND A."driver_id" != B."driver_id"
  WHERE A."pos_prev" > B."pos_prev" 
    AND A."pos_curr" < B."pos_curr"
),
overtakes_with_grid AS (
  SELECT 
    O.*,
    RES_A."grid" AS "grid_overtaking",
    RES_B."grid" AS "grid_overtaken"
  FROM overtakes O
  LEFT JOIN "F1"."F1"."RESULTS" RES_A 
    ON O."race_id" = RES_A."race_id" 
    AND O."overtaking_driver_id" = RES_A."driver_id"
  LEFT JOIN "F1"."F1"."RESULTS" RES_B 
    ON O."race_id" = RES_B."race_id" 
    AND O."overtaken_driver_id" = RES_B."driver_id"
),
overtakes_classified AS (
  SELECT 
    O.*,
    CASE 
      WHEN EXISTS (
        SELECT 1 
        FROM "F1"."F1"."RETIREMENTS" R 
        WHERE R."race_id" = O."race_id" 
          AND R."driver_id" = O."overtaken_driver_id" 
          AND R."lap" = O."lap"
      ) THEN 'R'
      WHEN EXISTS (
        SELECT 1 
        FROM "F1"."F1"."PIT_STOPS" P 
        WHERE P."race_id" = O."race_id" 
          AND P."driver_id" = O."overtaken_driver_id" 
          AND P."lap" = O."lap"
      ) THEN 'P'
      WHEN EXISTS (
        SELECT 1 
        FROM "F1"."F1"."PIT_STOPS" P 
        WHERE P."race_id" = O."race_id" 
          AND P."driver_id" = O."overtaken_driver_id" 
          AND P."lap" = O."lap" - 1
      ) THEN 'P'
      WHEN O."lap" = 1 AND ABS(O."grid_overtaking" - O."grid_overtaken") <= 2 THEN 'S'
      ELSE 'T'
    END AS "category"
  FROM overtakes_with_grid O
),
categories AS (
  SELECT 'R' AS "category" 
  UNION ALL 
  SELECT 'P' 
  UNION ALL 
  SELECT 'S' 
  UNION ALL 
  SELECT 'T'
)
SELECT 
  c."category",
  COUNT(o."category") AS "overtake_count"
FROM categories c
LEFT JOIN overtakes_classified o ON c."category" = o."category"
GROUP BY c."category"
ORDER BY c."category"