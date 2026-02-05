WITH races_with_pit AS (
  SELECT "race_id" FROM "F1"."F1"."RACES_EXT" WHERE "is_pit_data_available" = 1
),
lap_positions AS (
  SELECT lp."race_id", lp."lap", lp."driver_id", lp."position"
  FROM "F1"."F1"."LAP_POSITIONS" lp
  WHERE lp."lap_type" = 'Race'
    AND lp."race_id" IN (SELECT "race_id" FROM races_with_pit)
),
overtake_events AS (
  SELECT
    p1."race_id",
    p1."lap" + 1 AS "overtake_lap",
    p1."driver_id" AS "overtaken_driver_id",
    p2."driver_id" AS "overtaking_driver_id"
  FROM lap_positions p1
  JOIN lap_positions p2
    ON p1."race_id" = p2."race_id"
    AND p1."lap" = p2."lap"
    AND p1."driver_id" != p2."driver_id"
    AND p1."position" < p2."position"
  JOIN lap_positions c1
    ON p1."race_id" = c1."race_id"
    AND p1."lap" + 1 = c1."lap"
    AND p1."driver_id" = c1."driver_id"
  JOIN lap_positions c2
    ON p2."race_id" = c2."race_id"
    AND p1."lap" + 1 = c2."lap"
    AND p2."driver_id" = c2."driver_id"
  WHERE c1."position" > c2."position"
),
classified_overtakes AS (
  SELECT
    oe."race_id",
    oe."overtake_lap",
    oe."overtaken_driver_id",
    oe."overtaking_driver_id",
    CASE
      WHEN r."driver_id" IS NOT NULL THEN 'R'
      WHEN ps1."driver_id" IS NOT NULL THEN 'P'
      WHEN ps2."driver_id" IS NOT NULL THEN 'P'
      WHEN oe."overtake_lap" = 1 AND ABS(res1."grid" - res2."grid") <= 1 THEN 'S'
      ELSE 'T'
    END AS "overtake_type"
  FROM overtake_events oe
  LEFT JOIN "F1"."F1"."RETIREMENTS" r
    ON oe."race_id" = r."race_id"
    AND oe."overtaken_driver_id" = r."driver_id"
    AND oe."overtake_lap" = r."lap"
  LEFT JOIN "F1"."F1"."PIT_STOPS" ps1
    ON oe."race_id" = ps1."race_id"
    AND oe."overtaken_driver_id" = ps1."driver_id"
    AND oe."overtake_lap" = ps1."lap"
  LEFT JOIN "F1"."F1"."PIT_STOPS" ps2
    ON oe."race_id" = ps2."race_id"
    AND oe."overtaken_driver_id" = ps2."driver_id"
    AND oe."overtake_lap" - 1 = ps2."lap"
  LEFT JOIN "F1"."F1"."RESULTS" res1
    ON oe."race_id" = res1."race_id"
    AND oe."overtaken_driver_id" = res1."driver_id"
  LEFT JOIN "F1"."F1"."RESULTS" res2
    ON oe."race_id" = res2."race_id"
    AND oe."overtaking_driver_id" = res2."driver_id"
)
SELECT
  "overtake_type",
  COUNT(*) AS "count"
FROM classified_overtakes
GROUP BY "overtake_type"
ORDER BY "overtake_type"