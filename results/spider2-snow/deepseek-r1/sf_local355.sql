WITH all_races AS (
  SELECT "race_id", "year", "round"
  FROM "F1"."F1"."RACES"
  UNION
  SELECT "race_id", "year", "round"
  FROM "F1"."F1"."RACES_EXT"
),
drivers_per_season AS (
  SELECT DISTINCT "driver_id", "year"
  FROM "F1"."F1"."DRIVES"
),
driver_year_rounds AS (
  SELECT d."driver_id", d."year", ar."round", ar."race_id"
  FROM drivers_per_season d
  INNER JOIN all_races ar ON d."year" = ar."year"
),
driver_participation AS (
  SELECT dyr."driver_id", dyr."year", dyr."round", dyr."race_id",
         res."constructor_id",
         CASE WHEN res."race_id" IS NOT NULL THEN 1 ELSE 0 END as participated
  FROM driver_year_rounds dyr
  LEFT JOIN "F1"."F1"."RESULTS" res 
    ON dyr."race_id" = res."race_id" AND dyr."driver_id" = res."driver_id"
),
driver_year_total_missed AS (
  SELECT "driver_id", "year",
         COUNT(CASE WHEN participated = 0 THEN 1 END) as total_missed
  FROM driver_participation
  GROUP BY "driver_id", "year"
),
participation_seq AS (
  SELECT *,
         LAG(participated) OVER (PARTITION BY "driver_id", "year" ORDER BY "round") as prev_participated
  FROM driver_participation
),
gap_starts AS (
  SELECT *,
         CASE WHEN participated = 0 AND (prev_participated IS NULL OR prev_participated = 1) THEN 1 ELSE 0 END as gap_start_flag
  FROM participation_seq
),
gap_groups AS (
  SELECT *,
         SUM(gap_start_flag) OVER (PARTITION BY "driver_id", "year" ORDER BY "round" ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as gap_group
  FROM gap_starts
),
missed_blocks AS (
  SELECT "driver_id", "year",
         MIN("round") as first_missed_round,
         MAX("round") as last_missed_round,
         COUNT(*) as missed_count,
         MIN("round") - 1 as before_round,
         MAX("round") + 1 as after_round
  FROM gap_groups
  WHERE participated = 0
  GROUP BY "driver_id", "year", gap_group
),
filtered_blocks AS (
  SELECT mb.*
  FROM missed_blocks mb
  INNER JOIN driver_year_total_missed dyt 
    ON mb."driver_id" = dyt."driver_id" AND mb."year" = dyt."year"
  WHERE dyt.total_missed < 3
),
blocks_with_before_after AS (
  SELECT fb.*,
         before."constructor_id" as before_constructor_id,
         after."constructor_id" as after_constructor_id
  FROM filtered_blocks fb
  LEFT JOIN driver_participation before 
    ON fb."driver_id" = before."driver_id" 
    AND fb."year" = before."year" 
    AND fb.before_round = before."round"
    AND before.participated = 1
  LEFT JOIN driver_participation after 
    ON fb."driver_id" = after."driver_id" 
    AND fb."year" = after."year" 
    AND fb.after_round = after."round"
    AND after.participated = 1
)
SELECT 
  AVG(first_missed_round) as avg_first_round,
  AVG(last_missed_round) as avg_last_round
FROM blocks_with_before_after
WHERE before_constructor_id IS NOT NULL
  AND after_constructor_id IS NOT NULL
  AND before_constructor_id != after_constructor_id