WITH eligible_seasons AS (
  SELECT d1."driver_id", d1."year"
  FROM "F1"."F1"."DRIVES" d1
  INNER JOIN "F1"."F1"."DRIVES" d2 ON d1."driver_id" = d2."driver_id" AND d1."year" = d2."year"
  WHERE d1."year" BETWEEN 1950 AND 1959
    AND d1."is_first_drive_of_season" = 1
    AND d2."is_final_drive_of_season" = 1
    AND d1."constructor_id" = d2."constructor_id"
    AND d1."first_round" != d2."last_round"
),
eligible_drivers AS (
  SELECT DISTINCT "driver_id"
  FROM eligible_seasons
)
SELECT DISTINCT de."full_name"
FROM "F1"."F1"."DRIVERS_EXT" de
INNER JOIN eligible_drivers ed ON de."driver_id" = ed."driver_id"
ORDER BY de."full_name"