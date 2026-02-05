WITH driver_points AS (
  SELECT 
    r."year",
    d."full_name",
    SUM(re."points") AS "total_points",
    ROW_NUMBER() OVER (PARTITION BY r."year" ORDER BY SUM(re."points") DESC) AS "rank"
  FROM "F1"."F1"."RESULTS" re
  JOIN "F1"."F1"."RACES" r ON re."race_id" = r."race_id"
  JOIN "F1"."F1"."DRIVERS" d ON re."driver_id" = d."driver_id"
  GROUP BY r."year", d."full_name"
),
constructor_points AS (
  SELECT 
    r."year",
    c."name" AS "constructor_name",
    SUM(re."points") AS "total_points",
    ROW_NUMBER() OVER (PARTITION BY r."year" ORDER BY SUM(re."points") DESC) AS "rank"
  FROM "F1"."F1"."RESULTS" re
  JOIN "F1"."F1"."RACES" r ON re."race_id" = r."race_id"
  JOIN "F1"."F1"."CONSTRUCTORS" c ON re."constructor_id" = c."constructor_id"
  GROUP BY r."year", c."name"
)
SELECT 
  dp."year",
  dp."full_name" AS "driver_full_name",
  dp."total_points" AS "driver_points",
  cp."constructor_name",
  cp."total_points" AS "constructor_points"
FROM driver_points dp
JOIN constructor_points cp ON dp."year" = cp."year"
WHERE dp."rank" = 1 AND cp."rank" = 1
ORDER BY dp."year"