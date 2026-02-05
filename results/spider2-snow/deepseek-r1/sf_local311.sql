WITH driver_season_points AS (
    SELECT 
        a."driver_id",
        a."constructor_id",
        r."year",
        SUM(a."points") AS "total_driver_points"
    FROM (
        SELECT "driver_id", "constructor_id", "race_id", "points"
        FROM "F1"."F1"."RESULTS"
        UNION ALL
        SELECT "driver_id", "constructor_id", "race_id", "points"
        FROM "F1"."F1"."SPRINT_RESULTS"
    ) a
    INNER JOIN "F1"."F1"."RACES" r
        ON a."race_id" = r."race_id"
    GROUP BY a."driver_id", a."constructor_id", r."year"
),
constructor_season AS (
    SELECT 
        "constructor_id",
        "year",
        SUM("total_driver_points") AS "team_points",
        MAX("total_driver_points") AS "best_driver_points"
    FROM driver_season_points
    GROUP BY "constructor_id", "year"
),
ranked AS (
    SELECT 
        "constructor_id",
        "year",
        "team_points" + "best_driver_points" AS "combined_points",
        DENSE_RANK() OVER (ORDER BY "team_points" + "best_driver_points" DESC) AS "rank"
    FROM constructor_season
)
SELECT 
    c."name" AS "constructor_name",
    r."year",
    r."combined_points"
FROM ranked r
INNER JOIN "F1"."F1"."CONSTRUCTORS" c
    ON r."constructor_id" = c."constructor_id"
WHERE r."rank" <= 3
ORDER BY r."rank", r."year"