WITH driver_points AS (
    SELECT
        "F1"."F1"."RACES"."year",
        "F1"."F1"."RESULTS"."constructor_id",
        "F1"."F1"."RESULTS"."points"
    FROM
        "F1"."F1"."RESULTS"
    JOIN
        "F1"."F1"."RACES" ON "F1"."F1"."RESULTS"."race_id" = "F1"."F1"."RACES"."race_id"
    WHERE
        "F1"."F1"."RACES"."year" >= 2001
        AND "F1"."F1"."RESULTS"."points" > 0
    UNION ALL
    SELECT
        "F1"."F1"."RACES"."year",
        "F1"."F1"."SPRINT_RESULTS"."constructor_id",
        "F1"."F1"."SPRINT_RESULTS"."points"
    FROM
        "F1"."F1"."SPRINT_RESULTS"
    JOIN
        "F1"."F1"."RACES" ON "F1"."F1"."SPRINT_RESULTS"."race_id" = "F1"."F1"."RACES"."race_id"
    WHERE
        "F1"."F1"."RACES"."year" >= 2001
        AND "F1"."F1"."SPRINT_RESULTS"."points" > 0
),
season_constructor_points AS (
    SELECT
        "year",
        "constructor_id",
        SUM("points") AS "total_points"
    FROM
        driver_points
    GROUP BY
        "year",
        "constructor_id"
),
min_points_per_season AS (
    SELECT
        "year",
        MIN("total_points") AS "min_points"
    FROM
        season_constructor_points
    GROUP BY
        "year"
),
constructors_with_min AS (
    SELECT
        scp."year",
        scp."constructor_id"
    FROM
        season_constructor_points scp
    JOIN
        min_points_per_season mps ON scp."year" = mps."year" AND scp."total_points" = mps."min_points"
),
constructor_season_counts AS (
    SELECT
        "constructor_id",
        COUNT(*) AS "count_seasons"
    FROM
        constructors_with_min
    GROUP BY
        "constructor_id"
)
SELECT
    "F1"."F1"."CONSTRUCTORS_EXT"."name" AS "constructor_name",
    csc."count_seasons"
FROM
    constructor_season_counts csc
JOIN
    "F1"."F1"."CONSTRUCTORS_EXT" ON csc."constructor_id" = "F1"."F1"."CONSTRUCTORS_EXT"."constructor_id"
ORDER BY
    csc."count_seasons" DESC,
    "constructor_name"
LIMIT 5