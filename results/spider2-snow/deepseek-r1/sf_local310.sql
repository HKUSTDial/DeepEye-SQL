WITH driver_totals AS (
    SELECT
        "F1"."F1"."RACES"."year" AS year,
        "F1"."F1"."RESULTS"."driver_id" AS driver_id,
        SUM("F1"."F1"."RESULTS"."points") AS driver_total
    FROM
        "F1"."F1"."RESULTS"
    JOIN
        "F1"."F1"."RACES" ON "F1"."F1"."RESULTS"."race_id" = "F1"."F1"."RACES"."race_id"
    GROUP BY
        "F1"."F1"."RACES"."year", "F1"."F1"."RESULTS"."driver_id"
),
max_driver AS (
    SELECT
        year,
        MAX(driver_total) AS max_driver_points
    FROM
        driver_totals
    GROUP BY
        year
),
constructor_totals AS (
    SELECT
        "F1"."F1"."RACES"."year" AS year,
        "F1"."F1"."RESULTS"."constructor_id" AS constructor_id,
        SUM("F1"."F1"."RESULTS"."points") AS constructor_total
    FROM
        "F1"."F1"."RESULTS"
    JOIN
        "F1"."F1"."RACES" ON "F1"."F1"."RESULTS"."race_id" = "F1"."F1"."RACES"."race_id"
    GROUP BY
        "F1"."F1"."RACES"."year", "F1"."F1"."RESULTS"."constructor_id"
),
max_constructor AS (
    SELECT
        year,
        MAX(constructor_total) AS max_constructor_points
    FROM
        constructor_totals
    GROUP BY
        year
),
combined AS (
    SELECT
        md.year,
        md.max_driver_points,
        mc.max_constructor_points,
        (md.max_driver_points + mc.max_constructor_points) AS total
    FROM
        max_driver md
    JOIN
        max_constructor mc ON md.year = mc.year
)
SELECT
    year
FROM
    combined
ORDER BY
    total ASC, year ASC
LIMIT 3