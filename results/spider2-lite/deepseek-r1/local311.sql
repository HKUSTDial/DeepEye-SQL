WITH constructor_yearly AS (
    SELECT constructor_results.constructor_id, races.year, SUM(constructor_results.points) AS constructor_points
    FROM constructor_results
    JOIN races ON constructor_results.race_id = races.race_id
    GROUP BY constructor_results.constructor_id, races.year
),
driver_race_points AS (
    SELECT results.driver_id, results.constructor_id, races.year, SUM(results.points) AS points
    FROM results
    JOIN races ON results.race_id = races.race_id
    GROUP BY results.driver_id, results.constructor_id, races.year
    UNION ALL
    SELECT sprint_results.driver_id, sprint_results.constructor_id, races.year, SUM(sprint_results.points) AS points
    FROM sprint_results
    JOIN races ON sprint_results.race_id = races.race_id
    GROUP BY sprint_results.driver_id, sprint_results.constructor_id, races.year
),
driver_constructor_yearly AS (
    SELECT driver_id, constructor_id, year, SUM(points) AS total_driver_points
    FROM driver_race_points
    GROUP BY driver_id, constructor_id, year
),
best_driver_yearly AS (
    SELECT constructor_id, year, MAX(total_driver_points) AS best_driver_points
    FROM driver_constructor_yearly
    GROUP BY constructor_id, year
)
SELECT 
    constructors.name AS constructor_name,
    constructor_yearly.year,
    (constructor_yearly.constructor_points + best_driver_yearly.best_driver_points) AS combined_points
FROM constructor_yearly
JOIN best_driver_yearly ON constructor_yearly.constructor_id = best_driver_yearly.constructor_id 
    AND constructor_yearly.year = best_driver_yearly.year
JOIN constructors ON constructor_yearly.constructor_id = constructors.constructor_id
ORDER BY combined_points DESC
LIMIT 3;