WITH point_scoring_results AS (
    SELECT r.driver_id, r.constructor_id, ra.year, r.points
    FROM results r
    JOIN races ra ON r.race_id = ra.race_id
    WHERE ra.year >= 2001 AND r.points > 0
    UNION ALL
    SELECT sr.driver_id, sr.constructor_id, ra.year, sr.points
    FROM sprint_results sr
    JOIN races ra ON sr.race_id = ra.race_id
    WHERE ra.year >= 2001 AND sr.points > 0
),
constructor_season_totals AS (
    SELECT year, constructor_id, SUM(points) AS total_points
    FROM point_scoring_results
    GROUP BY year, constructor_id
    HAVING SUM(points) > 0
),
season_min_points AS (
    SELECT year, MIN(total_points) AS min_points
    FROM constructor_season_totals
    GROUP BY year
),
fewest_constructors AS (
    SELECT cst.year, cst.constructor_id
    FROM constructor_season_totals cst
    INNER JOIN season_min_points smp ON cst.year = smp.year AND cst.total_points = smp.min_points
),
constructor_counts AS (
    SELECT constructor_id, COUNT(*) AS season_count
    FROM fewest_constructors
    GROUP BY constructor_id
)
SELECT c.name, cc.season_count
FROM constructor_counts cc
JOIN constructors c ON cc.constructor_id = c.constructor_id
ORDER BY cc.season_count DESC
LIMIT 5