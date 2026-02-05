WITH driver_points AS (
    SELECT 
        r.year,
        d.driver_id,
        de.full_name,
        SUM(d.points) AS total_points
    FROM (
        SELECT race_id, driver_id, points FROM results
        UNION ALL
        SELECT race_id, driver_id, points FROM sprint_results
    ) d
    JOIN races r ON d.race_id = r.race_id
    JOIN drivers_ext de ON d.driver_id = de.driver_id
    GROUP BY r.year, d.driver_id, de.full_name
),
ranked_drivers AS (
    SELECT 
        year,
        driver_id,
        full_name,
        total_points,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_points DESC) AS rn
    FROM driver_points
),
constructor_points AS (
    SELECT 
        r.year,
        c.constructor_id,
        ce.name AS constructor_name,
        SUM(c.points) AS total_points
    FROM (
        SELECT race_id, constructor_id, points FROM results
        UNION ALL
        SELECT race_id, constructor_id, points FROM sprint_results
    ) c
    JOIN races r ON c.race_id = r.race_id
    JOIN constructors_ext ce ON c.constructor_id = ce.constructor_id
    GROUP BY r.year, c.constructor_id, ce.name
),
ranked_constructors AS (
    SELECT 
        year,
        constructor_id,
        constructor_name,
        total_points,
        ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_points DESC) AS rn
    FROM constructor_points
)
SELECT 
    rd.year,
    rd.full_name AS driver_full_name,
    rc.constructor_name
FROM ranked_drivers rd
LEFT JOIN ranked_constructors rc ON rd.year = rc.year AND rd.rn = 1 AND rc.rn = 1
WHERE rd.rn = 1
ORDER BY rd.year;