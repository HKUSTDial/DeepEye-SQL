WITH race_laps AS (
    SELECT driver_id, race_id, lap, position
    FROM lap_positions
    WHERE lap_type = 'Race' AND lap > 1
),
driver_sequences AS (
    SELECT 
        driver_id,
        race_id,
        lap,
        position,
        LAG(lap) OVER (PARTITION BY driver_id, race_id ORDER BY lap) AS prev_lap,
        LAG(position) OVER (PARTITION BY driver_id, race_id ORDER BY lap) AS prev_position
    FROM race_laps
),
changes AS (
    SELECT 
        driver_id,
        race_id,
        prev_lap,
        lap AS curr_lap,
        prev_position,
        position AS curr_position,
        CASE 
            WHEN prev_position > position THEN prev_position - position
            ELSE 0 
        END AS overtakes_made,
        CASE 
            WHEN prev_position < position THEN position - prev_position
            ELSE 0 
        END AS overtaken_times
    FROM driver_sequences
    WHERE prev_lap IS NOT NULL
),
pit_stops_laps AS (
    SELECT DISTINCT race_id, driver_id, lap
    FROM pit_stops
),
retirements_laps AS (
    SELECT DISTINCT race_id, driver_id, lap
    FROM retirements
),
excluded_changes AS (
    SELECT c.*
    FROM changes c
    WHERE EXISTS (
        SELECT 1 FROM pit_stops_laps p 
        WHERE p.race_id = c.race_id AND p.driver_id = c.driver_id 
        AND p.lap BETWEEN c.prev_lap AND c.curr_lap
    ) OR EXISTS (
        SELECT 1 FROM retirements_laps r 
        WHERE r.race_id = c.race_id AND r.driver_id = c.driver_id 
        AND r.lap BETWEEN c.prev_lap AND c.curr_lap
    )
),
valid_changes AS (
    SELECT * FROM changes
    EXCEPT
    SELECT * FROM excluded_changes
),
driver_totals AS (
    SELECT 
        driver_id,
        SUM(overtakes_made) AS total_overtakes_made,
        SUM(overtaken_times) AS total_overtaken
    FROM valid_changes
    GROUP BY driver_id
)
SELECT d.full_name
FROM driver_totals t
JOIN drivers_ext d ON t.driver_id = d.driver_id
WHERE t.total_overtaken > t.total_overtakes_made
ORDER BY d.full_name;