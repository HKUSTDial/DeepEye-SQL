WITH player_career AS (
    SELECT 
        player_id,
        debut,
        final_game,
        CAST(julianday(final_game) - julianday(debut) AS INTEGER) AS total_days
    FROM player
    WHERE debut IS NOT NULL AND final_game IS NOT NULL
),
career_span_calc AS (
    SELECT 
        player_id,
        total_days / 365 AS years,
        (total_days % 365) / 30 AS months,
        (total_days % 365) % 30 AS days
    FROM player_career
)
SELECT 
    ROUND(AVG(
        years + 
        ROUND(months / 12.0, 2) + 
        ROUND(days / 365.0, 2)
    ), 2) AS avg_career_span_years
FROM career_span_calc;