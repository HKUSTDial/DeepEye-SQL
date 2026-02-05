WITH filtered_data AS (
    SELECT 
        city_name,
        DATE(insert_date) AS date_only
    FROM cities
    WHERE country_code_2 = 'cn'
        AND insert_date >= '2021-07-01'
        AND insert_date <= '2021-07-31'
),
distinct_dates AS (
    SELECT DISTINCT date_only
    FROM filtered_data
),
date_sequence AS (
    SELECT 
        date_only,
        ROW_NUMBER() OVER (ORDER BY date_only) AS rn
    FROM distinct_dates
),
streak_groups AS (
    SELECT 
        date_only,
        JULIANDAY(date_only) - rn AS streak_id
    FROM date_sequence
),
streak_lengths AS (
    SELECT 
        streak_id,
        MIN(date_only) AS start_date,
        MAX(date_only) AS end_date,
        COUNT(*) AS streak_days
    FROM streak_groups
    GROUP BY streak_id
),
streak_boundaries AS (
    SELECT 
        MIN(streak_days) AS min_streak_days,
        MAX(streak_days) AS max_streak_days
    FROM streak_lengths
),
target_streaks AS (
    SELECT streak_id
    FROM streak_lengths
    WHERE streak_days = (SELECT min_streak_days FROM streak_boundaries)
        OR streak_days = (SELECT max_streak_days FROM streak_boundaries)
),
target_dates AS (
    SELECT sg.date_only
    FROM streak_groups sg
    JOIN target_streaks ts ON sg.streak_id = ts.streak_id
),
result_data AS (
    SELECT 
        td.date_only,
        MIN(fd.city_name) AS city_name
    FROM target_dates td
    JOIN filtered_data fd ON td.date_only = fd.date_only
    GROUP BY td.date_only
)
SELECT 
    date_only,
    UPPER(SUBSTR(city_name, 1, 1)) || LOWER(SUBSTR(city_name, 2)) AS city_name
FROM result_data
ORDER BY date_only;