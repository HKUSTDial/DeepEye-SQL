WITH june_data AS (
    SELECT DISTINCT "country_code_2", TO_DATE("insert_date") as date
    FROM "CITY_LEGISLATION"."CITY_LEGISLATION"."CITIES"
    WHERE TO_DATE("insert_date") BETWEEN '2022-06-01' AND '2022-06-30'
),
ranked AS (
    SELECT "country_code_2", date,
           ROW_NUMBER() OVER (PARTITION BY "country_code_2" ORDER BY date) as rn
    FROM june_data
),
grouped AS (
    SELECT "country_code_2", date,
           DATEADD(day, -rn, date) as grp
    FROM ranked
),
streaks AS (
    SELECT "country_code_2", grp, COUNT(*) as streak_length
    FROM grouped
    GROUP BY "country_code_2", grp
),
max_streak AS (
    SELECT MAX(streak_length) as max_len
    FROM streaks
)
SELECT DISTINCT s."country_code_2"
FROM streaks s
CROSS JOIN max_streak m
WHERE s.streak_length = m.max_len