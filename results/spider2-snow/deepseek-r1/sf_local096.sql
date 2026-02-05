WITH movie_years AS (
    SELECT 
        "MID",
        CAST(RIGHT("year", 4) AS INTEGER) AS year_num
    FROM "DB_IMDB"."DB_IMDB"."MOVIE"
    WHERE LENGTH("year") >= 4
),
actor_counts AS (
    SELECT 
        mc."MID",
        COUNT(*) AS total_actors,
        SUM(CASE WHEN p."Gender" = 'Female' THEN 1 ELSE 0 END) AS female_actors
    FROM "DB_IMDB"."DB_IMDB"."M_CAST" AS mc
    LEFT JOIN "DB_IMDB"."DB_IMDB"."PERSON" AS p ON mc."PID" = p."PID"
    GROUP BY mc."MID"
),
movie_flags AS (
    SELECT 
        my."MID",
        my.year_num,
        COALESCE(ac.total_actors, 0) AS total_actors,
        COALESCE(ac.female_actors, 0) AS female_actors,
        CASE 
            WHEN COALESCE(ac.total_actors, 0) > 0 
             AND COALESCE(ac.total_actors, 0) = COALESCE(ac.female_actors, 0) 
            THEN 1 
            ELSE 0 
        END AS is_exclusively_female
    FROM movie_years my
    LEFT JOIN actor_counts ac ON my."MID" = ac."MID"
)
SELECT 
    year_num AS year,
    COUNT(*) AS total_movies,
    SUM(is_exclusively_female) AS exclusively_female_movies,
    (SUM(is_exclusively_female) * 100.0) / COUNT(*) AS percentage
FROM movie_flags
GROUP BY year_num
ORDER BY year_num